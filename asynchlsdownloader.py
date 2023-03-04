import asyncio
import binascii
import contextlib
from datetime import timedelta
from datetime import datetime
import json
import logging
import os as syncos
import random
import re
import threading
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from shutil import rmtree
import copy

import aiofiles
import aiofiles.os as os
import httpx
import m3u8
from Cryptodome.Cipher import AES

from utils import (
    CONF_HLS_RESET_403_TIME,
    CONF_HLS_SPEED_PER_WORKER,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    CONFIG_EXTRACTORS,
    ProgressTimer,
    ProxyYTDL,
    SmoothETA,
    SpeedometerMA,
    _for_print,
    _for_print_entry,
    async_wait_time,
    wait_for_either,
    dec_retry_error,
    get_format_id,
    int_or_none,
    limiter_non,
    limiter_0_1,
    my_dec_on_exception,
    naturalsize,
    print_norm_time,
    print_tasks,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    try_get,
    CountDowns,
    myYTDL,
    async_waitfortasks,
    Union,
    cast,
    MySyncAsyncEvent,
    put_sequence
)

from functools import partial

from yt_dlp.extractor.nakedsword import NakedSwordBaseIE

logger = logging.getLogger("async_HLS_DL")

retry = my_dec_on_exception(Exception,
                            max_tries=3,
                            raise_on_giveup=True,
                            interval=1)


class AsyncHLSDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):

        super().__init__(msg)

        self.exc_info = exc_info


class AsyncHLSDLError(Exception):
    def __init__(self, msg, exc_info=None):

        super().__init__(msg)

        self.exc_info = exc_info


class AsyncHLSDLReset(Exception):
    def __init__(self, msg, exc_info=None):

        super().__init__(msg)

        self.exc_info = exc_info


class AsyncHLSDownloader:

    _CHUNK_SIZE = 102400
    _MAX_RETRIES = 5
    _MAX_RESETS = 10
    _MIN_TIME_RESETS = 15
    _CONFIG = CONFIG_EXTRACTORS.copy()
    _CLASSLOCK = threading.Lock()
    #  _COUNTDOWNS_READY = MySyncAsyncEvent(name="countdown", initset=True)
    _OK_503 = MySyncAsyncEvent("ok_403")
    _CLEAN = MySyncAsyncEvent("clean")
    _COUNTDOWNS = None
    _QUEUE = {}

    def __init__(self, enproxy: bool, video_dict: dict, vid_dl):

        try:
            self.background_tasks = set()
            self._test: list = []
            self.info_dict: dict = video_dict.copy()
            self.vid_dl = vid_dl
            self.enproxy: bool = enproxy
            self.n_workers: int = self.vid_dl.info_dl["n_workers"]
            self.count: int = 0
            # cuenta de los workers activos haciendo DL. Al comienzo serán
            # igual a n_workers
            #  self.video_url: str = self.info_dict["url"]  # url del format
            #  self.webpage_url: str = self.info_dict["webpage_url"]
            self.m3u8_doc = None
            #  self.id: str = self.info_dict["id"]
            self.ytdl: myYTDL = self.vid_dl.info_dl["ytdl"]
            self.verifycert: bool = not self.ytdl.params.get(
                "nocheckcertificate")
            self.timeout: httpx.Timeout = httpx.Timeout(15, connect=15)
            self.limits: httpx.Limits = httpx.Limits(
                max_keepalive_connections=None,
                max_connections=None,
                keepalive_expiry=30,
            )
            #  self.headers: dict[str, str] = self.info_dict["http_headers"]
            self.base_download_path = Path(
                str(self.info_dict["download_path"]))
            self.init_file = Path(
                self.base_download_path,
                f"init_file.{self.info_dict['format_id']}")
            _filename = Path(
                self.info_dict.get(
                    "_filename", self.info_dict.get("filename")))
            self.download_path = Path(
                self.base_download_path, self.info_dict["format_id"])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.fragments_base_path = Path(
                self.download_path,
                _filename.stem + "." + self.info_dict[
                    "format_id"] + "." + self.info_dict["ext"])
            self.filename = Path(
                self.base_download_path,
                _filename.stem + "." + self.info_dict[
                    "format_id"] + "." + "ts")

            self.key_cache = dict()
            self.n_reset = 0
            self._limit_reset = limiter_0_1.ratelimit("resetdl", delay=True)
            self.throttle = 0
            self.down_size = 0
            self.status = "init"
            self.error_message = ""
            self.upt = {}
            self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_hlsdl")
            self.special_extr: bool = False
            if self.enproxy:
                _seq = zip(
                    random.sample(
                        range(CONF_PROXIES_MAX_N_GR_HOST),
                        CONF_PROXIES_MAX_N_GR_HOST),
                    random.sample(
                        range(CONF_PROXIES_MAX_N_GR_HOST),
                        CONF_PROXIES_MAX_N_GR_HOST))

                self._qproxies = put_sequence(Queue(), _seq)

            _proxies = self._proxy if hasattr(self, '_proxy') else None
            self.init_client = httpx.Client(
                proxies=_proxies,  # type: ignore
                follow_redirects=True,
                headers=self.info_dict['http_headers'],
                limits=self.limits,
                timeout=self.timeout,
                verify=False,
            )
            self.filesize = None
            self.premsg = ''.join([
                f'[{self.info_dict["id"]}]',
                f'[{self.info_dict["title"]}]',
                f'[{self.info_dict["format_id"]}]'])

            self.count_msg = ''

            self.init()
        except Exception as e:
            logger.exception(repr(e))
            self.init_client.close()

    def init(self):

        def getter(x: Union[str, None]):
            try:
                if not x:
                    self.special_extr = False
                    return limiter_non.ratelimit("transp", delay=True)
                if "nakedsword" in x:
                    self._CONF_HLS_MAX_SPEED_PER_DL = 10 * 1048576
                    self.auto_pasres = True
                    logger.debug(
                        f'{self.premsg} ' +
                        f'{_for_print_entry(self.info_dict)}')
                    if self.info_dict.get("playlist_title", "") not in (
                        "MostWatchedScenes",
                        "Search",
                    ):
                        self.fromplns = self.info_dict.get(
                            "playlist_id", False)

                    if self.fromplns:
                        if not self.vid_dl.info_dl[
                                "fromplns"].get("ALL"):
                            _all_temp = MySyncAsyncEvent()
                            _all_temp.set()
                            self.vid_dl.info_dl[
                                "fromplns"]["ALL"] = {
                                "sem": threading.BoundedSemaphore(value=100),
                                "downloading": set(),
                                "in_reset": set(),
                                "reset": _all_temp,
                            }
                        if not self.vid_dl.info_dl["fromplns"].get(
                            self.fromplns
                        ):
                            temp = MySyncAsyncEvent()
                            temp.set()

                            self.vid_dl.info_dl["fromplns"][
                                self.fromplns] = {
                                "downloaders": {
                                    self.vid_dl.info_dict[
                                        "playlist_index"
                                    ]: self.vid_dl
                                },
                                "downloading": set(),
                                "in_reset": set(),
                                "reset": temp,
                                "sem": threading.BoundedSemaphore(value=1),
                            }
                        else:
                            self.vid_dl.info_dl["fromplns"][
                                self.fromplns][
                                "downloaders"
                            ].update(
                                {
                                    self.vid_dl.info_dict[
                                        "playlist_index"
                                    ]: self.vid_dl
                                }
                            )

                        _downloaders = self.vid_dl.info_dl[
                            'fromplns'][self.fromplns]['downloaders']
                        logger.debug(
                            f'{self.premsg}: ' +
                            f'added new dl to plns [{self.fromplns}], ' +
                            f'count [{len(_downloaders)}] ' +
                            f'members[{list(_downloaders.keys())}]'
                        )
            except Exception as e:
                logger.exception(
                    f'{self.premsg}: {str(e)}')

            value, key_text = try_get(
                [
                    (v, kt)
                    for k, v in self._CONFIG.items()
                    if any(x == (kt := _) for _ in k)
                ],
                lambda y: y[0],
            ) or ("", "")

            if value:
                self.special_extr = True
                return value["ratelimit"].ratelimit(key_text, delay=True)
            else:
                self.special_extr = False
                return limiter_non.ratelimit("transp", delay=True)

        try:
            self.auto_pasres = False
            self.fromplns = False
            self._CONF_HLS_MAX_SPEED_PER_DL = None

            self._extractor = try_get(self.info_dict.get(
                "extractor_key", "Generic").lower(), lambda x: x.lower())
            self._limit = getter(self._extractor)
            self.info_frag = []
            self.info_init_section = {}
            self.frags_to_dl = []
            self.n_dl_fragments = 0
            # for audio streams tbr is not present
            self.tbr = self.info_dict.get("tbr", 0)
            self.abr = self.info_dict.get("abr", 0)
            _br = self.tbr or self.abr

            byte_range = {}

            self.m3u8_doc = try_get(
                #  self.init_client.get(self.video_url),
                self.init_client.get(self.info_dict['url']),
                lambda x: x.content.decode("utf-8", "replace"),
            )

            self.info_dict["fragments"] = self.get_info_fragments()
            self.info_dict["init_section"] = self.info_dict[
                "fragments"][0].init_section

            logger.debug(
                f'fragments:\n{[str(f) for f in self.info_dict["fragments"]]}')

            logger.debug(
                f'init_section:\n{self.info_dict["init_section"]}')

            if _frag := self.info_dict["init_section"]:
                _file_path = Path(str(self.fragments_base_path) + ".Frag0")
                _url = _frag.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="
                if _frag.key is not None and _frag.key.method == "AES-128":
                    if _frag.key.absolute_uri not in self.key_cache:
                        self.key_cache.update({
                            _frag.key.absolute_uri: httpx.get(
                                    _frag.key.absolute_uri,
                                    headers=self.info_dict['http_headers']
                                ).content
                            }
                        )
                        logger.debug(
                            f'{self.premsg}:' +
                            f'{self.key_cache[_frag.key.absolute_uri]}')

                        logger.debug(f"{self.premsg}:{_frag.key.iv}")

                self.info_init_section.update({
                    "frag": 0, "url": _url,
                    "file": _file_path, "downloaded": False})

                self.get_init_section(_url, _file_path, _frag.key)

            if self.init_file.exists():
                with open(self.init_file, "r") as f:
                    init_data = json.loads(f.read())
                init_data = {int(k): v for k, v in init_data.items()}
            else:
                init_data = {}

            for i, fragment in enumerate(self.info_dict["fragments"]):

                if not fragment.uri and fragment.parts:
                    fragment.uri = fragment.parts[0].uri

                if fragment.byterange:
                    splitted_byte_range = fragment.byterange.split("@")
                    sub_range_start = (
                        int(splitted_byte_range[1])
                        if len(splitted_byte_range) == 2
                        else byte_range["end"]
                    )
                    byte_range = {
                        "start": sub_range_start,
                        "end": sub_range_start + int(splitted_byte_range[0]),
                    }

                else:
                    byte_range = {}

                est_size = int(_br * fragment.duration * 1000 / 8)

                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="

                _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i}")

                if _file_path.exists():
                    size = _file_path.stat().st_size
                    hsize = init_data.get(i + 1)
                    is_dl = True
                    if not size or ((size and hsize) and not
                                    (hsize - 100 <= size <= hsize + 100)):
                        is_dl = False
                        _file_path.unlink()

                    self.info_frag.append(
                        {
                            "frag": i + 1,
                            "url": _url,
                            "key": fragment.key,
                            "file": _file_path,
                            "byterange": byte_range,
                            "downloaded": is_dl,
                            "estsize": est_size,
                            "headersize": hsize,
                            "size": size,
                            "n_retries": 0,
                            "error": ["AlreadyDL"],
                        }
                    )
                    self.n_dl_fragments += 1
                    if is_dl:
                        self.down_size += size
                    if not is_dl or not hsize:
                        self.frags_to_dl.append(i + 1)

                else:
                    self.info_frag.append(
                        {
                            "frag": i + 1,
                            "url": _url,
                            "key": fragment.key,
                            "file": _file_path,
                            "byterange": byte_range,
                            "downloaded": False,
                            "estsize": est_size,
                            "headersize": init_data.get(i + 1),
                            "size": None,
                            "n_retries": 0,
                            "error": [],
                        }
                    )
                    self.frags_to_dl.append(i + 1)

                if fragment.key and fragment.key.method == "AES-128":
                    if fragment.key.absolute_uri not in self.key_cache:
                        self.key_cache.update(
                            {
                                fragment.key.absolute_uri: httpx.get(
                                    fragment.key.absolute_uri,
                                    headers=self.info_dict['http_headers']
                                ).content
                            }
                        )
                        logger.debug(
                            f"{self.premsg}:{self.key_cache[fragment.key.absolute_uri]}")
                        logger.debug(f"{self.premsg}:{fragment.key.iv}")

            logger.debug(
                f"{self.premsg}: Frags already DL: {len(self.fragsdl())}, " +
                f"Frags not DL: {len(self.fragsnotdl())}, " +
                f"Frags to request: {len(self.frags_to_dl)}")
            logger.debug(
                f"{self.premsg}: \nFrags DL: {self.fragsdl()}\nFrags not DL: {self.fragsnotdl()}")

            self.n_total_fragments = len(self.info_dict["fragments"])

            self.totalduration = self.info_dict.get("duration", 0) or 0
            if not self.totalduration:
                self.calculate_duration()  # get total duration
            self.filesize = self.info_dict.get(
                "filesize") or self.info_dict.get("filesize_approx")
            if not self.filesize:
                self.calculate_filesize()  # get filesize estimated

            self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 120

            if not self.filesize:
                _est_size = "NA"

            else:
                if (_todl := (self.filesize - self.down_size)) < 250000000:
                    self.n_workers = min(self.n_workers, 8)
                elif 250000000 <= _todl < 500000000:
                    self.n_workers = min(self.n_workers, 16)
                elif 500000000 <= _todl < 1250000000:
                    self.n_workers = min(self.n_workers, 32)
                    # self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 90
                else:
                    self.n_workers = min(self.n_workers, 64)
                    # self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 120

                _est_size = naturalsize(self.filesize)

            logger.debug(
                f"{self.premsg}: total duration " +
                f"{print_norm_time(self.totalduration)} -- " +
                f"estimated filesize {_est_size} -- already downloaded " +
                f"{naturalsize(self.down_size)} -- total fragments " +
                f"{self.n_total_fragments} -- fragments already dl " +
                f"{self.n_dl_fragments}")

            if self.filename.exists() and self.filename.stat().st_size > 0:
                self.status = "done"

            elif not self.frags_to_dl:
                self.status = "init_manipulating"
        except Exception as e:
            logger.exception(f"{self.premsg}[init] {repr(e)}")
            self.status = "error"

    def calculate_duration(self):
        self.totalduration = 0
        for fragment in self.info_dict["fragments"]:
            self.totalduration += fragment.duration

    def calculate_filesize(self):
        _bitrate = self.tbr or self.abr
        self.filesize = int(self.totalduration * 1000 * _bitrate / 8)

    def get_info_fragments(self):

        try:
            self.m3u8_obj = m3u8.loads(self.m3u8_doc, self.info_dict['url'])

            if not self.m3u8_obj or not self.m3u8_obj.segments:
                raise AsyncHLSDLError("couldnt get m3u8 file")

            if self.m3u8_obj.keys:
                for _key in self.m3u8_obj.keys:
                    if _key and _key.method != "AES-128":
                        logger.warning(f"key AES method: {_key.method}")

            return self.m3u8_obj.segments

        except Exception as e:
            logger.error(f"{self.premsg}[get_info_fragments] - {repr(e)}")
            raise AsyncHLSDLErrorFatal(repr(e))

    # def get_m3u8_obj(self):

    #     if not self.m3u8_doc:
    #         self.m3u8_doc = try_get(
    #             #  self.init_client.get(self.video_url),
    #             self.init_client.get(self.info_dict['url']),
    #             lambda x: x.content.decode("utf-8", "replace"),
    #         )

    #     #  return m3u8.loads(self.m3u8_doc, self.video_url)
    #     return m3u8.loads(self.m3u8_doc, self.info_dict['url'])

    @dec_retry_error
    def get_init_section(self, uri, file, key):

        try:
            cipher = None
            if key is not None and key.method == "AES-128":
                iv = binascii.unhexlify(key.iv[2:])
                cipher = AES.new(self.key_cache[key.absolute_uri],
                                 AES.MODE_CBC, iv)
            res = self.init_client.get(uri)
            res.raise_for_status()
            _frag = res.content if not cipher else cipher.decrypt(res.content)
            with open(file, "wb") as f:
                f.write(_frag)

            self.info_init_section.update({"downloaded": True})

        except Exception as e:

            logger.exception(
                f"{self.premsg}:[get_init_section] {repr(e)}"
            )
            raise

    def multi_extract_info(self, url, proxy=None, msg=None):

        premsg = ""
        if msg:
            premsg = msg

        try:
            if proxy:
                with ProxyYTDL(opts=self.ytdl.params.copy(),
                               proxy=proxy) as proxy_ytdl:
                    _info_video = proxy_ytdl.sanitize_info(
                        proxy_ytdl.extract_info(url))

            else:
                _info_video = self.ytdl.sanitize_info(
                    self.ytdl.extract_info(url, download=False)
                )

            if not _info_video:
                raise AsyncHLSDLErrorFatal("no info video")

            return _info_video

        except Exception as e:
            logger.error(f"{premsg} fails when extracting info {str(e)}")
            raise

    @retry
    def get_reset_info(self, _reset_url, first=False):

        _proxy = self._proxy["all://"] if hasattr(self, '_proxy') else None
        _print_proxy = _proxy.split(":")[-1] if _proxy else None

        logger.debug(
            f"{self.premsg}:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:" +
            f"FIRST[{first}] proxy [{_print_proxy}]: " +
            f"get video dict: {_reset_url}")

        _info = None

        info_reset = None

        if self.fromplns:

            if first:
                with contextlib.suppress(OSError):
                    syncos.remove(
                        self.ytdl.cache._get_cache_fn(
                            "nakedswordmovie", str(self.fromplns), "json"
                        )
                    )

                _info = self.multi_extract_info(
                    _reset_url,
                    proxy=_proxy,
                    msg=(
                        f"{self.premsg}:RESET[{self.n_reset}]:" +
                        f"PLNS[{self.fromplns}]:FIRST[{first}] proxy " +
                        f"[{_print_proxy}]:"))

                self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), _info)

            else:
                _info = self.ytdl.cache.load("nakedswordmovie", str(self.fromplns))

                if not _info:
                    _info = self.multi_extract_info(
                        _reset_url,
                        proxy=_proxy,
                        msg=(
                            f"{self.premsg}:RESET[{self.n_reset}]:" +
                            f"PLNS[{self.fromplns}]:FIRST[{first}] " +
                            f"proxy [{_print_proxy}]:"))

                    self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), _info)

            if _info:
                info_reset = try_get(traverse_obj(
                    _info,
                    (
                        "entries",
                        int(self.vid_dl.info_dict["playlist_index"]) - 1
                    )),
                    lambda x: get_format_id(
                        x, self.info_dict["format_id"]) if x else None)

        else:

            _info = self.multi_extract_info(
                _reset_url,
                proxy=_proxy,
                msg=(
                    f"{self.premsg}:RESET[{self.n_reset}]:" +
                    f"PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]:"))

            if _info:
                info_reset = get_format_id(_info, self.info_dict["format_id"])

        if not info_reset:
            raise AsyncHLSDLErrorFatal(
                f"{self.premsg}:RESET[{self.n_reset}]:" +
                f"PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]: " +
                "fails no descriptor")

        logger.debug(
            f"{self.premsg}:RESET[{self.n_reset}]:" +
            f"PLNS[{self.fromplns}]:FIRST[{first}] " +
            f"proxy [{_print_proxy}]: format extracted info video ok")

        logger.debug(
            f"{self.premsg}:RESET[{self.n_reset}]:" +
            f"PLNS[{self.fromplns}]:FIRST[{first}] " +
            f"proxy [{_print_proxy}]: format extracted info video ok\n" +
            f"%no%{_for_print(info_reset)}")

        try:
            self.prep_reset(info_reset)
            return True
        except Exception as e:
            logger.exception(
                f"{self.premsg}:RESET[{self.n_reset}]:" +
                f"PLNS[{self.fromplns}]:FIRST[{first}] " +
                f"proxy [{_print_proxy}]: Exception occurred when reset: " +
                f"{repr(e)} {_for_print(info_reset)}")
            raise AsyncHLSDLErrorFatal("RESET fails: preparation frags failed")

    def total_in_reset(self, plns=None):

        if not plns:
            plid_total = self.vid_dl.info_dl['fromplns']['ALL']['in_reset']
        else:
            plid_total = [plns]

        count = 0
        for plid in plid_total:
            count += len(self.vid_dl.info_dl['fromplns'][plid]['in_reset'])
        return count

    def resetdl(self, cause=None):

        logger.info(f"{self.premsg}:RESET[{self.n_reset}] cause[{cause}] fromplns[{self.fromplns}]")

        try:

            if str(cause) == "403":

                #  _quiet = True
                #  AsyncHLSDownloader._COUNTDOWNS_READY.wait()
                with AsyncHLSDownloader._CLASSLOCK:
                    if not AsyncHLSDownloader._COUNTDOWNS:
                        if self.fromplns:
                            _total = self.total_in_reset(plns=self.fromplns)
                        else:
                            _total = 100000
                        AsyncHLSDownloader._COUNTDOWNS = CountDowns(
                            _total, AsyncHLSDownloader, events=AsyncHLSDownloader._OK_503, logger=logger)
                        #  _quiet = False
                        logger.info(f"{self.premsg}:RESET[{self.n_reset}] new COUNTDOWN with expected [{_total}]")

                _ev = AsyncHLSDownloader._COUNTDOWNS.add(CONF_HLS_RESET_403_TIME, index=str(self.vid_dl.index),
                                                         event=self.vid_dl.stop_event, msg=self.premsg)
                if not _ev:
                    return

                logger.info(f"{self.premsg}:RESET[{self.n_reset}] fin wait in reset cause 403")

            if self.enproxy:
                el1, el2 = cast(tuple, self._qproxies.get())
                _proxy_port = CONF_PROXIES_BASE_PORT + el1*100 + el2
                _proxy = f"http://127.0.0.1:{_proxy_port}"
                self._proxy = {"all://": _proxy}

            _wurl = self.info_dict["webpage_url"]

            if self.fromplns and str(cause) in ("403", "hard"):

                if cause == "403":
                    NakedSwordBaseIE._STATUS = "403"

                _webpage_url = smuggle_url(
                    re.sub(r"(/scene/\d+)", "", _wurl),
                    {
                        "indexdl": self.vid_dl.index,
                        "args": {
                            "nakedswordmovie": {
                                "listreset": [
                                    int(index)
                                    for index in list(
                                        self.vid_dl.info_dl[
                                            "fromplns"][
                                            self.fromplns
                                        ]["in_reset"]
                                    )
                                ]
                            }
                        },
                    },
                )

                with (_sem := self.vid_dl.info_dl["fromplns"]["ALL"]["sem"]):

                    # logger.debug(f"{self.premsg}:RESET[{self.n_reset}] in sem")

                    # _first_all = False
                    # if _sem._initial_value == 1:
                    #     _first_all = True
                    #     if cause == "403":
                    #         NakedSwordBaseIE._STATUS = "403"

                    #   NakedSwordBaseIE._API.logout()
                    #   NakedSwordBaseIE._API.get_auth()

                    assert _sem

                    with (_sem2 := self.vid_dl.info_dl["fromplns"][self.fromplns]["sem"]):

                        logger.debug(f"{self.premsg}:RESET[{self.n_reset}] in sem2")

                        _first = False
                        if _sem2._initial_value == 1:
                            _first = True
                            NakedSwordBaseIE._API.logout()
                            time.sleep(5)
                            NakedSwordBaseIE._API.get_auth()

                        if self.get_reset_info(
                            _webpage_url,
                            first=_first,
                        ):
                            #   if _first_all:
                            #     AsyncHLSDownloader._OK_503.set()
                            #     _sem._initial_value = 100
                            #     _sem.release(50)

                            if _first:
                                _sem2._initial_value = 100
                                _sem2.release(50)

            else:
                if self.special_extr:
                    _webpage_url = smuggle_url(_wurl, {"indexdl": self.vid_dl.index})
                else:
                    _webpage_url = _wurl
                self.get_reset_info(_webpage_url)

        except Exception as e:
            logger.exception(
                f"{self.premsg}:RESET[{self.n_reset}]: outer Exception occurred when reset: {repr(e)}")
            raise
        finally:
            if self.fromplns and cause in ("403", "hard"):

                with AsyncHLSDownloader._CLASSLOCK:

                    _inreset = self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]
                    try:
                        _inreset.remove(self.vid_dl.info_dict["playlist_index"])
                    except Exception:
                        logger.warning(
                            f'{self.premsg}[resetdl]:RESET[{self.n_reset}]: ' +
                            f'error when removing [{self.vid_dl.info_dict["playlist_index"]}] ' +
                            f'from inreset[{self.fromplns}] {_inreset}')

                    if not self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]:

                        logger.info(
                            f"{self.premsg}:RESET[{self.n_reset}]: end of resets fromplns [{self.fromplns}]")

                        self.vid_dl.info_dl["fromplns"][self.fromplns]["reset"].set()
                        self.vid_dl.info_dl["fromplns"][self.fromplns]["sem"] = threading.BoundedSemaphore(value=1)

                if self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]:

                    logger.info(
                        f"{self.premsg}:RESET[{self.n_reset}]: " +
                        f"waits for rest scenes in [{self.fromplns}] to start DL " +
                        f"[{self.vid_dl.info_dl['fromplns'][self.fromplns]['in_reset']}]")

                    #  self.vid_dl.reset_event.clear()
                    _ev_set = wait_for_either(
                        events=(self.vid_dl.info_dl["fromplns"][self.fromplns]["reset"], self.vid_dl.stop_event))

                    if _ev_set == 'stop':
                        return

                    # if _ev_set == 'reset':
                    #     self.vid_dl.reset_event.clear()
                    #     self.vid_dl.info_dl["fromplns"][self.fromplns]["reset"].set()
                    #     self.vid_dl.info_dl['fromplns'][self.fromplns]['in_reset'].clear()
                    #     self.vid_dl.info_dl["fromplns"][self.fromplns]["sem"] = threading.BoundedSemaphore(value=1)

                with AsyncHLSDownloader._CLASSLOCK:

                    try:
                        self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"].remove(self.fromplns)
                    except Exception:
                        pass

                    if not self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"]:

                        logger.info(
                            f"{self.premsg}:RESET[{self.n_reset}]: end for all plns ")

                        self.vid_dl.info_dl["fromplns"]["ALL"]["reset"].set()
                        self.vid_dl.info_dl["fromplns"]["ALL"]["sem"] = threading.BoundedSemaphore(value=100)

                        # NakedSwordBaseIE._STATUS = "NORMAL"
                        # AsyncHLSDownloader._COUNTDOWNS.clean()
                        # AsyncHLSDownloader._COUNTDOWNS = None
                        # AsyncHLSDownloader._COUNTDOWNS_READY.set()
                        # AsyncHLSDownloader._OK_503.clear()

                if self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"]:

                    logger.info(
                        f"{self.premsg}:RESET[{self.n_reset}]: " +
                        f"all scenes in [{self.fromplns}], waiting for scenes in other plns " +
                        f"[{self.vid_dl.info_dl['fromplns']['ALL']['in_reset']}]")

                    #  self.vid_dl.reset_event.clear()
                    _ev_set = wait_for_either(
                        events=(self.vid_dl.info_dl["fromplns"]["ALL"]["reset"], self.vid_dl.stop_event))

                    if _ev_set == 'stop':
                        return

                    #  if _ev_set == 'reset':
                    #     self.vid_dl.reset_event.clear()
                    #     self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"].clear()
                    #     self.vid_dl.info_dl["fromplns"]["ALL"]["reset"].set()
                    #     self.vid_dl.info_dl["fromplns"]["ALL"]["sem"] = threading.BoundedSemaphore(value=1)
                    #  NakedSwordBaseIE._STATUS = "NORMAL"
                    #  AsyncHLSDownloader._COUNTDOWNS.clean()
                    #  AsyncHLSDownloader._COUNTDOWNS = None
                    #  AsyncHLSDownloader._COUNTDOWNS_READY.set()
                    #  AsyncHLSDownloader._OK_503.clear()

            self.n_reset += 1
            logger.info(f"{self.premsg}:RESET[{self.n_reset}]: exit reset")

    def prep_reset(self, info_reset):

        self.info_dict.update(info_reset)

        #  self.headers = info_reset.get("http_headers")
        #  self.video_url = info_reset.get("url")
        self.init_client.close()
        _proxies = self._proxy if hasattr(self, '_proxy') else None
        self.init_client = httpx.Client(
            proxies=_proxies,  # type: ignore
            follow_redirects=True,
            headers=self.info_dict['http_headers'],
            limits=self.limits,
            timeout=self.timeout,
            verify=False
        )

        self.frags_to_dl = []

        byte_range = {}

        self.info_dict["fragments"] = self.get_info_fragments()

        for i, fragment in enumerate(self.info_dict["fragments"]):

            if not fragment.uri and fragment.parts:
                fragment.uri = fragment.parts[0].uri

            _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i}")

            if fragment.byterange:
                splitted_byte_range = fragment.byterange.split("@")
                sub_range_start = (
                    int(splitted_byte_range[1])
                    if len(splitted_byte_range) == 2
                    else byte_range["end"]
                )
                byte_range = {
                    "start": sub_range_start,
                    "end": sub_range_start + int(splitted_byte_range[0]),
                }

            else:
                byte_range = {}

            try:

                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="

                if not self.info_frag[i]["downloaded"] or (
                    self.info_frag[i]["downloaded"]
                    and not self.info_frag[i]["headersize"]
                ):
                    self.frags_to_dl.append(i + 1)
                    self.info_frag[i]["url"] = _url
                    self.info_frag[i]["file"] = _file_path
                    if (
                        not self.info_frag[i]["downloaded"]
                        and self.info_frag[i]["file"].exists()
                    ):
                        self.info_frag[i]["file"].unlink()

                    self.info_frag[i]["n_retries"] = 0
                    self.info_frag[i]["byterange"] = byte_range
                    self.info_frag[i]["key"] = fragment.key
                    if fragment.key and fragment.key.method == "AES-128":
                        if fragment.key.absolute_uri not in self.key_cache:
                            self.key_cache[
                                fragment.key.absolute_uri] = httpx.get(
                                fragment.key.absolute_uri, headers=self.info_dict['http_headers']
                            ).content

            except Exception:
                logger.debug(
                    f"{self.premsg}:RESET[{self.n_reset}]:prep_reset error " +
                    f"with i = [{i}] \n\ninfo_dict['fragments'] " +
                    f"{len(self.info_dict['fragments'])}\n\n" +
                    f"{[str(f) for f in self.info_dict['fragments']]}\n\n"
                    f"info_frag {len(self.info_frag)}")

                raise

        if not self.frags_to_dl:
            self.status = "init_manipulating"
        else:
            logger.debug(
                f"{self.premsg}:RESET[{self.n_reset}]:prep_reset:OK " +
                f"{self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}")

    async def check_speed(self):
        def getter(x):
            return x * CONF_HLS_SPEED_PER_WORKER

        _speed = []
        _num_chunks = self._CONF_HLS_MIN_N_TO_CHECK_SPEED
        _index = self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2
        _max_bd_det = 0
        prog = ProgressTimer()

        try:
            while True:

                _res = await async_waitfortasks(
                    self._qspeed.get(),
                    events=(self.vid_dl.reset_event,
                            self.vid_dl.stop_event),
                    background_tasks=self.background_tasks)
                if _res.get("event"):
                    logger.info(f"{self.premsg}[check_speed] event detected")
                    self._test.append(("event detected"))
                    return
                elif (_e := _res.get("exception")):
                    raise AsyncHLSDLError(f"couldnt get input spped: {repr(_e)}")
                else:
                    _input_speed = _res.get("result")
                    if not _input_speed:
                        continue

                if isinstance(
                    _input_speed, tuple) and isinstance(
                        _input_speed[0], str) and _input_speed[0] == "KILL":
                    return

                _speed.append(_input_speed)

                nsecs = 20

                if len(_speed) > _num_chunks:

                    if any(
                        [
                            all(
                                [
                                    el == 0
                                    for el in _speed[
                                        -(_index):
                                    ]
                                ]
                            ),
                            not _max_bd_det
                            and all(
                                [
                                    el < getter(self.count)
                                    for el in _speed[
                                        -(_index):
                                    ]
                                ]
                            ),
                        ]
                    ):

                        logger.info(f"{self.premsg}[check_speed] speed reset: n_el_speed[{len(_speed)}]")
                        self.vid_dl.reset_event.set("speedreset")
                        self._test.append(("speed reset"))
                        break

                    elif (
                        self._CONF_HLS_MAX_SPEED_PER_DL
                        and all(
                            [
                                el >= self._CONF_HLS_MAX_SPEED_PER_DL
                                for el in _speed[
                                    -(_index):
                                ]
                            ]
                        )
                        and (
                            not _max_bd_det
                            or (_max_bd_det and prog.elapsed_seconds() > nsecs)
                        )
                    ):

                        _old_throttle = self.throttle

                        if not _max_bd_det:
                            self.throttle = float(f"{self.throttle + 0.05:.2f}")
                            _max_bd_det += 1
                            prog.reset()
                            nsecs = 20

                        else:

                            _max_bd_det += 1
                            self.throttle = float(f"{self.throttle + 0.01:.2f}")
                            prog.reset()
                            nsecs = 20

                        _str_speed = ", ".join([
                            f"{el}" for el in _speed[
                                -(_index):]
                            ]
                        )
                        logger.info(
                            f"{self.premsg}[check_speed] MAX BD -> " +
                            f"decrease speed: throttle[{_old_throttle} -> " +
                            f"{self.throttle}] n_el_speed[{len(_speed)}]")
                        logger.debug(
                            f"{self.premsg}[check_speed] MAX BD -> " +
                            f"decrease speed: throttle[{_old_throttle} -> " +
                            f"{self.throttle}] n_el_speed[{len(_speed)}]\n" +
                            f"%no%{_str_speed}")

                        self._test.append((f"decrease speed: throttle {_old_throttle} -> {self.throttle}]"))

                    elif (
                        self._CONF_HLS_MAX_SPEED_PER_DL
                        and all(
                            [
                                0.8 * self._CONF_HLS_MAX_SPEED_PER_DL
                                <= el
                                < self._CONF_HLS_MAX_SPEED_PER_DL
                                for el in _speed[
                                    -(_index):
                                ]
                            ]
                        )
                        and (_max_bd_det and prog.elapsed_seconds() > nsecs)
                    ):

                        _str_speed = ", ".join(
                            [
                                f"{el}"
                                for el in _speed[
                                    -(_index):
                                ]
                            ]
                        )

                        logger.info(
                            f"{self.premsg}[check_speed] estable with throttle[{self.throttle}]: " +
                            f"n_el_speed[{len(_speed)}]")
                        logger.debug(
                            f"{self.premsg}[check_speed] estable with throttle" +
                            f"[{self.throttle}]: n_el_speed[{len(_speed)}]\n" +
                            f"%no%{_str_speed}")

                        _max_bd_det = 0
                        self._test.append((f"estable throttle {self.throttle}"))

                    elif (
                        self._CONF_HLS_MAX_SPEED_PER_DL
                        and all(
                            [
                                el < 0.8 * self._CONF_HLS_MAX_SPEED_PER_DL
                                for el in _speed[
                                    -(_index):
                                ]
                            ]
                        )
                        and (_max_bd_det and prog.elapsed_seconds() > nsecs)
                    ):

                        _old_throttle = self.throttle

                        if _max_bd_det == 1:
                            self.throttle = float(
                                f"{self.throttle - 0.01:.2f}")
                            prog.reset()
                            nsecs = 20
                        else:
                            self.throttle = float(
                                f"{self.throttle - 0.005:.2f}")
                            prog.reset()
                            nsecs = 20

                        if self.throttle < 0:
                            self.throttle = 0

                        _str_speed = ", ".join(
                            [
                                f"{el}"
                                for el in _speed[
                                    -(_index):
                                ]
                            ]
                        )
                        logger.info(
                            f"{self.premsg}[check_speed] MIN 0.8 -> " +
                            f"increase speed: throttle[{_old_throttle} -> " +
                            f"{self.throttle}] n_el_speed[{len(_speed)}]")
                        logger.debug(
                            f"{self.premsg}[check_speed] MIN 0.8 -> " +
                            f"increase speed: throttle[{_old_throttle} -> " +
                            f"{self.throttle}] n_el_speed[{len(_speed)}]\n" +
                            f"%no%{_str_speed}")

                        self._test.append((f"increase speed: throttle {_old_throttle} -> {self.throttle}]"))

                await asyncio.sleep(0)

        except Exception as e:
            logger.warning(f"{self.premsg}[check_speed] {repr(e)}")
        finally:
            self._speed.extend(_speed)
            logger.debug(f"{self.premsg}[check_speed] bye")

    async def upt_status(self):

        while not any([self.kill.is_set(), self.vid_dl.end_tasks.is_set()]):

            if self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):

                if self.first_data.is_set() and not any(
                    [
                        self.vid_dl.pause_event
                        and self.vid_dl.pause_event.is_set(),
                        self.vid_dl.reset_event
                        and self.vid_dl.reset_event.is_set(),
                        self.vid_dl.stop_event
                        and self.vid_dl.stop_event.is_set(),
                    ]
                ):

                    _down_size = self.down_size
                    _speed_meter = self.speedometer(_down_size)
                    self._test.append((time.monotonic(), _down_size, _speed_meter))

                    self.upt.update(
                        {
                            "speed_meter": _speed_meter,
                            "down_size": _down_size
                        })

                    if _speed_meter and self.filesize:

                        _est_time = (self.filesize - _down_size) / _speed_meter
                        _est_time_smooth = self.smooth_eta(_est_time)
                        self.upt.update(
                            {
                                "est_time": _est_time,
                                "est_time_smooth": _est_time_smooth
                            })

                        self._speed.append((datetime.now(), copy.deepcopy(self.upt)))

            await asyncio.sleep(0)

    async def fetch(self, nco):

        logger.debug(f"{self.premsg}:[worker-{nco}]: init worker")

        _proxies = self._proxy if hasattr(self, '_proxy') else None
        client = httpx.AsyncClient(
            proxies=_proxies,  # type: ignore
            limits=self.limits,
            follow_redirects=True,
            timeout=self.timeout,
            verify=self.verifycert,
            headers=self.info_dict['http_headers'],
        )

        try:

            while True:

                _res = await async_waitfortasks(
                    self.frags_queue.get(),
                    events=(self.vid_dl.reset_event, self.vid_dl.stop_event),
                    background_tasks=self.background_tasks)
                if _res.get("event"):
                    return
                elif (_e := _res.get("exception")):
                    raise AsyncHLSDLError(f'couldnt get frag from queue {repr(_e)}')
                else:
                    q = _res.get("result")
                    if q is None:
                        continue
                    elif isinstance(q, str) and q == "KILL":
                        logger.debug(f"{self.premsg}:[worker-{nco}]: KILL")
                        return

                assert isinstance(q, int)

                _premsg = f'{self.premsg}:[worker-{nco}]:[frag-{q}] '

                logger.debug(f'{_premsg}\n{self.info_frag[q - 1]}')

                if self.vid_dl.pause_event.is_set():
                    self._speed.append((datetime.now(), "pause"))
                    _res = await async_waitfortasks(
                        events=(self.vid_dl.resume_event, self.vid_dl.reset_event, self.vid_dl.stop_event),
                        background_tasks=self.background_tasks)
                    self.vid_dl.pause_event.clear()
                    self.vid_dl.resume_event.clear()
                    self._speed.append((datetime.now(), "resume"))

                    if _res.get("event") in ("stop", "reset"):
                        return

                url = self.info_frag[q - 1]["url"]
                filename = Path(self.info_frag[q - 1]["file"])
                filename_exists = await os.path.exists(filename)
                key = self.info_frag[q - 1]["key"]
                cipher = None
                if key is not None and key.method == "AES-128":
                    iv = binascii.unhexlify(key.iv[2:])
                    cipher = AES.new(self.key_cache[key.absolute_uri], AES.MODE_CBC, iv)
                byte_range = self.info_frag[q - 1].get("byterange")
                headers = {}
                if byte_range:
                    headers["range"] = f"bytes={byte_range['start']}-{byte_range['end'] - 1}"

                await asyncio.sleep(0)

                while self.info_frag[q - 1]["n_retries"] < self._MAX_RETRIES:

                    if any([self.vid_dl.stop_event.is_set(),
                            self.vid_dl.reset_event.is_set()]):
                        return

                    try:

                        async with self._limit:
                            logger.debug(f'{_premsg}: limiter speed')

                        async with (
                            aiofiles.open(filename, mode="ab") as f,
                            client.stream("GET", url, headers=headers) as res
                        ):

                            logger.debug(
                                f'{_premsg}:{res.request}, {res.status_code}, ' +
                                f'{res.reason_phrase}, {res.headers}')

                            if res.status_code == 403:

                                if self.fromplns:
                                    _wait_tasks = await self.vid_dl.reset_plns("403", plns=self.fromplns)
                                else:
                                    _wait_tasks = await self.vid_dl.reset("403")
                                logger.debug(
                                    f'{_premsg}: wait_tasks\n{_wait_tasks}\n{print_tasks(_wait_tasks)}')
                                if _wait_tasks:
                                    done, pending = await asyncio.wait(_wait_tasks)
                                    logger.debug(
                                        f'{_premsg}:wait_tasks result\nDONE\n{done}\nPENDING\n{pending}')
                                return

                            elif res.status_code >= 400:

                                raise AsyncHLSDLError(f'Frag:{str(q)} resp code:{str(res)}')

                            else:

                                if not self.info_frag[q - 1]["headersize"]:
                                    self.info_frag[q - 1]["headersize"] = int_or_none(res.headers.get("content-length"))

                                _hsize = self.info_frag[q - 1]["headersize"]

                                if not self.filesize and _hsize:
                                    async with self._LOCK:
                                        self.filesize = _hsize * \
                                            len(self.info_dict[
                                                "fragments"])
                                    async with self.vid_dl.alock:
                                        self.vid_dl.info_dl[
                                            "filesize"] += self.filesize

                                if self.info_frag[q - 1]["downloaded"]:

                                    if filename_exists:
                                        _size = self.info_frag[q - 1][
                                            "size"] = (await os.stat(
                                                filename)).st_size
                                        if _hsize and (
                                            _hsize - 100
                                            <= _size
                                            <= _hsize + 100
                                        ) or not _hsize:

                                            logger.debug(f'{_premsg}:DL-hsize[{_hsize}] size [{_size}]')
                                            break
                                        else:
                                            await f.truncate(0)
                                            self.info_frag[q - 1]["downloaded"] = False
                                            async with self._LOCK:
                                                self.n_dl_fragments -= 1
                                                self.down_size -= _size
                                            async with self.vid_dl.alock:
                                                self.vid_dl.info_dl["down_size"] -= _size
                                    else:
                                        logger.warning(
                                            f'{_premsg}: frag with mark downloaded but file ' +
                                            f'[{filename}] doesnt exists')

                                        self.info_frag[q - 1]["downloaded"] = False
                                        async with self._LOCK:
                                            self.n_dl_fragments -= 1

                                if (self.info_frag[q - 1]["headersize"] and
                                   self.info_frag[q - 1]["headersize"] < self._CHUNK_SIZE):

                                    _chunk_size = self.info_frag[q - 1]["headersize"]

                                else:
                                    _chunk_size = self._CHUNK_SIZE

                                num_bytes_downloaded = res.num_bytes_downloaded

                                self.info_frag[q - 1]["time2dlchunks"] = []
                                self.info_frag[q - 1]["sizechunks"] = []
                                self.info_frag[q - 1]["nchunks_dl"] = 0

                                _started = time.monotonic()

                                async for chunk in res.aiter_bytes(chunk_size=_chunk_size):

                                    if self.vid_dl.pause_event.is_set():
                                        self._speed.append((datetime.now(), "pause"))
                                        await async_waitfortasks(
                                            events=(
                                                    self.vid_dl.resume_event,
                                                    self.vid_dl.reset_event,
                                                    self.vid_dl.stop_event
                                            ),
                                            background_tasks=self.background_tasks)
                                        self.vid_dl.pause_event.clear()
                                        self.vid_dl.resume_event.clear()
                                        self._speed.append((datetime.now(), "resume"))

                                    _timechunk = time.monotonic() - _started
                                    self.info_frag[q - 1]["time2dlchunks"].append(_timechunk)
                                    if cipher:
                                        data = cipher.decrypt(chunk)
                                    else:
                                        data = chunk

                                    await f.write(data)

                                    _dif = 0
                                    async with self._LOCK:
                                        self.down_size += (_iter_bytes := (
                                            res.num_bytes_downloaded - num_bytes_downloaded))
                                        if self.filesize and (_dif := self.down_size - self.filesize) > 0:
                                            self.filesize += _dif
                                        self.first_data.set()

                                    async with self.vid_dl.alock:
                                        if _dif > 0:
                                            self.vid_dl.info_dl["filesize"] += _dif
                                        self.vid_dl.info_dl["down_size"] += _iter_bytes

                                    num_bytes_downloaded = res.num_bytes_downloaded
                                    self.info_frag[q - 1]["nchunks_dl"] += 1
                                    self.info_frag[q - 1]["sizechunks"].append(_iter_bytes)

                                    if self.throttle:
                                        await async_wait_time(self.throttle)
                                    else:
                                        await asyncio.sleep(0)
                                    _started = time.monotonic()

                        _size = (await os.stat(filename)).st_size
                        _hsize = self.info_frag[q - 1]["headersize"]

                        if (_hsize and _hsize - 100 <= _size <= _hsize + 100) or not _hsize:
                            self.info_frag[q - 1]["downloaded"] = True
                            self.info_frag[q - 1]["size"] = _size
                            async with self._LOCK:
                                self.n_dl_fragments += 1
                            logger.debug(
                                f'{_premsg}: OK DL: total' +
                                f'[{self.n_dl_fragments}]\n' +
                                f'{self.info_frag[q - 1]}')
                            break
                        else:
                            logger.warning(
                                f'{_premsg}: end of streaming. Fragment not completed\n' +
                                f'{self.info_frag[q - 1]}')
                            raise AsyncHLSDLError(f"fragment not completed frag[{q}]")

                    except AsyncHLSDLErrorFatal as e:

                        logger.debug(f"{_premsg}: errorfatal {repr(e)}")
                        self.info_frag[q - 1]["error"].append(repr(e))
                        self.info_frag[q - 1]["downloaded"] = False
                        if await os.path.exists(filename):
                            _size = (await os.stat(filename)).st_size
                            await os.remove(filename)

                            async with self._LOCK:
                                self.down_size -= _size
                            async with self.vid_dl.alock:
                                self.vid_dl.info_dl[
                                    "down_size"] -= _size
                        return
                    except (
                        asyncio.exceptions.CancelledError,
                        asyncio.CancelledError,
                        CancelledError,
                    ) as e:
                        self.info_frag[q - 1]["error"].append(repr(e))
                        self.info_frag[q - 1]["downloaded"] = False

                        logger.debug(f"{_premsg}: CancelledError: {repr(e)}")
                        if await os.path.exists(filename):
                            _size = (await os.stat(filename)).st_size
                            await os.remove(filename)
                            async with self._LOCK:
                                self.down_size -= _size
                            async with self.vid_dl.alock:
                                self.vid_dl.info_dl[
                                    "down_size"] -= _size
                        return
                    except RuntimeError as e:
                        self.info_frag[q - 1]["error"].append(repr(e))
                        self.info_frag[q - 1]["downloaded"] = False

                        if await os.path.exists(filename):
                            _size = (await os.stat(filename)).st_size
                            await os.remove(filename)

                            async with self._LOCK:
                                self.down_size -= _size
                            async with self.vid_dl.alock:
                                self.vid_dl.info_dl["down_size"] -= _size

                        logger.exception(f"{_premsg}: runtime error: {repr(e)}")
                        return

                    except Exception as e:
                        if any([_ in (str(e.__class__)).lower() for _ in ("httpx", "asynchlsdl")]):
                            logger.debug(f"{_premsg}: error {repr(e)}")
                        else:
                            logger.exception(f"{_premsg}: error {repr(e)}")

                        self.info_frag[q - 1]["error"].append(repr(e))
                        self.info_frag[q - 1]["downloaded"] = False
                        self.info_frag[q - 1]["n_retries"] += 1
                        if await os.path.exists(filename):
                            _size = (await os.stat(filename)).st_size
                            await os.remove(filename)
                            async with self._LOCK:
                                self.down_size -= _size
                            async with self.vid_dl.alock:
                                self.vid_dl.info_dl["down_size"] -= _size

                        if any(
                            [
                                self.vid_dl.stop_event.is_set(),
                                self.vid_dl.reset_event.is_set(),
                            ]
                        ):
                            return

                        if self.info_frag[q - 1]["n_retries"] < self._MAX_RETRIES:

                            await async_wait_time(random.choice([i for i in range(1, 5)]))
                            await asyncio.sleep(0)

                        else:
                            self.info_frag[q - 1]["error"].append("MaxLimitRetries")
                            logger.warning(f"{_premsg}: MaxLimitRetries:skip")
                            self.info_frag[q - 1]["skipped"] = True
                            break
                    finally:
                        await asyncio.sleep(0)

        finally:
            async with self._LOCK:
                self.count -= 1
            logger.debug(f"{self.premsg}:[worker{nco}]: bye worker")
            await client.aclose()

    async def fetch_async(self):

        self._LOCK = asyncio.Lock()
        self.first_data = asyncio.Event()
        self.kill = asyncio.Event()
        self.frags_queue = asyncio.Queue()

        self.areset = sync_to_async(self.resetdl, executor=self.ex_dl)

        for frag in self.frags_to_dl:
            self.frags_queue.put_nowait(frag)

        for _ in range(self.n_workers):
            self.frags_queue.put_nowait("KILL")

        self._speed = []
        n_frags_dl = 0
        self.premsg = f'[{self.vid_dl.index}]{self.premsg}'
        AsyncHLSDownloader._QUEUE[str(self.vid_dl.index)] = Queue()

        if self.fromplns:
            _event = traverse_obj(self.vid_dl.info_dl["fromplns"], ("ALL", "reset"))
            if _event:
                await _event.async_wait()  # type: ignore
            self.vid_dl.info_dl["fromplns"][self.fromplns]["downloading"].add(
                self.vid_dl.info_dict["playlist_index"])
            self.vid_dl.info_dl["fromplns"]["ALL"]["downloading"].add(self.fromplns)

        _tstart = time.monotonic()

        try:

            while True:

                logger.debug(f"{self.premsg} TASKS INIT")

                try:
                    self.count = self.n_workers
                    self.vid_dl.reset_event.clear()
                    self.vid_dl.pause_event.clear()
                    self.vid_dl.resume_event.clear()
                    self.vid_dl.end_tasks.clear()
                    self.kill.clear()
                    self.first_data.clear()
                    self._qspeed = asyncio.Queue()
                    self.speedometer = SpeedometerMA(
                        initial_bytes=self.down_size)
                    self.progress_timer = ProgressTimer()
                    self.smooth_eta = SmoothETA()
                    self._test.append(("starting tasks to dl"))
                    self.status = "downloading"
                    self.count_msg = ''

                    upt_task = [asyncio.create_task(self.upt_status())]
                    self.background_tasks.add(upt_task[0])
                    upt_task[0].add_done_callback(self.background_tasks.discard)
                    check_task = []
                    self.tasks = [
                        asyncio.create_task(self.fetch(i), name=f"{self.premsg}[{i}]")
                        for i in range(self.n_workers)
                    ]
                    for _task in self.tasks:
                        self.background_tasks.add(_task)
                        _task.add_done_callback(self.background_tasks.discard)

                    done, pending = await asyncio.wait(self.tasks)

                    logger.debug(
                        f'{self.premsg}[fetch_async]: done[{len(list(done))}] ' +
                        f'pending[{len(list(pending))}]')

                    _nfragsdl = len(self.fragsdl())
                    inc_frags_dl = _nfragsdl - n_frags_dl
                    n_frags_dl = _nfragsdl

                    if n_frags_dl == len(self.info_dict["fragments"]):

                        await self.clean_from_reset()
                        self.vid_dl.end_tasks.set()
                        self._qspeed.put_nowait("KILL")
                        self.kill.set()
                        await asyncio.sleep(0)
                        await asyncio.wait(check_task + upt_task)
                        break

                    else:
                        if self.vid_dl.stop_event.is_set():

                            await self.clean_from_reset()
                            self.vid_dl.end_tasks.set()
                            self.status = "stop"
                            await asyncio.wait(check_task + upt_task)
                            return

                        else:

                            self.vid_dl.end_tasks.set()

                            if _cause := self.vid_dl.reset_event.is_set():

                                dump_init_task = [asyncio.create_task(self.dump_init_file())]
                                self.background_tasks.add(dump_init_task[0])
                                dump_init_task[0].add_done_callback(self.background_tasks.discard)

                                await asyncio.wait(dump_init_task + check_task + upt_task)

                                if self.n_reset < self._MAX_RESETS:

                                    if _cause in ("403", "hard"):
                                        if self.fromplns:
                                            await self.vid_dl.back_from_reset_plns(self.premsg)
                                            if self.vid_dl.stop_event.is_set():
                                                await self.clean_from_reset()
                                                return
                                        _cause = self.vid_dl.reset_event.is_set()
                                        self._speed.append((datetime.now(), _cause))
                                        logger.info(f'{self.premsg}:RESET[{self.n_reset}]:CAUSE[{_cause}]')

                                    elif _cause == "manual":
                                        self._speed.append((datetime.now(), _cause))
                                        logger.info(f'{self.premsg}:RESET[{self.n_reset}]:CAUSE[{_cause}]')
                                        continue

                                    try:
                                        async with self._limit_reset:
                                            await self.areset(_cause)
                                        if self.vid_dl.stop_event.is_set():
                                            return
                                        self.frags_queue = asyncio.Queue()
                                        for frag in self.frags_to_dl:
                                            self.frags_queue.put_nowait(frag)
                                        if ((_t := time.monotonic()) - _tstart) < self._MIN_TIME_RESETS:
                                            _minus = self.n_workers // 4
                                            self.n_workers -= _minus
                                        _tstart = _t
                                        for _ in range(self.n_workers):
                                            self.frags_queue.put_nowait("KILL")
                                        logger.debug(
                                            f'{self.premsg}:RESET[{self.n_reset}]:OK:Pending frags\n' +
                                            f'{len(self.fragsnotdl())}')
                                        await asyncio.sleep(0)
                                        continue

                                    except Exception as e:
                                        logger.exception(
                                            f'{self.premsg}:RESET[{self.n_reset}]:' +
                                            f'ERROR reset couldnt progress:[{repr(e)}]')
                                        self.status = "error"
                                        await self.clean_when_error()
                                        raise AsyncHLSDLErrorFatal(
                                            f'{self.premsg}: ERROR reset couldnt progress')

                                else:

                                    logger.warning(
                                        f'{self.premsg}:RESET[{self.n_reset}]:ERROR:Max_number_of_resets')
                                    self.status = "error"
                                    await self.clean_when_error()
                                    await asyncio.sleep(0)
                                    raise AsyncHLSDLErrorFatal(f"{self.premsg}: ERROR max resets")

                            else:

                                self._qspeed.put_nowait("KILL")
                                if check_task:
                                    await asyncio.wait(check_task)
                                if inc_frags_dl > 0:

                                    logger.debug(
                                        f'{self.premsg}: [{n_frags_dl} -> ' +
                                        f'{inc_frags_dl}] new cycle with no fatal error')
                                    try:
                                        async with self._limit_reset:
                                            await self.areset()
                                        if self.vid_dl.stop_event.is_set():
                                            return
                                        self.frags_queue = asyncio.Queue()
                                        for frag in self.frags_to_dl:
                                            self.frags_queue.put_nowait(frag)
                                        for _ in range(self.n_workers):
                                            self.frags_queue.put_nowait("KILL")
                                        logger.debug(
                                            f'{self.premsg}:RESET new cycle' +
                                            f'[{self.n_reset}]:OK:Pending frags ' +
                                            f'{len(self.fragsnotdl())}')
                                        self.n_reset -= 1
                                        continue

                                    except Exception as e:
                                        logger.exception(
                                            f'{self.premsg}:RESET' +
                                            f'[{self.n_reset}]:ERROR reset ' +
                                            f'couldnt progress:[{repr(e)}]')
                                        self.status = "error"
                                        await self.clean_when_error()
                                        await asyncio.sleep(0)
                                        raise AsyncHLSDLErrorFatal(
                                            f'{self.premsg}: ERROR reset couldnt progress')

                                else:
                                    logger.debug(
                                        f'{self.premsg}: [{n_frags_dl} <-> ' +
                                        f'{inc_frags_dl}] no improvement, ' +
                                        'lets raise an error"')
                                    self.status = "error"
                                    raise AsyncHLSDLErrorFatal(
                                        f'{self.premsg}: no changes in number ' +
                                        'of dl frags in one cycle')

                except AsyncHLSDLErrorFatal:
                    raise
                except Exception as e:
                    logger.exception(f"{self.premsg}[fetch_async] error {repr(e)}")
                finally:
                    await asyncio.sleep(0)

        except Exception as e:
            logger.error(f"{self.premsg} error {repr(e)}")
            self.status = "error"
        finally:
            await self.dump_init_file()
            if self.fromplns:
                _downloading = self.vid_dl.info_dl["fromplns"][self.fromplns]["downloading"]
                try:
                    _downloading.remove(self.vid_dl.info_dict["playlist_index"])
                except Exception:
                    logger.warning(
                        f'{self.premsg}[fetch_async] error when removing ' +
                        f'[{self.vid_dl.info_dict["playlist_index"]}] ' +
                        f'from downloading [{_downloading}]')

                if not self.vid_dl.info_dl["fromplns"][self.fromplns]["downloading"]:
                    self.vid_dl.info_dl["fromplns"]["ALL"]["downloading"].remove(self.fromplns)

            logger.debug(f'{self.premsg}%no%\n\n{json.dumps(self._test)}')

            self.init_client.close()
            logger.debug(f'{self.premsg}:Frags DL completed')
            if (
                not self.vid_dl.stop_event.is_set()
                and not self.status == "error"
            ):
                self.status = "init_manipulating"

    async def clean_from_reset(self):

        if not self.fromplns:
            return

        try:
            try:
                self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"].remove(
                    self.vid_dl.info_dict["playlist_index"])
            except Exception:
                logger.warning(
                    f'{self.premsg}[clean_from_reset] error when removing ' +
                    f'[{self.vid_dl.info_dict["playlist_index"]}] from ' +
                    f'{self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]}')

            if not self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]:
                self.vid_dl.info_dl["fromplns"][self.fromplns]["reset"].set()
                logger.info(f'{self.premsg} end of resets fromplns [{self.fromplns}]')

                try:

                    self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"].remove(self.fromplns)

                except Exception:
                    logger.warning(
                        f'{self.premsg}[clean_from_reset] error when removing ' +
                        f'[{self.fromplns}] from ' +
                        f'{self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"]}')

                if not self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"]:
                    self.vid_dl.info_dl["fromplns"]["ALL"]["reset"].set()

        except Exception:
            logger.warning(
                f'{self.premsg}[clean_from_reset] error when removing ' +
                f'[{self.vid_dl.info_dict["playlist_index"]}] from' +
                f'{self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]}')

    async def dump_init_file(self):
        init_data = {el["frag"]: el["headersize"] for el in self.info_frag if el["headersize"]}

        async with aiofiles.open(self.init_file, mode="w") as f:
            await f.write(json.dumps(init_data))

        logger.debug(f'{self.premsg} init data\n{init_data}')

    async def clean_when_error(self):
        for f in self.info_frag:
            if f["downloaded"] is False and await os.path.exists(f["file"]):
                await os.remove(f["file"])

    def sync_clean_when_error(self):
        for f in self.info_frag:
            if f["downloaded"] is False and f["file"].exists():
                f["file"].unlink()

    async def ensamble_file(self):
        self.status = "manipulating"
        logger.debug(f"{self.premsg}:{self.filename} Fragments DL \n{self.fragsdl()}")
        _skipped = 0
        try:

            async with aiofiles.open(self.filename, mode="wb") as dest:

                for f in self.info_frag:
                    if f.get("skipped", False):
                        _skipped += 1
                        continue
                    if not f["size"] and await os.path.exists(f["file"]):
                        f["size"] = (await os.stat(f["file"])).st_size

                    if (f["size"] and (
                        (f["headersize"] and (f["headersize"] - 100 <= f["size"] <= f["headersize"] + 100)) or
                            not f["headersize"])):

                        async with aiofiles.open(f["file"], mode="rb") as source:
                            await dest.write(await source.read())
                    else:
                        raise AsyncHLSDLError(f'{self.premsg}: error when ensambling: {f}')

        except Exception as e:
            if await os.path.exists(self.filename):
                await os.remove(self.filename)
            logger.exception(f'{self.premsg}:Exception ocurred: {repr(e)}')
            self.status = "error"
            self.sync_clean_when_error()
            raise
        finally:
            if await os.path.exists(self.filename):
                armtree = sync_to_async(
                    partial(rmtree, ignore_errors=True), executor=self.ex_dl)
                await armtree(str(self.download_path))
                self.status = "done"
                logger.debug(
                    f'{self.premsg}: [ensamble_file] file ensambled')
                if _skipped:
                    logger.warning(
                        f'{self.premsg}: [ensamble_file] skipped frags [{_skipped}]')
            else:
                self.status = "error"
                await self.clean_when_error()
                raise AsyncHLSDLError(
                    f'{self.premsg}: error when ensambling parts')

    def fragsnotdl(self):
        res = []
        for frag in self.info_frag:
            if (frag["downloaded"] is False) and not frag.get(
                    "skipped", False):
                res.append(frag)
        return res

    def fragsdl(self):
        res = []
        for frag in self.info_frag:
            if (frag["downloaded"] is True) or frag.get("skipped", False):
                res.append(frag)
        return res

    def print_hookup(self):

        def format_frags():
            import math
            return f"{(int(math.log(self.n_total_fragments, 10)) + 1)}d"

        _filesize_str = naturalsize(self.filesize) if self.filesize else "--"
        _proxy = self._proxy["http://"].split(":")[-1] if hasattr(
            self, '_proxy') else None

        if self.status == "done":
            return f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] Completed\n"

        elif self.status == "init":
            return ''.join([f'[HLS][{self.info_dict["format_id"]}]: PROXY[{_proxy}] ',
                            f'Waiting to DL [{_filesize_str}] ',
                            f'[{self.n_dl_fragments:{format_frags()}}/{self.n_total_fragments}]\n'])

        elif self.status == "error":
            _rel_size_str = (
                f"{naturalsize(self.down_size)}/{naturalsize(self.filesize)}"
                if self.filesize
                else "--"
            )
            return ''.join([f'[HLS][{self.info_dict["format_id"]}]: PROXY[{_proxy}] ',
                            f'ERROR [{_rel_size_str}] ',
                            f'[{self.n_dl_fragments:{format_frags()}}/{self.n_total_fragments}]\n'])

        elif self.status == "stop":
            _rel_size_str = (
                f"{naturalsize(self.down_size)}/{naturalsize(self.filesize)}"
                if self.filesize
                else "--"
            )
            return ''.join([f'[HLS][{self.info_dict["format_id"]}]: PROXY[{_proxy}] ',
                            f'STOPPED [{_rel_size_str}] ',
                            f'[{self.n_dl_fragments:{format_frags()}}/{self.n_total_fragments}]\n'])

        elif self.status == "downloading":

            _eta_smooth_str = "--"
            _speed_meter_str = "--"

            _temp = copy.deepcopy(self.upt)

            if not any(
                [
                    self.vid_dl.pause_event.is_set(),
                    self.vid_dl.reset_event.is_set(),
                    self.vid_dl.stop_event.is_set(),
                ]
            ):

                if _speed_meter := _temp.get("speed_meter"):
                    _speed_meter_str = f"{naturalsize(_speed_meter)}ps"
                else:
                    _speed_meter_str = "--"

                if (
                    _est_time_smooth := _temp.get("est_time_smooth")
                ) and _est_time_smooth < 3600:

                    _eta_smooth_str = ":".join(
                        [_item.split(".")[0]
                            for _item in
                            f"{timedelta(seconds=_est_time_smooth)}".split(
                                ":"
                            )[1:]]
                    )
                else:
                    _eta_smooth_str = "--"

            else:

                if self.vid_dl.reset_event.is_set():
                    try:
                        self.count_msg = AsyncHLSDownloader._QUEUE[str(self.vid_dl.index)].get_nowait()
                    except Exception:
                        pass

            _dsize = _temp.get("down_size", self.down_size)
            _progress_str = (
                f'{(_dsize/self.filesize)*100:5.2f}%'
                if self.filesize
                else "-----"
            )

            return ''.join([f'[HLS][{self.info_dict["format_id"]}]: PROXY[{_proxy}] ',
                            f'WK[{self.count:2d}/{self.n_workers:2d}] ',
                            f'FR[{self.n_dl_fragments:{format_frags()}}/{self.n_total_fragments}]',
                            f'PR[{_progress_str}] DL[{_speed_meter_str}] ETA[{_eta_smooth_str}]',
                            f'\n{self.count_msg}'])

        elif self.status == "init_manipulating":
            return ''.join([
                f'[HLS][{self.info_dict["format_id"]}]: ',
                'Waiting for Ensambling \n'])

        elif self.status == "manipulating":
            if self.filename.exists():
                _size = self.filename.stat().st_size
            else:
                _size = 0
            _str = (
                f'[{naturalsize(_size)}/{naturalsize(self.filesize)}]' +
                f'({(_size/self.filesize)*100:.2f}%)'
                if self.filesize
                else f"[{naturalsize(_size)}]"
            )
            return f"[HLS][{self.info_dict['format_id']}]: Ensambling {_str}\n"
