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
import sys
import threading
import time
import traceback
from concurrent.futures import CancelledError, ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from shutil import rmtree
from urllib.parse import urlparse
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
    CONF_PROXIES_N_GR_VIDEO,
    CONFIG_EXTRACTORS,
    NakedSwordBaseIE,
    ProgressTimer,
    ProxyYTDL,
    SmoothETA,
    SpeedometerMA,
    StatusStop,
    _for_print,
    _for_print_entry,
    async_ex_in_executor,
    async_wait_time,
    cmd_extract_info,
    dec_retry_error,
    get_domain,
    get_format_id,
    int_or_none,
    limiter_15,
    limiter_non,
    my_dec_on_exception,
    naturalsize,
    print_norm_time,
    print_tasks,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    try_get,
    unsmuggle_url,
    wait_for_change_ip,
    wait_time,
)

logger = logging.getLogger("async_HLS_DL")

retry = my_dec_on_exception(Exception, max_tries=3, raise_on_giveup=True, interval=1)


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

    def __init__(self, enproxy, video_dict, vid_dl):

        try:

            self._test = []
            self.info_dict = video_dict.copy()
            self.video_downloader = vid_dl
            self.enproxy = enproxy != 0
            self.n_workers = self.video_downloader.info_dl["n_workers"]
            self.count = 0  # cuenta de los workers activos haciendo DL. Al comienzo serán igual a n_workers
            self.video_url = self.info_dict.get("url")  # url del format
            self.webpage_url = self.info_dict.get("webpage_url")  # url de la web
            self.m3u8_doc = None
            self.id = self.info_dict["id"]
            self.ytdl = self.video_downloader.info_dl["ytdl"]
            self.verifycert = not self.ytdl.params.get("nocheckcertificate")
            self.timeout = httpx.Timeout(30, connect=30)
            self.limits = httpx.Limits(
                max_keepalive_connections=None,
                max_connections=None,
                keepalive_expiry=30,
            )
            self.headers = self.info_dict.get("http_headers")

            self.base_download_path = Path(str(self.info_dict["download_path"]))

            self.init_file = Path(
                self.base_download_path, f"init_file.{self.info_dict['format_id']}"
            )

            _filename = self.info_dict.get("_filename") or self.info_dict.get(
                "filename"
            )
            self.download_path = Path(
                self.base_download_path, self.info_dict["format_id"]
            )
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.fragments_base_path = Path(
                self.download_path,
                _filename.stem
                + "."
                + self.info_dict["format_id"]
                + "."
                + self.info_dict["ext"],
            )
            self.filename = Path(
                self.base_download_path,
                _filename.stem + "." + self.info_dict["format_id"] + "." + "ts",
            )

            self.key_cache = dict()

            self.n_reset = 0

            self.throttle = 0
            self.down_size = 0
            self.status = "init"
            self.error_message = ""
            self.upt = {}

            self._qspeed = None
            self.ex_hlsdl = ThreadPoolExecutor(thread_name_prefix="ex_hlsdl")

            self._proxy = None

            if self.enproxy:

                self._qproxies = Queue()
                for el1, el2 in zip(
                    random.sample(
                        range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST
                    ),
                    random.sample(
                        range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST
                    ),
                ):
                    self._qproxies.put_nowait((el1, el2))

            self.init_client = httpx.Client(
                proxies=self._proxy,
                follow_redirects=True,
                headers=self.headers,
                limits=self.limits,
                timeout=self.timeout,
                verify=False,
            )

            self.filesize = None

            self.premsg = f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]"

            self.init()

        except Exception as e:
            logger.exception(repr(e))
            self.init_client.close()

    def init(self):
        def getter(x):
            try:
                if "nakedsword" in x:
                    self._CONF_HLS_MAX_SPEED_PER_DL = 10 * 1048576
                    self.auto_pasres = True
                    logger.debug(
                        f"{self.premsg}[{self.count}/{self.n_workers}] {_for_print_entry(self.info_dict)}"
                    )
                    if self.info_dict.get("playlist_title", "") not in (
                        "MostWatchedScenes",
                        "Search",
                    ):
                        self.fromplns = self.info_dict.get("playlist_id", False)

                    if self.fromplns:
                        if not self.video_downloader.info_dl["fromplns"].get("ALL"):
                            _all_temp = threading.Event()
                            _all_temp.set()
                            self.video_downloader.info_dl["fromplns"]["ALL"] = {
                                "sem": threading.BoundedSemaphore(value=1),
                                "downloading": set(),
                                "in_reset": set(),
                                "reset": _all_temp,
                            }
                        if not self.video_downloader.info_dl["fromplns"].get(
                            self.fromplns
                        ):
                            temp = threading.Event()
                            temp.set()

                            self.video_downloader.info_dl["fromplns"][self.fromplns] = {
                                "downloaders": {
                                    self.video_downloader.info_dict[
                                        "playlist_index"
                                    ]: self.video_downloader
                                },
                                "downloading": set(),
                                "in_reset": set(),
                                "reset": temp,
                                "sem": threading.BoundedSemaphore(value=1),
                            }
                        else:
                            self.video_downloader.info_dl["fromplns"][self.fromplns][
                                "downloaders"
                            ].update(
                                {
                                    self.video_downloader.info_dict[
                                        "playlist_index"
                                    ]: self.video_downloader
                                }
                            )

                        logger.info(
                            f"{self.premsg}[{self.count}/{self.n_workers}]: added new dl to plns [{self.fromplns}], count [{len(self.video_downloader.info_dl['fromplns'][self.fromplns]['downloaders'])}] members[{list(self.video_downloader.info_dl['fromplns'][self.fromplns]['downloaders'].keys())}]"
                        )
            except Exception as e:
                logger.exception(
                    f"{self.premsg}[{self.count}/{self.n_workers}]: {str(e)}"
                )

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

            self._extractor = try_get(
                self.info_dict.get("extractor_key").lower(), lambda x: x.lower()
            )

            self._limit = getter(self._extractor)
            self.info_frag = []
            self.info_init_section = {}
            self.frags_to_dl = []
            self.n_dl_fragments = 0

            self.tbr = self.info_dict.get(
                "tbr", 0
            )  # for audio streams tbr is not present
            self.abr = self.info_dict.get("abr", 0)
            _br = self.tbr or self.abr

            part = 0
            uri_ant = ""
            byte_range = {}

            self.info_dict["fragments"] = self.get_info_fragments()
            self.info_dict["init_section"] = self.info_dict["fragments"][0].init_section

            logger.debug(
                f"fragments: \n{[str(f) for f in self.info_dict['fragments']]}"
            )
            logger.debug(f"init_section: \n{self.info_dict['init_section']}")

            if _frag := self.info_dict["init_section"]:
                _file_path = Path(str(self.fragments_base_path) + ".Frag0")
                _url = _frag.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="
                if _frag.key is not None and _frag.key.method == "AES-128":
                    if _frag.key.absolute_uri not in self.key_cache:
                        self.key_cache.update(
                            {
                                _frag.key.absolute_uri: httpx.get(
                                    _frag.key.absolute_uri, headers=self.headers
                                ).content
                            }
                        )
                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}]:{self.key_cache[_frag.key.absolute_uri]}"
                        )
                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}]:{_frag.key.iv}"
                        )
                self.get_init_section(_url, _file_path, _frag.key)
                self.info_init_section.update(
                    {"frag": 0, "url": _url, "file": _file_path, "downloaded": True}
                )

            if self.init_file.exists():
                with open(self.init_file, "r") as f:
                    init_data = json.loads(f.read())

                init_data = {int(k): v for k, v in init_data.items()}
            else:
                init_data = {}

            for i, fragment in enumerate(self.info_dict["fragments"]):

                if fragment.byterange:
                    if fragment.uri == uri_ant:
                        part += 1
                    else:
                        part = 1

                    uri_ant = fragment.uri

                    _file_path = Path(
                        f"{str(self.fragments_base_path)}.Frag{i}.part.{part}"
                    )
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
                    _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i}")
                    byte_range = {}

                est_size = int(_br * fragment.duration * 1000 / 8)

                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="

                if _file_path.exists():
                    size = _file_path.stat().st_size
                    hsize = init_data.get(i + 1)
                    is_dl = True
                    if not size or ((size and hsize) and not (hsize - 100 <= size <= hsize + 100)):
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
                    if is_dl: self.down_size += size
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

                if fragment.key is not None and fragment.key.method == "AES-128":
                    if fragment.key.absolute_uri not in self.key_cache:
                        self.key_cache.update(
                            {
                                fragment.key.absolute_uri: httpx.get(
                                    fragment.key.absolute_uri, headers=self.headers
                                ).content
                            }
                        )
                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}]:{self.key_cache[fragment.key.absolute_uri]}"
                        )
                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}]:{fragment.key.iv}"
                        )

            logger.info(
                f"{self.premsg}[{self.count}/{self.n_workers}]: Frags already DL: {len(self.fragsdl())}, Frags not DL: {len(self.fragsnotdl())}, Frags to request: {len(self.frags_to_dl)}"
            )
            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]: \nFrags DL: {self.fragsdl()}\nFrags not DL: {self.fragsnotdl()}"
            )

            self.n_total_fragments = len(self.info_dict["fragments"])

            self.totalduration = self.info_dict.get("duration")
            if not self.totalduration:
                self.calculate_duration()  # get total duration
            self.filesize = self.info_dict.get("filesize") or self.info_dict.get(
                "filesize_approx"
            )
            if not self.filesize:
                self.calculate_filesize()  # get filesize estimated

            self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 120

            if not self.filesize:
                _est_size = "NA"

            else:
                if (self.filesize - self.down_size) < 250000000:
                    self.n_workers = min(self.n_workers, 8)
                elif 250000000 <= (self.filesize - self.down_size) < 500000000:
                    self.n_workers = min(self.n_workers, 16)
                elif 500000000 <= (self.filesize - self.down_size) < 1250000000:
                    self.n_workers = min(self.n_workers, 32)
                    # self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 90
                else:
                    self.n_workers = min(self.n_workers, 64)
                    # self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 120

                _est_size = naturalsize(self.filesize)

            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]: total duration {print_norm_time(self.totalduration)} -- estimated filesize {_est_size} -- already downloaded {naturalsize(self.down_size)} -- total fragments {self.n_total_fragments} -- fragments already dl {self.n_dl_fragments}"
            )

            if self.filename.exists() and self.filename.stat().st_size > 0:
                self.status = "done"

            elif not self.frags_to_dl:
                self.status = "init_manipulating"
        except Exception as e:
            logger.exception(
                f"{self.premsg}[{self.count}/{self.n_workers}][init] {repr(e)}"
            )
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

            self.m3u8_obj = self.get_m3u8_obj()

            if not self.m3u8_obj or not self.m3u8_obj.segments:
                raise AsyncHLSDLError("couldnt get m3u8 file")

            # self.cookies = self.init_client.cookies.jar.__dict__['_cookies']
            if self.m3u8_obj.keys:
                for _key in self.m3u8_obj.keys:
                    if _key and _key.method != "AES-128":
                        logger.warning(f"key AES method: {_key.method}")

            return self.m3u8_obj.segments

        except Exception as e:
            logger.error(
                f"{self.premsg}[{self.count}/{self.n_workers}][get_info_fragments] - {repr(e)}"
            )
            raise AsyncHLSDLErrorFatal(repr(e))

    def get_m3u8_obj(self):

        if not self.m3u8_doc:
            self.m3u8_doc = try_get(
                self.init_client.get(self.video_url),
                lambda x: x.content.decode("utf-8", "replace"),
            )

        return m3u8.loads(self.m3u8_doc, self.video_url)

    @dec_retry_error
    def get_init_section(self, uri, file, key):
        try:
            cipher = None
            if key is not None and key.method == "AES-128":
                iv = binascii.unhexlify(key.iv[2:])
                cipher = AES.new(self.key_cache[key.absolute_uri], AES.MODE_CBC, iv)
            res = self.init_client.get(uri)
            res.raise_for_status()
            _frag = res.content if not cipher else cipher.decrypt(res.content)
            with open(file, "wb") as f:
                f.write(_frag)

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]:[get_init_section] {repr(e)} \n{'!!'.join(lines)}"
            )
            raise

    def multi_extract_info(self, url, proxy=None, msg=None):

        premsg = ""
        if msg:
            premsg = msg

        try:
            if proxy:
                with ProxyYTDL(opts=self.ytdl.params.copy(), proxy=proxy) as proxy_ytdl:
                    _info_video = proxy_ytdl.sanitize_info(proxy_ytdl.extract_info(url))

            else:
                _info_video = self.ytdl.sanitize_info(
                    self.ytdl.extract_info(url, download=False)
                )

            if not _info_video:
                raise ExtractorError("no info video")

            return _info_video

        except Exception as e:
            logger.error(f"{premsg} fails when extracting info {str(e)}")
            raise

    @retry
    def get_reset_info(self, _reset_url, first=False):
            
        _proxy = self._proxy["http://"] if self._proxy else None
        _print_proxy = _proxy.split(":")[-1] if _proxy else None

        logger.debug(
            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]: get video dict: {_reset_url}"
        )

        _info = None

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
                    msg=f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]:",
                )
                
                self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), _info)

            else:
                _info = self.ytdl.cache.load("nakedswordmovie", str(self.fromplns))
                if not _info:

                    _info = self.multi_extract_info(
                        _reset_url,
                        proxy=_proxy,
                        msg=f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]:",
                    )

                    self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), _info)


            if _info:
                info_reset = get_format_id(
                    traverse_obj(
                        _info,
                        (
                            "entries",
                            int(self.video_downloader.info_dict["playlist_index"]) - 1,
                        ),
                    ),
                    self.info_dict["format_id"],
                )

        else:

            _info = self.multi_extract_info(
                _reset_url,
                proxy=_proxy,
                msg=f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]:",
            )

            if _info:
                info_reset = get_format_id(_info, self.info_dict["format_id"])

        if not info_reset:
            raise AsyncHLSDLErrorFatal(
                f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]:  fails no descriptor"
            )            

        logger.info(
            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]: format extracted info video ok"
        )
        logger.debug(
            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]: format extracted info video ok\n%no%{_for_print(info_reset)}"
        )

        try:
            self.prep_reset(info_reset)
            return True
        except Exception as e:
            logger.exception(
                f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]:FIRST[{first}] proxy [{_print_proxy}]: Exception occurred when reset: {repr(e)} {_for_print(info_reset)}"
            )
            raise AsyncHLSDLErrorFatal("RESET fails: preparation frags failed")

    
    def resetdl(self, cause=None):

        try:

            if cause and str(cause) == "403":

                if not self.fromplns:
                    self.video_downloader.on_hold_event.set()
                    self.video_downloader.info_dl['onhold'].put_nowait(self.video_downloader)
                
                logger.info(
                    f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}] start wait in reset cause 403"
                )
                if not wait_time(
                    CONF_HLS_RESET_403_TIME, event=self.video_downloader.stop_event
                ):
                    return
                logger.info(
                    f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}] fin wait in reset cause 403"
                )

            
            if self.enproxy:
                el1, el2 = self._qproxies.get()
                _proxy = f"http://127.0.0.1:{CONF_PROXIES_BASE_PORT + el1*100 + el2}"
                self._proxy = {"http://": _proxy, "https://": _proxy}

            _wurl = self.info_dict.get("webpage_url")

            if self.fromplns and cause in ("403", "hard"):

                _webpage_url = smuggle_url(
                    re.sub(r"(/scene/\d+)", "", _wurl),
                    {
                        "indexdl": self.video_downloader.index,
                        "args": {
                            "nakedswordmovie": {
                                "listreset": [
                                    int(index)
                                    for index in list(
                                        self.video_downloader.info_dl["fromplns"][
                                            self.fromplns
                                        ]["in_reset"]
                                    )
                                ]
                            }
                        },
                    },
                )

                with (_sem := self.video_downloader.info_dl["fromplns"]["ALL"]["sem"]):

                    logger.debug(
                        f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}] in sem"
                    )
                    
                    if _sem._initial_value == 1:
                        if cause == "403": NakedSwordBaseIE._STATUS = "403"
                                               
                        #_st_ip = wait_for_change_ip(logger)
                        #logger.info(
                        #    f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}] change ip {_st_ip}"
                        #)
                        NakedSwordBaseIE._API.logout()
                        NakedSwordBaseIE._API.get_auth()

                    with (_sem2 := self.video_downloader.info_dl["fromplns"][self.fromplns]["sem"]):

                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}] in sem2"
                        )
                        
                        if  _sem2._initial_value == 1: _first=True
                        else: _first=False

                        if self.get_reset_info(
                            _webpage_url,                           
                            first=_first,
                        ):
                            
                            if _sem._initial_value == 1:
                                _sem._initial_value = 100
                                _sem.release(50)
                            if _sem2._initial_value == 1:
                                _sem2._initial_value = 100
                                _sem2.release(50)
                                
            else:
                _webpage_url = (
                    smuggle_url(_wurl, {"indexdl": self.video_downloader.index})
                    if self.special_extr
                    else _wurl
                )
                self.get_reset_info(_webpage_url)

        except Exception as e:
            logger.exception(
                f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]: outer Exception occurred when reset: {repr(e)}"
            )
            raise
        else:
            if self.fromplns and cause in ("403", "hard"):
                
                try:
                    self.video_downloader.info_dl["fromplns"][self.fromplns]["in_reset"].remove(self.video_downloader.info_dict["playlist_index"])
                except Exception as e:
                    self.warning(
                        f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]: error when removing [{self.video_downloader.info_dict['playlist_index']}] from {self.video_downloader.info_dl['fromplns'][self.fromplns]['downloading']}"
                    )

                if not self.video_downloader.info_dl["fromplns"][self.fromplns]["in_reset"]:

                    logger.info(
                        f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]: end of resets fromplns [{self.fromplns}]"
                    )

                    self.video_downloader.info_dl["fromplns"][self.fromplns]["reset"].set()
                    self.video_downloader.info_dl["fromplns"][self.fromplns]["sem"] = threading.BoundedSemaphore(value=1)
                    
                    self.video_downloader.info_dl["fromplns"]["ALL"]["in_reset"].remove(self.fromplns)
                    if not self.video_downloader.info_dl["fromplns"]["ALL"]["in_reset"]:
                        self.video_downloader.info_dl["fromplns"]["ALL"]["reset"].set()
                        self.video_downloader.info_dl["fromplns"]["ALL"]["sem"] = threading.BoundedSemaphore(value=1)
                    else:
                        self.video_downloader.info_dl["fromplns"]["ALL"]["reset"].wait()
                else:
                    logger.info(
                        f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]: waits for rest scenes to start DL [{self.fromplns}]"
                    )
                    self.video_downloader.info_dl["fromplns"][self.fromplns]["reset"].wait()

                NakedSwordBaseIE._STATUS = "NORMAL"
            self.n_reset += 1
            logger.info(
                        f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]: exit reset"
                    )
            if not self.fromplns and cause == "403":
                try:
                    dl = self.video_downloader.info_dl['onhold'].get(block=False, timeout=0)
                    dl.on_hold_event.clear()
                    logger.info(
                        f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}] clear onhold event fort [{dl.info_dict['id']}][{dl.info_dict['title']}]"
                    )
                except Exception as e:
                    pass


    def prep_reset(self, info_reset):

        self.headers = info_reset.get("http_headers")
        self.video_url = info_reset.get("url")
        self.init_client.close()
        self.init_client = httpx.Client(
            proxies=self._proxy,
            follow_redirects=True,
            headers=self.headers,
            limits=self.limits,
            timeout=self.timeout,
            verify=False,
        )

        self.frags_to_dl = []

        part = 0
        uri_ant = ""
        byte_range = {}

        self.info_dict["fragments"] = self.get_info_fragments()

        for i, fragment in enumerate(self.info_dict["fragments"]):

            try:
                if fragment.byterange:
                    if fragment.uri == uri_ant:
                        part += 1
                    else:
                        part = 1

                    uri_ant = fragment.uri

                    _file_path = Path(
                        f"{str(self.fragments_base_path)}.Frag{i}.part.{part}"
                    )
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
                    _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i}")
                    byte_range = {}

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
                    if fragment.key is not None and fragment.key.method == "AES-128":
                        if fragment.key.absolute_uri not in self.key_cache:
                            self.key_cache[fragment.key.absolute_uri] = httpx.get(
                                fragment.key.absolute_uri, headers=self.headers
                            ).content

            except Exception as e:
                logger.debug(
                    f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:prep_reset error with i = [{i}] \n\ninfo_dict['fragments'] {len(self.info_dict['fragments'])}\n\n{[str(f) for f in self.info_dict['fragments']]}\n\ninfo_frag {len(self.info_frag)}"
                )  
                raise

        if not self.frags_to_dl:
            self.status = "init_manipulating"
        else:
            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:prep_reset:OK {self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}"
            )

    async def check_speed(self):
        def getter(x):
            return x * CONF_HLS_SPEED_PER_WORKER

        _speed = []

        _num_chunks = self._CONF_HLS_MIN_N_TO_CHECK_SPEED

        pending = None
        _max_bd_detected = 0
        prog = ProgressTimer()

        try:
            while True:

                done, pending = await asyncio.wait(
                    [
                        _reset := asyncio.create_task(
                            self.video_downloader.reset_event.wait()
                        ),
                        _stop := asyncio.create_task(
                            self.video_downloader.stop_event.wait()
                        ),
                        _qspeed := asyncio.create_task(self._qspeed.get()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for _el in pending:
                    _el.cancel()
                if any([_ in done for _ in [_reset, _stop]]):
                    logger.info(
                        f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] event detected"
                    )
                    self._test.append((f"event detected"))
                    return
                _input_speed = try_get(list(done), lambda x: x[0].result())
                if _input_speed == "KILL":
                    return

                _speed.append(_input_speed)

                if len(_speed) > _num_chunks:

                    if any(
                        [
                            all(
                                [
                                    el == 0
                                    for el in _speed[
                                        -(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2) :
                                    ]
                                ]
                            ),
                            not _max_bd_detected
                            and all(
                                [
                                    el < getter(self.count)
                                    for el in _speed[
                                        -(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2) :
                                    ]
                                ]
                            ),
                        ]
                    ):

                        logger.info(
                            f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] speed reset: n_el_speed[{len(_speed)}]"
                        )

                        self.video_downloader.reset_event.set("speedreset")

                        self._test.append(("speed reset"))

                        break

                    elif (
                        self._CONF_HLS_MAX_SPEED_PER_DL
                        and all(
                            [
                                el >= self._CONF_HLS_MAX_SPEED_PER_DL
                                for el in _speed[
                                    -(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2) :
                                ]
                            ]
                        )
                        and (
                            not _max_bd_detected
                            or (_max_bd_detected and prog.elapsed_seconds() > nsecs)
                        )
                    ):

                        _old_throttle = self.throttle

                        if not _max_bd_detected:
                            self.throttle = float(f"{self.throttle + 0.05:.2f}")
                            _max_bd_detected += 1
                            prog.reset()
                            nsecs = 20

                        else:

                            _max_bd_detected += 1
                            self.throttle = float(f"{self.throttle + 0.01:.2f}")
                            prog.reset()
                            nsecs = 20

                        _str_speed = ", ".join(
                            [
                                f"{el}"
                                for el in _speed[
                                    -(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2) :
                                ]
                            ]
                        )
                        logger.info(
                            f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] MAX BD -> decrease speed: throttle[{_old_throttle} -> {self.throttle}] n_el_speed[{len(_speed)}]"
                        )
                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] MAX BD -> decrease speed: throttle[{_old_throttle} -> {self.throttle}] n_el_speed[{len(_speed)}]\n%no%{_str_speed}"
                        )

                        self._test.append(
                            (
                                f"decrease speed: throttle {_old_throttle} -> {self.throttle}]"
                            )
                        )

                    elif (
                        self._CONF_HLS_MAX_SPEED_PER_DL
                        and all(
                            [
                                0.8 * self._CONF_HLS_MAX_SPEED_PER_DL
                                <= el
                                < self._CONF_HLS_MAX_SPEED_PER_DL
                                for el in _speed[
                                    -(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2) :
                                ]
                            ]
                        )
                        and (_max_bd_detected and prog.elapsed_seconds() > nsecs)
                    ):

                        _str_speed = ", ".join(
                            [
                                f"{el}"
                                for el in _speed[
                                    -(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2) :
                                ]
                            ]
                        )
                        logger.info(
                            f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] estable with throttle[{self.throttle}]: n_el_speed[{len(_speed)}]"
                        )
                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] estable with throttle[{self.throttle}]: n_el_speed[{len(_speed)}]\n%no%{_str_speed}"
                        )

                        _max_bd_detected = 0

                        self._test.append((f"estable throttle {self.throttle}"))

                    elif (
                        self._CONF_HLS_MAX_SPEED_PER_DL
                        and all(
                            [
                                el < 0.8 * self._CONF_HLS_MAX_SPEED_PER_DL
                                for el in _speed[
                                    -(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2) :
                                ]
                            ]
                        )
                        and (_max_bd_detected and prog.elapsed_seconds() > nsecs)
                    ):

                        _old_throttle = self.throttle

                        if _max_bd_detected == 1:
                            self.throttle = float(f"{self.throttle - 0.01:.2f}")
                            prog.reset()
                            nsecs = 20
                        else:
                            self.throttle = float(f"{self.throttle - 0.005:.2f}")
                            prog.reset()
                            nsecs = 20

                        if self.throttle < 0:
                            self.throttle = 0

                        _str_speed = ", ".join(
                            [
                                f"{el}"
                                for el in _speed[
                                    -(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2) :
                                ]
                            ]
                        )
                        logger.info(
                            f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] MIN 0.8 -> increase speed: throttle[{_old_throttle} -> {self.throttle}] n_el_speed[{len(_speed)}]"
                        )
                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] MIN 0.8 -> increase speed: throttle[{_old_throttle} -> {self.throttle}] n_el_speed[{len(_speed)}]\n%no%{_str_speed}"
                        )

                        self._test.append(
                            (
                                f"increase speed: throttle {_old_throttle} -> {self.throttle}]"
                            )
                        )

                if pending:
                    await asyncio.wait(pending)
                await asyncio.sleep(0)

        except Exception as e:
            logger.warning(
                f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] {repr(e)}"
            )
        finally:
            if pending:
                await asyncio.wait(pending)
            self._speed.extend(_speed)
            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}][check_speed] bye"
            )

    async def upt_status(self):

        while not any([self.kill.is_set(), self.video_downloader.end_tasks.is_set()]):

            if self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):

                if self.first_data.is_set() and not any(
                    [
                        self.video_downloader.pause_event
                        and self.video_downloader.pause_event.is_set(),
                        self.video_downloader.reset_event
                        and self.video_downloader.reset_event.is_set(),
                        self.video_downloader.stop_event
                        and self.video_downloader.stop_event.is_set(),
                    ]
                ):

                    _down_size = self.down_size
                    _speed_meter = self.speedometer(_down_size)
                    self._test.append((time.monotonic(), _down_size, _speed_meter))
                    _progress_str = (
                        f"{(_down_size/self.filesize)*100:5.2f}%"
                        if self.filesize
                        else "-----"
                    )
                    self.upt.update(
                        {"speed_meter": _speed_meter, "down_size": _down_size}
                    )

                    if _speed_meter and self.filesize:

                        _est_time = (self.filesize - _down_size) / _speed_meter
                        _est_time_smooth = self.smooth_eta(_est_time)
                        self.upt.update(
                            {"est_time": _est_time, "est_time_smooth": _est_time_smooth}
                        )
                        self._speed.append((datetime.now(), copy.deepcopy(self.upt)))
                        # if self._qspeed:
                        #    self._qspeed.put_nowait(_speed_meter)

            await asyncio.sleep(0)

    async def fetch(self, nco):

        logger.debug(
            f"{self.premsg}[{self.count}/{self.n_workers}]:[worker-{nco}]: init worker"
        )

        client = httpx.AsyncClient(
            proxies=self._proxy,
            limits=self.limits,
            follow_redirects=True,
            timeout=self.timeout,
            verify=self.verifycert,
            headers=self.headers,
        )

        try:

            while True:

                try:
                    done, pending = await asyncio.wait(
                        [
                            _reset := asyncio.create_task(
                                self.video_downloader.reset_event.wait()
                            ),
                            _stop := asyncio.create_task(
                                self.video_downloader.stop_event.wait()
                            ),
                            _frags := asyncio.create_task(self.frags_queue.get()),
                        ],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for _el in pending:
                        _el.cancel()
                    if any([_ in done for _ in [_reset, _stop]]):
                        if pending: await asyncio.wait(pending)
                        return

                    q = try_get(list(done), lambda x: x[0].result())

                    if q == "KILL":
                        logger.debug(
                            f"{self.premsg}[{self.count}/{self.n_workers}]:[worker-{nco}]: KILL"
                        )
                        return

                    logger.debug(
                        f"{self.premsg}[{self.count}/{self.n_workers}]:[worker-{nco}]: frag[{q}]\n{self.info_frag[q - 1]}"
                    )

                    if self.video_downloader.pause_event.is_set():
                        self._speed.append((datetime.now(), "pause"))
                        done2, pending2 = await asyncio.wait(
                            [
                                asyncio.create_task(
                                    self.video_downloader.resume_event.wait()
                                ),
                                asyncio.create_task(
                                    self.video_downloader.reset_event.wait()
                                ),
                                asyncio.create_task(
                                    self.video_downloader.stop_event.wait()
                                ),
                            ],
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        for _el in pending2:
                            _el.cancel()
                        self.video_downloader.pause_event.clear()
                        self.video_downloader.resume_event.clear()
                        self._speed.append((datetime.now(), "resume"))

                        if any(
                            [
                                self.video_downloader.stop_event.is_set(),
                                self.video_downloader.reset_event.is_set(),
                            ]
                        ):
                            return

                    url = self.info_frag[q - 1]["url"]
                    filename = Path(self.info_frag[q - 1]["file"])
                    filename_exists = await os.path.exists(filename)
                    key = self.info_frag[q - 1]["key"]
                    cipher = None
                    if key is not None and key.method == "AES-128":
                        iv = binascii.unhexlify(key.iv[2:])
                        cipher = AES.new(
                            self.key_cache[key.absolute_uri], AES.MODE_CBC, iv
                        )
                    byte_range = self.info_frag[q - 1].get("byterange")
                    headers = {}
                    if byte_range:
                        headers[
                            "range"
                        ] = f"bytes={byte_range['start']}-{byte_range['end'] - 1}"

                    await asyncio.sleep(0)

                    while self.info_frag[q - 1]["n_retries"] < self._MAX_RETRIES:

                        if any(
                            [
                                self.video_downloader.stop_event.is_set(),
                                self.video_downloader.reset_event.is_set(),
                            ]
                        ):
                            return

                        try:

                            self._premsg = f"{self.premsg}[{self.count}/{self.n_workers}]:[worker-{nco}]:[frag[{q}] "

                            async with aiofiles.open(filename, mode="ab") as f:

                                async with self._limit:
                                    logger.debug(f"{self._premsg}: limiter speed")

                                async with client.stream(
                                    "GET", url, headers=headers, timeout=15
                                ) as res:

                                    logger.debug(
                                        f"{self._premsg}: {res.request} {res.status_code} {res.reason_phrase} {res.headers}"
                                    )

                                    if res.status_code == 403:
                                        if self.fromplns:
                                            _wait_tasks = (
                                                await self.video_downloader.reset_plns(
                                                    "403"
                                                )
                                            )

                                        else:
                                            _wait_tasks = (
                                                await self.video_downloader.reset("403")
                                            )

                                        logger.debug(
                                            f"{self._premsg}: wait_tasks\n{_wait_tasks}\n{print_tasks(_wait_tasks)}"
                                        )
                                        if _wait_tasks:
                                            done, pending = await asyncio.wait(
                                                _wait_tasks
                                            )
                                            logger.debug(
                                                f"{self._premsg}: wait_tasks result\nDONE\n{done}\nPENDING\n{pending}"
                                            )

                                        return

                                    elif res.status_code >= 400:
                                        raise AsyncHLSDLError(
                                            f"Frag:{str(q)} resp code:{str(res)}"
                                        )
                                    else:
                                        _hsize = int_or_none(
                                            res.headers.get("content-length")
                                        )
                                        if _hsize:
                                            self.info_frag[q - 1]["headersize"] = _hsize

                                            if not self.filesize:
                                                async with self._LOCK:
                                                    self.filesize = _hsize * len(
                                                        self.info_dict["fragments"]
                                                    )
                                                async with self.video_downloader.alock:
                                                    self.video_downloader.info_dl[
                                                        "filesize"
                                                    ] += self.filesize
                                        else:
                                            logger.warning(
                                                f"{self._premsg}: Frag:{str(q)} _hsize is None"
                                            )
                                            


                                        if self.info_frag[q - 1]["downloaded"]:

                                            if filename_exists:
                                                _size = self.info_frag[q - 1][
                                                    "size"
                                                ] = (await os.stat(filename)).st_size
                                                if _size and (
                                                    _hsize - 100
                                                    <= _size
                                                    <= _hsize + 100
                                                ):

                                                    logger.debug(
                                                        f"{self._premsg}: Already DL with hsize[{_hsize}] and size [{_size}] check[{_hsize - 100 <=_size <= _hsize + 100}]"
                                                    )
                                                    break
                                                else:
                                                    await f.truncate(0)
                                                    self.info_frag[q - 1][
                                                        "downloaded"
                                                    ] = False
                                                    async with self._LOCK:
                                                        self.n_dl_fragments -= 1
                                                        self.down_size -= _size
                                                        # self.down_temp -= _size
                                                    async with self.video_downloader.alock:
                                                        self.video_downloader.info_dl[
                                                            "down_size"
                                                        ] -= _size
                                            else:
                                                logger.warning(
                                                    f"{self.premsg}[{self.count}/{self.n_workers}]: frag with mark downloaded but file [{filename}] doesnt exists"
                                                )
                                                self.info_frag[q - 1][
                                                    "downloaded"
                                                ] = False
                                                async with self._LOCK:
                                                    self.n_dl_fragments -= 1

                                        if self.info_frag[q - 1]["headersize"] and self.info_frag[q - 1]["headersize"] < self._CHUNK_SIZE:
                                        
                                            _chunk_size = self.info_frag[q - 1][
                                                "headersize"
                                            ]
                                        
                                        else:
                                            _chunk_size = self._CHUNK_SIZE

                                        num_bytes_downloaded = res.num_bytes_downloaded

                                        self.info_frag[q - 1]["time2dlchunks"] = []
                                        self.info_frag[q - 1]["sizechunks"] = []
                                        self.info_frag[q - 1]["nchunks_dl"] = 0

                                        _started = time.monotonic()

                                        async for chunk in res.aiter_bytes(
                                            chunk_size=_chunk_size
                                        ):

                                            if (
                                                self.video_downloader.pause_event.is_set()
                                            ):
                                                self._speed.append(
                                                    (datetime.now(), "pause")
                                                )

                                                done, pending = await asyncio.wait(
                                                    [
                                                        asyncio.create_task(
                                                            self.video_downloader.resume_event.wait()
                                                        ),
                                                        asyncio.create_task(
                                                            self.video_downloader.reset_event.wait()
                                                        ),
                                                        asyncio.create_task(
                                                            self.video_downloader.stop_event.wait()
                                                        ),
                                                    ],
                                                    return_when=asyncio.FIRST_COMPLETED,
                                                )
                                                for _el in pending:
                                                    _el.cancel()
                                                if pending: await asyncio.wait(pending)
                                                self.video_downloader.pause_event.clear()
                                                self.video_downloader.resume_event.clear()
                                                self._speed.append(
                                                    (datetime.now(), "resume")
                                                )


                                            _timechunk = time.monotonic() - _started
                                            self.info_frag[q - 1][
                                                "time2dlchunks"
                                            ].append(_timechunk)
                                            if cipher:
                                                data = cipher.decrypt(chunk)
                                            else:
                                                data = chunk
                                            await f.write(data)

                                            async with self._LOCK:
                                                self.down_size += (
                                                    _iter_bytes := (
                                                        res.num_bytes_downloaded
                                                        - num_bytes_downloaded
                                                    )
                                                )
                                                if (
                                                    _dif := self.down_size
                                                    - self.filesize
                                                ) > 0:
                                                    self.filesize += _dif
                                                self.first_data.set()

                                            async with self.video_downloader.alock:
                                                if _dif > 0:
                                                    self.video_downloader.info_dl[
                                                        "filesize"
                                                    ] += _dif
                                                self.video_downloader.info_dl[
                                                    "down_size"
                                                ] += _iter_bytes

                                            num_bytes_downloaded = (
                                                res.num_bytes_downloaded
                                            )
                                            self.info_frag[q - 1]["nchunks_dl"] += 1
                                            self.info_frag[q - 1]["sizechunks"].append(
                                                _iter_bytes
                                            )

                                            if self.throttle:
                                                await async_wait_time(self.throttle)
                                            else:
                                                await asyncio.sleep(0)
                                            _started = time.monotonic()

                            _size = (await os.stat(filename)).st_size
                            _hsize = self.info_frag[q - 1]["headersize"]
                            if _hsize - 100 <= _size <= _hsize + 100:
                                self.info_frag[q - 1]["downloaded"] = True
                                self.info_frag[q - 1]["size"] = _size
                                async with self._LOCK:
                                    self.n_dl_fragments += 1

                                logger.debug(
                                    f"{self._premsg}: OK DL: total[{self.n_dl_fragments}]\n{self.info_frag[q - 1]}"
                                )
                                break
                            else:
                                logger.warning(
                                    f"{self._premsg}: end of streaming. fragment not completed\n{self.info_frag[q - 1]}"
                                )
                                raise AsyncHLSDLError(
                                    f"fragment not completed frag[{q}]"
                                )

                        except AsyncHLSDLErrorFatal as e:

                            logger.debug(f"{self._premsg}: errorfatal {repr(e)}")
                            self.info_frag[q - 1]["error"].append(repr(e))
                            self.info_frag[q - 1]["downloaded"] = False
                            if await os.path.exists(filename):
                                _size = (await os.stat(filename)).st_size
                                await os.remove(filename)

                                async with self._LOCK:
                                    self.down_size -= _size
                                    # self.down_temp -= _size
                                async with self.video_downloader.alock:
                                    self.video_downloader.info_dl["down_size"] -= _size

                            return
                        except (
                            asyncio.exceptions.CancelledError,
                            asyncio.CancelledError,
                            CancelledError,
                        ) as e:
                            # logger.info(f"{self.premsg}[{self.count}/{self.n_workers}]:[worker-{nco}]: cancelled exception")
                            self.info_frag[q - 1]["error"].append(repr(e))
                            self.info_frag[q - 1]["downloaded"] = False
                            lines = traceback.format_exception(*sys.exc_info())
                            logger.debug(
                                f"{self._premsg}: CancelledError: \n{'!!'.join(lines)}"
                            )
                            if await os.path.exists(filename):
                                _size = (await os.stat(filename)).st_size
                                await os.remove(filename)

                                async with self._LOCK:
                                    self.down_size -= _size
                                async with self.video_downloader.alock:
                                    self.video_downloader.info_dl["down_size"] -= _size
                            return
                        except RuntimeError as e:
                            self.info_frag[q - 1]["error"].append(repr(e))
                            self.info_frag[q - 1]["downloaded"] = False

                            if await os.path.exists(filename):
                                _size = (await os.stat(filename)).st_size
                                await os.remove(filename)

                                async with self._LOCK:
                                    self.down_size -= _size
                                async with self.video_downloader.alock:
                                    self.video_downloader.info_dl["down_size"] -= _size

                            logger.exception(
                                f"{self._premsg}: runtime error: {repr(e)}"
                            )
                            return

                        except Exception as e:
                            self.info_frag[q - 1]["error"].append(repr(e))
                            self.info_frag[q - 1]["downloaded"] = False
                            lines = traceback.format_exception(*sys.exc_info())
                            if not "httpx" in str(
                                e.__class__
                            ) and not "AsyncHLSDLError" in str(e.__class__):
                                logger.exception(
                                    f"{self._premsg}: error {str(e.__class__)}"
                                )

                            logger.debug(
                                f"{self._premsg}: error {repr(e)} \n{'!!'.join(lines)}"
                            )
                            self.info_frag[q - 1]["n_retries"] += 1
                            if await os.path.exists(filename):
                                _size = (await os.stat(filename)).st_size
                                await os.remove(filename)

                                async with self._LOCK:
                                    self.down_size -= _size
                                async with self.video_downloader.alock:
                                    self.video_downloader.info_dl["down_size"] -= _size

                            if any(
                                [
                                    self.video_downloader.stop_event.is_set(),
                                    self.video_downloader.reset_event.is_set(),
                                ]
                            ):
                                return

                            if self.info_frag[q - 1]["n_retries"] < self._MAX_RETRIES:

                                await async_wait_time(
                                    random.choice([i for i in range(1, 5)])
                                )
                                await asyncio.sleep(0)

                            else:
                                self.info_frag[q - 1]["error"].append("MaxLimitRetries")
                                logger.warning(f"{self._premsg}: MaxLimitRetries:skip")
                                self.info_frag[q - 1]["skipped"] = True
                                break
                        finally:
                            await asyncio.sleep(0)

                except Exception as e:
                    logger.exception(f"{self._premsg}: {str(e)}")

        finally:

            async with self._LOCK:
                self.count -= 1
            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]:[worker{nco}]: bye worker"
            )
            await client.aclose()

    async def fetch_async(self):

        self._LOCK = asyncio.Lock()
        self.first_data = asyncio.Event()
        self.kill = asyncio.Event()
        self.frags_queue = asyncio.Queue()

        self.areset = sync_to_async(self.resetdl, self.ex_hlsdl)

        for frag in self.frags_to_dl:
            self.frags_queue.put_nowait(frag)

        for _ in range(self.n_workers):
            self.frags_queue.put_nowait("KILL")

        self._speed = []

        n_frags_dl = 0

        

        
        if self.fromplns:
            _event = traverse_obj(
                self.video_downloader.info_dl["fromplns"], ("ALL", "reset")
            )
            await async_ex_in_executor(self.ex_hlsdl, _event.wait)
            self.video_downloader.info_dl["fromplns"][self.fromplns]["downloading"].add(
                self.video_downloader.info_dict["playlist_index"]
            )
            self.video_downloader.info_dl["fromplns"]["ALL"]["downloading"].add(
                self.fromplns
            )

        _tstart = time.monotonic()

        try:

            while True:

                if self.video_downloader.on_hold_event.is_set():

                    logger.info(
                        f"{self.premsg}[{self.count}/{self.n_workers}][fetch_async]: waiting on hold"
                    )

                    async def prop(ev):
                        return not ev.is_set()

                    try:
                    
                        await async_wait_until(300, cor=prop,args=(self.video_downloader.on_hold_event,), interv=5)
                        logger.info(
                            f"{self.premsg}[{self.count}/{self.n_workers}][fetch_async]: end waiting on hold for action of colleague"
                        )

                    except Exception as e:
                        logger.info(
                            f"{self.premsg}[{self.count}/{self.n_workers}][fetch_async]: end waiting on hold for timeout"
                        )

                        self.video_downloader.on_hold_event.clear()

                logger.debug(f"{self.premsg}[{self.count}/{self.n_workers}] TASKS INIT")

                try:
                    self.count = self.n_workers
                    self.video_downloader.reset_event.clear()
                    self.video_downloader.pause_event.clear()
                    self.video_downloader.resume_event.clear()
                    self.video_downloader.end_tasks.clear()
                    self.kill.clear()
                    self.first_data.clear()
                    self._qspeed = asyncio.Queue()
                    self.speedometer = SpeedometerMA(initial_bytes=self.down_size)
                    self.progress_timer = ProgressTimer()
                    self.smooth_eta = SmoothETA()
                    self._test.append(("starting tasks to dl"))
                    self.status = "downloading"

                    upt_task = [asyncio.create_task(self.upt_status())]
                    # check_task = [asyncio.create_task(self.check_speed())]
                    check_task = []
                    self.tasks = [
                        asyncio.create_task(
                            self.fetch(i),
                            name=f"{self.premsg}[{self.count}/{self.n_workers}][{i}]",
                        )
                        for i in range(self.n_workers)
                    ]

                    done, pending = await asyncio.wait(
                        self.tasks, return_when=asyncio.ALL_COMPLETED
                    )

                    # self.video_downloader.end_tasks.set()

                    logger.debug(
                        f"{self.premsg}[{self.count}/{self.n_workers}][fetch_async]: done[{len(list(done))}] pending[{len(list(pending))}]"
                    )

                    _nfragsdl = len(self.fragsdl())
                    inc_frags_dl = _nfragsdl - n_frags_dl
                    n_frags_dl = _nfragsdl

                    if n_frags_dl == len(self.info_dict["fragments"]):

                        await self.clean_from_reset()
                        self.video_downloader.end_tasks.set()
                        self._qspeed.put_nowait("KILL")
                        self.kill.set()
                        await asyncio.sleep(0)
                        await asyncio.wait(check_task + upt_task)
                        break

                    else:
                        if self.video_downloader.stop_event.is_set():

                            await self.clean_from_reset()
                            self.video_downloader.end_tasks.set()
                            self.status = "stop"
                            await asyncio.wait(check_task + upt_task)
                            return

                        else:

                            self.video_downloader.end_tasks.set()

                            if _cause := self.video_downloader.reset_event.is_set():

                                dump_init_task = [
                                    asyncio.create_task(
                                        async_ex_in_executor(
                                            self.ex_hlsdl, self.dump_init_file
                                        )
                                    )
                                ]

                                await asyncio.wait(
                                    dump_init_task + check_task + upt_task
                                )

                                if self.n_reset < self._MAX_RESETS:

                                    if self.fromplns and _cause in ("403", "hard"):
                                        await self.video_downloader.back_from_reset_plns(
                                            logger,
                                            f"{self.premsg}[{self.count}/{self.n_workers}]",
                                        )
                                        if self.video_downloader.stop_event.is_set():
                                            await self.clean_from_reset()
                                            return

                                        _cause = (
                                            self.video_downloader.reset_event.is_set()
                                        )
                                        self._speed.append((datetime.now(), "speed"))
                                        logger.info(
                                            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:CAUSE[{_cause}]"
                                        )

                                    elif _cause == "manual":
                                        logger.info(
                                            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:CAUSE[{_cause}]"
                                        )
                                        continue

                                    try:
                                        await self.areset(_cause)
                                        if self.video_downloader.stop_event.is_set():
                                            return
                                        self.frags_queue = asyncio.Queue()
                                        for frag in self.frags_to_dl:
                                            self.frags_queue.put_nowait(frag)
                                        if (
                                            (_t := time.monotonic()) - _tstart
                                        ) < self._MIN_TIME_RESETS:
                                            self.n_workers -= self.n_workers // 4
                                        _tstart = _t
                                        for _ in range(self.n_workers):
                                            self.frags_queue.put_nowait("KILL")
                                        logger.debug(
                                            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:OK:Pending frags {len(self.fragsnotdl())}"
                                        )
                                        await asyncio.sleep(0)
                                        continue

                                    except Exception as e:
                                        lines = traceback.format_exception(
                                            *sys.exc_info()
                                        )
                                        logger.debug(
                                            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:ERROR reset couldnt progress:[{repr(e)}]\n{'!!'.join(lines)}"
                                        )
                                        self.status = "error"
                                        await self.clean_when_error()
                                        raise AsyncHLSDLErrorFatal(
                                            f"{self.premsg}[{self.count}/{self.n_workers}]: ERROR reset couldnt progress"
                                        )

                                else:

                                    logger.warning(
                                        f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:ERROR:Max_number_of_resets"
                                    )
                                    self.status = "error"
                                    await self.clean_when_error()
                                    await asyncio.sleep(0)
                                    raise AsyncHLSDLErrorFatal(
                                        f"{self.premsg}[{self.count}/{self.n_workers}]: ERROR max resets"
                                    )

                            else:

                                self._qspeed.put_nowait("KILL")
                                if check_task:
                                    await asyncio.wait(check_task)
                                if inc_frags_dl > 0:

                                    logger.debug(
                                        f"{self.premsg}[{self.count}/{self.n_workers}]: [{n_frags_dl} -> {inc_frags_dl}] new cycle with no fatal error"
                                    )
                                    try:
                                        await self.areset()
                                        if self.video_downloader.stop_event.is_set():
                                            return
                                        self.frags_queue = asyncio.Queue()
                                        for frag in self.frags_to_dl:
                                            self.frags_queue.put_nowait(frag)
                                        for _ in range(self.n_workers):
                                            self.frags_queue.put_nowait("KILL")
                                        logger.debug(
                                            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET new cycle[{self.n_reset}]:OK:Pending frags {len(self.fragsnotdl())}"
                                        )
                                        self.n_reset -= 1
                                        continue

                                    except Exception as e:
                                        lines = traceback.format_exception(
                                            *sys.exc_info()
                                        )
                                        logger.debug(
                                            f"{self.premsg}[{self.count}/{self.n_workers}]:RESET[{self.n_reset}]:ERROR reset couldnt progress:[{repr(e)}]\n{'!!'.join(lines)}"
                                        )
                                        self.status = "error"
                                        await self.clean_when_error()
                                        await asyncio.sleep(0)
                                        raise AsyncHLSDLErrorFatal(
                                            f"{self.premsg}[{self.count}/{self.n_workers}]: ERROR reset couldnt progress"
                                        )

                                else:
                                    logger.debug(
                                        f"{self.premsg}[{self.count}/{self.n_workers}]: [{n_frags_dl} <-> {inc_frags_dl}] no improvement, lets raise an error"
                                    )
                                    self.status = "error"
                                    raise AsyncHLSDLErrorFatal(
                                        f"{self.premsg}[{self.count}/{self.n_workers}]: no changes in number of dl frags in one cycle"
                                    )

                except AsyncHLSDLErrorFatal:
                    raise
                except Exception as e:
                    logger.exception(
                        f"{self.premsg}[{self.count}/{self.n_workers}][fetch_async] error {repr(e)}"
                    )
                finally:
                    await asyncio.sleep(0)

        except Exception as e:
            logger.error(
                f"{self.premsg}[{self.count}/{self.n_workers}] error {repr(e)}"
            )
            self.status = "error"
        finally:
            await async_ex_in_executor(self.ex_hlsdl, self.dump_init_file)
            if self.fromplns:
                try:
                    self.video_downloader.info_dl["fromplns"][self.fromplns][
                        "downloading"
                    ].remove(self.video_downloader.info_dict["playlist_index"])
                except Exception as e:
                    self.warning(
                        f"{self.premsg}[{self.count}/{self.n_workers}] error when removing [{self.video_downloader.info_dict['playlist_index']}] from {self.video_downloader.info_dl['fromplns'][self.fromplns]['downloading']}"
                    )

                if not self.video_downloader.info_dl["fromplns"][self.fromplns][
                    "downloading"
                ]:
                    self.video_downloader.info_dl["fromplns"]["ALL"][
                        "downloading"
                    ].remove(self.fromplns)

            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]%no%\n\n{json.dumps(self._test)}"
            )
            self.init_client.close()
            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]:Frags DL completed"
            )
            if (
                not self.video_downloader.stop_event.is_set()
                and not self.status == "error"
            ):
                self.status = "init_manipulating"

            self.ex_hlsdl.shutdown(wait=False, cancel_futures=True)

    async def clean_from_reset(self):

        if self.fromplns and self.video_downloader.reset_event.is_set():
            try:
                self.video_downloader.info_dl["fromplns"][self.fromplns][
                    "in_reset"
                ].remove(self.video_downloader.info_dict["playlist_index"])
                if not self.video_downloader.info_dl["fromplns"][self.fromplns][
                    "in_reset"
                ]:
                    logger.info(
                        f"{self.premsg}[{self.count}/{self.n_workers}] end of resets fromplns [{self.fromplns}]"
                    )
                    try:
                        self.video_downloader.info_dl["fromplns"]["ALL"][
                            "in_reset"
                        ].remove(self.fromplns)
                        if not self.video_downloader.info_dl["fromplns"]["ALL"][
                            "in_reset"
                        ]:
                            self.video_downloader.info_dl["fromplns"]["ALL"][
                                "reset"
                            ].set()

                    except Exception as e:
                        self.warning(
                            f"{self.premsg}[{self.count}/{self.n_workers}] error when removing [{self.fromplns}] from {self.video_downloader.info_dl['fromplns']['ALL']['in_reset']}"
                        )

                    self.video_downloader.info_dl["fromplns"][self.fromplns][
                        "reset"
                    ].set()

            except Exception as e:
                self.warning(
                    f"{self.premsg}[{self.count}/{self.n_workers}] error when removing [{self.video_downloader.info_dict['playlist_index']}] from {self.video_downloader.info_dl['fromplns'][self.fromplns]['in_reset']}"
                )

    def dump_init_file(self):

        init_data = {
            el["frag"]: el["headersize"] for el in self.info_frag if el["headersize"]
        }
        logger.debug(
            f"{self.premsg}[{self.count}/{self.n_workers}] init data\n{init_data}"
        )
        with open(self.init_file, "w") as f:
            json.dump(init_data, f)

    async def clean_when_error(self):

        for f in self.info_frag:
            if f["downloaded"] == False:

                if await os.path.exists(f["file"]):
                    await os.remove(f["file"])

    def sync_clean_when_error(self):

        for f in self.info_frag:
            if f["downloaded"] == False:
                if f["file"].exists():
                    f["file"].unlink()

    def ensamble_file(self):

        self.status = "manipulating"
        logger.debug(
            f"{self.premsg}[{self.count}/{self.n_workers}]: Fragments DL \n{self.fragsdl()}"
        )

        try:
            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]:{self.filename}"
            )
            with open(self.filename, mode="wb") as dest:
                _skipped = 0
                for f in self.info_frag:
                    if f.get("skipped", False):
                        _skipped += 1
                        continue
                    if not f["size"]:
                        if f["file"].exists():
                            f["size"] = f["file"].stat().st_size
                        if f["size"] and (
                            f["headersize"] - 100 <= f["size"] <= f["headersize"] + 100
                        ):

                            with open(f["file"], "rb") as source:
                                dest.write(source.read())
                        else:
                            raise AsyncHLSDLError(
                                f"{self.premsg}[{self.count}/{self.n_workers}]: error when ensambling: {f}"
                            )
                    else:
                        with open(f["file"], "rb") as source:
                            dest.write(source.read())

        except Exception as e:
            if self.filename.exists():
                self.filename.unlink()
            lines = traceback.format_exception(*sys.exc_info())
            logger.debug(
                f"{self.premsg}[{self.count}/{self.n_workers}]:Exception ocurred: \n{'!!'.join(lines)}"
            )
            self.status = "error"
            self.sync_clean_when_error()
            raise
        finally:
            if self.filename.exists():
                rmtree(str(self.download_path), ignore_errors=True)
                self.status = "done"
                logger.debug(
                    f"{self.premsg}[{self.count}/{self.n_workers}]: [ensamble_file] file ensambled"
                )
                if _skipped:
                    logger.warning(
                        f"{self.premsg}[{self.count}/{self.n_workers}]: [ensamble_file] skipped frags [{_skipped}]"
                    )
            else:
                self.status = "error"
                self.sync_clean_when_error()
                raise AsyncHLSDLError(
                    f"{self.premsg}[{self.count}/{self.n_workers}]: error when ensambling parts"
                )

    def fragsnotdl(self):
        res = []
        for frag in self.info_frag:
            if (frag["downloaded"] == False) and not frag.get("skipped", False):
                # res.append(frag['frag'])
                res.append(frag)
        return res

    def fragsdl(self):
        res = []
        for frag in self.info_frag:
            if (frag["downloaded"] == True) or frag.get("skipped", False):
                # res.append({'frag': frag['frag'], 'headersize': frag['headersize'], 'size': frag['size']})
                res.append(frag)
        return res

    def format_frags(self):
        import math

        return f"{(int(math.log(self.n_total_fragments, 10)) + 1)}d"

    def print_hookup(self):

        _filesize_str = naturalsize(self.filesize) if self.filesize else "--"
        _proxy = self._proxy["http://"].split(":")[-1] if self._proxy else None

        if self.status == "done":
            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] Completed \n"
        elif self.status == "init":
            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] Waiting to DL [{_filesize_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "error":
            _rel_size_str = (
                f"{naturalsize(self.down_size)}/{naturalsize(self.filesize)}"
                if self.filesize
                else "--"
            )
            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] ERROR [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "stop":
            _rel_size_str = (
                f"{naturalsize(self.down_size)}/{naturalsize(self.filesize)}"
                if self.filesize
                else "--"
            )
            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] STOPPED [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "downloading":

            _eta_smooth_str = "--"
            _speed_meter_str = "--"

            _temp = copy.deepcopy(self.upt)

            if not any(
                [
                    self.video_downloader.pause_event.is_set(),
                    self.video_downloader.reset_event.is_set(),
                    self.video_downloader.stop_event.is_set(),
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
                        [
                            _item.split(".")[0]
                            for _item in f"{timedelta(seconds=_est_time_smooth)}".split(
                                ":"
                            )[1:]
                        ]
                    )
                else:
                    _eta_smooth_str = "--"

            _progress_str = (
                f'{(_temp.get("down_size", self.down_size)/self.filesize)*100:5.2f}%'
                if self.filesize
                else "-----"
            )

            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] WK[{self.count:2d}/{self.n_workers:2d}] FR[{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}] PR[{_progress_str}] DL[{_speed_meter_str}] ETA[{_eta_smooth_str}]"

        elif self.status == "init_manipulating":
            msg = f"[HLS][{self.info_dict['format_id']}]: Waiting for Ensambling \n"
        elif self.status == "manipulating":
            if self.filename.exists():
                _size = self.filename.stat().st_size
            else:
                _size = 0
            _str = (
                f"[{naturalsize(_size)}/{naturalsize(self.filesize)}]({(_size/self.filesize)*100:.2f}%)"
                if self.filesize
                else f"[{naturalsize(_size)}]"
            )
            msg = f"[HLS][{self.info_dict['format_id']}]: Ensambling {_str} \n"

        return msg
