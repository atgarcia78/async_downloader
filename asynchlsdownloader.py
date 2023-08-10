import asyncio
import binascii
import json
import logging
import random
import threading
import time
import copy
import math

from datetime import timedelta, datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from shutil import rmtree
from functools import partial
from argparse import Namespace

import aiofiles
from aiofiles import os
import httpx
from httpx._types import ProxiesTypes
import m3u8

from Cryptodome.Cipher import AES

from yt_dlp.extractor.nakedsword import NakedSwordBaseIE

from utils import (
    CONF_HLS_RESET_403_TIME,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    load_config_extractors,
    getter_basic_config_extr,
    FrontEndGUI,
    ProgressTimer,
    SmoothETA,
    SpeedometerMA,
    _for_print,
    get_format_id,
    int_or_none,
    limiter_non,
    limiter_0_1,
    my_dec_on_exception,
    naturalsize,
    print_norm_time,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    try_get,
    CountDowns,
    myYTDL,
    async_waitfortasks,
    async_wait_for_any,
    Union,
    cast,
    MySyncAsyncEvent,
    put_sequence,
    async_lock,
    StatusStop,
    wait_for_either,
    change_status_nakedsword,
    get_host,
    LimitContextDecorator,
    send_http_request,
    ReExtractInfo,
    StatusError503,
    Token,
    empty_queue
)

logger = logging.getLogger("async_HLS_DL")

kill_token = Token("kill")


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


retry = my_dec_on_exception(AsyncHLSDLErrorFatal, max_tries=5, raise_on_giveup=True, interval=5)

on_exception = my_dec_on_exception(
    (TimeoutError, AsyncHLSDLError, ReExtractInfo), max_tries=5, raise_on_giveup=False, interval=5
)

on_503 = my_dec_on_exception(StatusError503, max_time=360, raise_on_giveup=False, interval=20)


class InReset403:
    def __init__(self):
        self.inreset = set()

    def add(self, member):
        self.inreset.add(member)
        change_status_nakedsword("403")

    def remove(self, member):
        if member in self.inreset:
            self.inreset.remove(member)
        if not self.inreset:
            change_status_nakedsword("NORMAL")


class AsyncHLSDownloader:
    _CHUNK_SIZE = 102400
    _MAX_RETRIES = 5
    _MAX_RESETS = 10
    _MIN_TIME_RESETS = 15
    _CONFIG = load_config_extractors()
    _CLASSLOCK = threading.Lock()
    _COUNTDOWNS = None
    _QUEUE = {}
    _INPUT = Queue()
    _INRESET_403 = InReset403()
    _qproxies = None

    def __init__(self, args: Namespace, video_dict: dict, vid_dl):
        try:
            self.background_tasks = set()
            self.tasks = []
            self.info_dict = video_dict
            self.vid_dl = vid_dl
            self.args = args
            self.n_workers: int = self.vid_dl.info_dl["n_workers"]
            self.count: int = 0
            self.m3u8_doc = None
            self.ytdl: myYTDL = self.vid_dl.info_dl["ytdl"]
            self.verifycert: bool = not self.ytdl.params.get("nocheckcertificate")
            self.timeout: httpx.Timeout = httpx.Timeout(15, connect=15)
            self.limits: httpx.Limits = httpx.Limits(
                max_keepalive_connections=None, max_connections=None, keepalive_expiry=30
            )
            self.base_download_path = Path(self.info_dict["download_path"])
            _filename = Path(self.info_dict.get("_filename", self.info_dict.get("filename")))
            self.download_path = Path(self.base_download_path, self.info_dict["format_id"])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.init_file = Path(self.base_download_path, f"init_file.{self.info_dict['format_id']}")
            self.fragments_base_path = Path(
                self.download_path, f'{_filename.stem}.{self.info_dict["format_id"]}.{self.info_dict["ext"]}'
            )
            self.filename = Path(
                self.base_download_path, f'{_filename.stem}.{self.info_dict["format_id"]}.ts'
            )

            self.key_cache = {}
            self.n_reset = 0
            self._limit_reset = limiter_0_1.ratelimit("resetdl", delay=True)
            self.down_size = 0
            self.status = "init"
            self.error_message = ""
            self.upt = {}
            self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_hlsdl")
            self.special_extr: bool = False

            self.filesize = self.info_dict.get("filesize") or self.info_dict.get("filesize_approx")

            self.premsg = "".join(
                [
                    f'[{self.info_dict["id"]}]',
                    f'[{self.info_dict["title"]}]',
                    f'[{self.info_dict["format_id"]}]',
                ]
            )

            self.count_msg = ""

            self._proxy = {}

            if _proxy := cast(str, self.args.proxy):
                self._proxy = {"http://": _proxy, "https://": _proxy}
            elif self.args.enproxy:
                with AsyncHLSDownloader._CLASSLOCK:
                    if not AsyncHLSDownloader._qproxies:
                        _seq = zip(
                            random.sample(range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST),
                            random.sample(range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST),
                        )

                        AsyncHLSDownloader._qproxies = put_sequence(Queue(), _seq)

                el1, el2 = cast(tuple, AsyncHLSDownloader._qproxies.get())
                _proxy_port = CONF_PROXIES_BASE_PORT + el1 * 100 + el2
                _proxy = f"http://127.0.0.1:{_proxy_port}"
                self._proxy = {"http://": _proxy, "https://": _proxy}

                try:
                    _gvd_pl = True
                    if "gvdblog.com" not in (_url := self.info_dict["original_url"]):
                        _gvd_pl = False
                        _url = self.info_dict["webpage_url"]
                    info = self.multi_extract_info(_url, proxy=_proxy)
                    if info:
                        if _len := len(info.get("entries", [])):
                            if _len == 1:
                                _pl_index = 1
                            else:
                                _pl_index = (
                                    self.info_dict.get("__gvd_playlist_index", 1)
                                    if _gvd_pl
                                    else (
                                        self.info_dict.get("playlist_index")
                                        or self.info_dict.get("playlist_autonumber")
                                        or 1
                                    )
                                )
                            info = info["entries"][_pl_index - 1]
                        new_info = get_format_id(info, self.info_dict["format_id"])
                        _info = {key: new_info[key] for key in ("url", "manifest_url") if key in new_info}
                        self.info_dict.update(_info)
                except Exception as e:
                    logger.exception("[init info proxy] %s", repr(e))

            if self.filename.exists() and self.filename.stat().st_size > 0:
                self.status = "done"

            self.smooth_eta = SmoothETA()
            self.progress_timer = ProgressTimer()
            self.speedometer = SpeedometerMA()
            self.frags_queue = asyncio.Queue()
            self._asynclock = asyncio.Lock()
            self._event_lock = asyncio.Lock()
            self.areset = sync_to_async(self.resetdl, thread_sensitive=False, executor=self.ex_dl)
            self._test = []
            self._speed = []

            self.init_client = httpx.Client(
                proxies=cast(ProxiesTypes, self._proxy),
                follow_redirects=True,
                headers=self.info_dict["http_headers"],
                limits=self.limits,
                timeout=self.timeout,
                verify=False,
            )
            self.auto_pasres = False
            self.fromplns = False
            self._extractor = cast(str, try_get(self.info_dict.get("extractor_key"), lambda x: x.lower()))
            self.m3u8_doc = ""
            self._host = get_host(self.info_dict["url"])
            self.frags_to_dl = []
            self.info_frag = []
            self.info_init_section = {}
            self.n_dl_fragments = 0
            self.info_decrypt = {}
            self.init()

        except Exception as e:
            logger.exception(repr(e))

    def add_task(self, coro, *, name=None):
        _task = asyncio.create_task(coro, name=name)
        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    def init(self):

        @on_503
        @on_exception
        def get_m3u8_doc():
            return try_get(
                send_http_request(self.info_dict["url"], client=self.init_client, new_e=AsyncHLSDLError),
                lambda x: x.content.decode("utf-8", "replace") if x else None,
            )

        @on_503
        @on_exception
        def get_key(key_uri):
            return try_get(
                send_http_request(key_uri, client=self.init_client, new_e=AsyncHLSDLError),
                lambda x: x.content if x else None,
            )

        def getter(name: Union[str, None]) -> tuple[int, Union[int, float], LimitContextDecorator]:
            try:
                if not name:
                    self.special_extr = False
                    return (self.n_workers, 0, limiter_non.ratelimit("transp", delay=True))

                if "nakedsword" in name:
                    self.auto_pasres = True
                    if not any(
                        _ in self.info_dict.get("playlist_title", "")
                        for _ in ("MostWatchedScenes", "Search")
                    ):
                        self.fromplns = self.info_dict.get("_id_movie", None)

                    if self.fromplns:
                        if not self.vid_dl.info_dl["fromplns"].get("ALL"):
                            self.vid_dl.info_dl["fromplns"]["ALL"] = {
                                "sem": threading.BoundedSemaphore(value=1),
                                "downloading": set(),
                                "in_reset": set(),
                                "reset": MySyncAsyncEvent("fromplns[ALL]", initset=True),
                            }
                        if not self.vid_dl.info_dl["fromplns"].get(self.fromplns):
                            self.vid_dl.info_dl["fromplns"][self.fromplns] = {
                                "downloaders": {self.vid_dl.info_dict["_index_scene"]: self.vid_dl},
                                "downloading": set(),
                                "in_reset": set(),
                                "reset": MySyncAsyncEvent(f"fromplns[{self.fromplns}]", initset=True),
                                "sem": threading.BoundedSemaphore(value=1),
                            }
                        else:
                            self.vid_dl.info_dl["fromplns"][self.fromplns]["downloaders"].update(
                                {self.vid_dl.info_dict["_index_scene"]: self.vid_dl}
                            )

                        _downloaders = self.vid_dl.info_dl["fromplns"][self.fromplns]["downloaders"]
                        logger.debug(
                            f"{self.premsg}: "
                            + f"added new dl to plns [{self.fromplns}], "
                            + f"count [{len(_downloaders)}] "
                            + f"members[{list(_downloaders.keys())}]"
                        )

            except Exception as e:
                logger.exception(f"{self.premsg}: {str(e)}")

            value, key_text = getter_basic_config_extr(name, AsyncHLSDownloader._CONFIG) or (None, None)

            self.special_extr = False
            if value and key_text:
                self.special_extr = True
                if "nakedsword" in key_text:
                    key_text = "nakedsword"
                return (
                    value["maxsplits"],
                    value["interval"],
                    value["ratelimit"].ratelimit(key_text, delay=True),
                )
            return (self.n_workers, 0, limiter_non.ratelimit("transp", delay=True))

        try:
            _nworkers, self._interv, self._limit = getter(self._extractor)
            self.n_workers = min(self.n_workers, _nworkers)

            self.m3u8_doc = get_m3u8_doc()
            self.info_dict["fragments"] = self.get_info_fragments()

            if (_initfrag := self.info_dict["fragments"][0].init_section):
                _file_path = Path(str(self.fragments_base_path) + ".Frag0")
                _url = _initfrag.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="
                _cipher = None
                if _initfrag.key and _initfrag.key.method == "AES-128" and _initfrag.key.iv:
                    if (_key := get_key(_initfrag.key.absolute_uri)):
                        _cipher = AES.new(_key, AES.MODE_CBC, binascii.unhexlify(_initfrag.key.iv[2:]))
                self.info_init_section.update(
                    {"frag": 0, "url": _url, "file": _file_path, "downloaded": False})
                self.get_init_section(_url, _file_path, _cipher)

            if self.init_file.exists():
                with open(self.init_file, "rt", encoding='utf-8') as finit:
                    init_data = json.loads(finit.read())
                init_data = {int(k): v for k, v in init_data.items()}

            else:
                init_data = {}

            byte_range = {}

            for i, fragment in enumerate(self.info_dict["fragments"]):
                if not fragment.uri and fragment.parts:
                    fragment.uri = fragment.parts[0].uri

                if fragment.byterange:
                    splitted_byte_range = fragment.byterange.split("@")
                    sub_range_start = (
                        int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range["end"])
                    byte_range = {
                        "start": sub_range_start,
                        "end": sub_range_start + int(splitted_byte_range[0])
                    }

                else:
                    byte_range = {}

                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="
                is_dl = False
                size = None
                hsize = init_data.get(i + 1)

                cipher = None
                if fragment.key:
                    cipher = traverse_obj(self.key_cache, (fragment.key.uri, "cipher"))

                _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i + 1}")
                if _file_path.exists():
                    size = _file_path.stat().st_size
                    is_dl = True
                    if not size or (hsize and not hsize - 100 <= size <= hsize + 100):
                        is_dl = False
                        _file_path.unlink()
                        size = None

                _frag = {
                    "frag": i + 1,
                    "url": _url,
                    "key": fragment.key,
                    "cipher": cipher,
                    "file": _file_path,
                    "byterange": byte_range,
                    "downloaded": is_dl,
                    "headersize": hsize,
                    "size": size,
                    "n_retries": 0,
                    "error": [],
                }

                if is_dl and size:
                    self.down_size += size
                    self.n_dl_fragments += 1
                if not is_dl or not hsize:
                    self.frags_to_dl.append(i + 1)

                self.info_frag.append(_frag)

            logger.debug(
                "".join([
                    f"{self.premsg}: Frags already DL: {len(self.fragsdl())}, ",
                    f"Frags not DL: {len(self.fragsnotdl())}, ",
                    f"Frags to request: {len(self.frags_to_dl)}"
                ])
            )

            self.n_total_fragments = len(self.info_dict["fragments"])
            self.format_frags = f"{(int(math.log(self.n_total_fragments, 10)) + 1)}d"

            self.totalduration = cast(int, self.info_dict.get("duration", self.calculate_duration()))

            if not self.filesize:
                self.filesize = self.calculate_filesize()

            if not self.filesize:
                _est_size = "NA"

            else:
                _est_size = naturalsize(self.filesize)

            logger.debug(
                "".join([
                    f"{self.premsg}: total duration ",
                    f"{print_norm_time(self.totalduration)} -- ",
                    f"estimated filesize {_est_size} -- already downloaded ",
                    f"{naturalsize(self.down_size)} -- total fragments ",
                    f"{self.n_total_fragments} -- fragments already dl ",
                    f"{self.n_dl_fragments}"
                ])
            )

            if not self.frags_to_dl:
                self.status = "init_manipulating"
        except Exception as e:
            logger.exception(f"{self.premsg}[init] {repr(e)}")
            self.status = "error"
            self.init_client.close()

    def calculate_duration(self):
        totalduration = 0
        for fragment in self.info_dict["fragments"]:
            totalduration += fragment.duration
        return totalduration

    def calculate_filesize(self):
        _filesize = None
        if _bitrate := cast(float, traverse_obj(self.info_dict, "tbr", "abr")):
            _filesize = int(self.totalduration * 1000 * _bitrate / 8)
        return _filesize

    def get_info_fragments(self):

        @on_503
        @on_exception
        def get_key(key_uri):
            return try_get(
                send_http_request(key_uri, client=self.init_client, new_e=AsyncHLSDLError),
                lambda x: x.content if x else None,
            )

        try:
            self._host = get_host(self.info_dict["url"])
            m3u8_obj = m3u8.loads(self.m3u8_doc, self.info_dict["url"])

            if not m3u8_obj or not m3u8_obj.segments:
                raise AsyncHLSDLError("couldnt get m3u8 file")

            if m3u8_obj.keys:
                for _key in m3u8_obj.keys:
                    if _key and _key.method == "AES-128" and _key.iv:
                        if not (_cipher := traverse_obj(self.key_cache, (_key.uri, "cipher"))):
                            if (_valkey := get_key(_key.absolute_uri)):
                                _cipher = AES.new(_valkey, AES.MODE_CBC, binascii.unhexlify(_key.iv[2:]))
                                self.key_cache[_key.uri] = {"key": _valkey, "cipher": _cipher}

            if _start_time := self.info_dict.get("_start_time"):
                logger.info(f"{self.premsg}[get_info_fragments] start time {_start_time}")
                _duration = try_get(
                    getattr(m3u8_obj, "target_duration", None), lambda x: float(x) if x else None
                )
                logger.info(f"{self.premsg}[get_info_fragments] duration {_duration}")
                if _duration:
                    _start_segment = int(_start_time // _duration) - 1
                    logger.info(f"{self.premsg}[get_info_fragments] start seg {_start_segment}")
                    if _end_time := self.info_dict.get("_end_time"):
                        _last_segment = min(int(_end_time // _duration), len(m3u8_obj.segments))
                    else:
                        _last_segment = len(m3u8_obj.segments)
                    logger.info(f"{self.premsg}[get_info_fragments] last seg {_last_segment}")

                    return m3u8_obj.segments[_start_segment:_last_segment]

            return m3u8_obj.segments

        except Exception as e:
            logger.error(f"{self.premsg}[get_info_fragments] - {repr(e)}")
            raise AsyncHLSDLErrorFatal("error get info fragments") from e

    def check_any_event_is_set(self, incpause=True):
        if incpause:
            return any(
                [
                    self.vid_dl.pause_event.is_set(),
                    self.vid_dl.reset_event.is_set(),
                    self.vid_dl.stop_event.is_set(),
                ]
            )
        return any([self.vid_dl.reset_event.is_set(), self.vid_dl.stop_event.is_set()])

    def check_stop(self):
        if self.vid_dl.stop_event.is_set():
            raise StatusStop("stop event")

    @on_503
    @on_exception
    def get_init_section(self, uri, file, cipher):
        try:
            if res := send_http_request(uri, client=self.init_client, new_e=AsyncHLSDLError):
                _data = res.content if not cipher else cipher.decrypt(res.content)
                with open(file, "wb") as fsect:
                    fsect.write(_data)
                self.info_init_section.update({"downloaded": True})
            else:
                raise AsyncHLSDLError(
                    f"{self.premsg}:[get_init_section] couldnt get init section")
        except Exception as e:
            logger.exception(f"{self.premsg}:[get_init_section] {repr(e)}")
            raise

    def multi_extract_info(self, url, proxy=None, msg=None):
        premsg = "[multi_extract_info]"
        if msg:
            premsg += msg

        try:
            self.check_stop()

            if not self.args.proxy and proxy:
                with myYTDL(params=self.ytdl.params, proxy=proxy) as proxy_ytdl:
                    _info_video = proxy_ytdl.sanitize_info(proxy_ytdl.extract_info(url, download=False))
            else:
                # if proxy was included in args main program, ytdl will have this proxy in its params
                _info_video = self.ytdl.sanitize_info(self.ytdl.extract_info(url, download=False))

            self.check_stop()

            if not _info_video:
                raise AsyncHLSDLErrorFatal("no info video")
            return _info_video

        except StatusStop:
            raise
        except AsyncHLSDLErrorFatal:
            raise
        except Exception as e:
            logger.error(f"{premsg} fails when extracting info {repr(e)}")
            raise AsyncHLSDLErrorFatal("error extracting info video") from e

    def prep_reset(self, info_reset):
        self.info_dict.update(info_reset)

        self.init_client.close()

        self.init_client = httpx.Client(
            proxies=cast(ProxiesTypes, self._proxy),
            follow_redirects=True,
            headers=self.info_dict["http_headers"],
            limits=self.limits,
            timeout=self.timeout,
            verify=False,
        )

        @on_503
        @on_exception
        def get_key(key_uri):
            return try_get(
                send_http_request(key_uri, client=self.init_client, new_e=AsyncHLSDLError),
                lambda x: x.content if x else None,
            )

        self.frags_to_dl = []

        self.info_dict["fragments"] = self.get_info_fragments()

        if self.init_file.exists():
            with open(self.init_file, "rt", encoding='utf-8') as finit:
                init_data = json.loads(finit.read())
            init_data = {int(_key): _value for _key, _value in init_data.items()}
        else:
            init_data = {}

        byte_range = {}

        for i, fragment in enumerate(self.info_dict["fragments"]):
            if not fragment.uri and fragment.parts:
                fragment.uri = fragment.parts[0].uri

            if fragment.byterange:
                splitted_byte_range = fragment.byterange.split("@")
                sub_range_start = (
                    int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range["end"])
                byte_range = {
                    "start": sub_range_start,
                    "end": sub_range_start + int(splitted_byte_range[0])}

            else:
                byte_range = {}

            try:
                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="
                if not self.info_frag[i]["headersize"]:
                    if _hsize := init_data.get(i + 1):
                        self.info_frag[i]["headersize"] = _hsize
                if not self.info_frag[i]["downloaded"] or (
                    self.info_frag[i]["downloaded"] and not self.info_frag[i]["headersize"]
                ):
                    self.frags_to_dl.append(i + 1)
                    self.info_frag[i]["url"] = _url
                    if not self.info_frag[i]["downloaded"] and self.info_frag[i]["file"].exists():
                        self.info_frag[i]["file"].unlink()

                    self.info_frag[i]["n_retries"] = 0
                    self.info_frag[i]["byterange"] = byte_range
                    self.info_frag[i]["key"] = fragment.key

                    cipher = None
                    if fragment.key:
                        cipher = traverse_obj(self.key_cache, (fragment.key.uri, "cipher"))

                    self.info_frag[i]["cipher"] = cipher

            except Exception:
                logger.debug(
                    "".join([
                        f"{self.premsg}:RESET[{self.n_reset}]:prep_reset error ",
                        f"with i = [{i}] \n\ninfo_dict['fragments'] ",
                        f"{len(self.info_dict['fragments'])}\n\n",
                        f"{[str(frag) for frag in self.info_dict['fragments']]}\n\n",
                        f"info_frag {len(self.info_frag)}"
                    ])
                )

                raise

        if not self.frags_to_dl:
            self.status = "init_manipulating"
        else:
            logger.debug(
                "".join([
                    f"{self.premsg}:RESET[{self.n_reset}]:prep_reset:OK ",
                    f"{self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}"
                ])
            )

    @retry
    def get_reset_info(self, _reset_url, plns=True, first=False):
        _proxy = cast(str, traverse_obj(self._proxy, "http://"))
        _print_proxy = f':proxy[{_proxy.split(":")[-1]}]' if _proxy else ""
        _print_plns = f":PLNS[{self.fromplns}]:first[{first}]" if self.fromplns else ""

        _pre = f"{self.premsg}[get_reset_inf]{_print_plns}{_print_proxy}:RESET[{self.n_reset}]"

        logger.debug(_pre)

        _info = None
        info_reset = None

        try:
            self.check_stop()
            if plns and self.fromplns:
                if first:
                    _info = self.multi_extract_info(_reset_url, proxy=_proxy, msg=_pre)
                    self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), _info)

                else:
                    _info = self.ytdl.cache.load("nakedswordmovie", str(self.fromplns))
                    if not _info:
                        _info = self.multi_extract_info(_reset_url, proxy=_proxy, msg=_pre)
                        self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), _info)

                if _info:
                    info_reset = try_get(
                        traverse_obj(_info, ("entries", int(self.vid_dl.info_dict["_index_scene"]) - 1)),
                        lambda x: get_format_id(x, self.info_dict["format_id"]) if x else None,
                    )

            else:
                _info = self.multi_extract_info(_reset_url, proxy=_proxy, msg=_pre)
                if _info:
                    if _info.get("entries") and (_pl_index := self.info_dict["playlist_index"]):
                        _info = _info["entries"][_pl_index - 1]
                    info_reset = get_format_id(_info, self.info_dict["format_id"])

            self.check_stop()

            if not info_reset:
                raise AsyncHLSDLErrorFatal(f"{_pre} fails no descriptor")

            logger.debug(f"{_pre} format extracted info video ok\n{_for_print(info_reset)}")

            try:
                self.prep_reset(info_reset)
                return {"res": "ok"}
            except Exception as e:
                logger.exception(f"{_pre} Exception occurred when reset {repr(e)}")
                raise AsyncHLSDLErrorFatal("RESET fails: preparation frags failed") from e

        except StatusStop as e:
            logger.debug(f"{_pre} check stop {repr(e)}")
            raise
        except AsyncHLSDLErrorFatal as e:
            logger.debug(f"{_pre} {repr(e)}")
            raise
        except Exception as e:
            logger.error(f"{_pre}fails when extracting reset info {repr(e)}")
            return {"error": e}

    def resetdl(self, cause=None):
        _proxy = self._proxy["http://"] if getattr(self, "_proxy", None) else None
        _print_proxy = f':proxy[{_proxy.split(":")[-1]}]' if _proxy else ""

        _pre = (
            f"{self.premsg}[resetdl]:CAUSE[{cause}]:RESET[{self.n_reset}]:PLNS[{self.fromplns}]{_print_proxy}"
        )

        logger.debug(_pre)

        _pasres_cont = False

        try:
            if str(cause) == "403":
                AsyncHLSDownloader._INRESET_403.add(self.info_dict["id"])
                with AsyncHLSDownloader._CLASSLOCK:
                    _pasres_cont = FrontEndGUI.pasres_break()
                    if not AsyncHLSDownloader._COUNTDOWNS:
                        AsyncHLSDownloader._COUNTDOWNS = CountDowns(AsyncHLSDownloader, logger=logger)
                        logger.debug(f"{self.premsg}:RESET[{self.n_reset}] new COUNTDOWN")

                AsyncHLSDownloader._COUNTDOWNS.add(
                    CONF_HLS_RESET_403_TIME,
                    index=str(self.vid_dl.index),
                    event=self.vid_dl.stop_event,
                    msg=self.premsg,
                )

                self.check_stop()

                logger.info(f"{_pre} fin wait in reset cause 403")

            if self.fromplns and str(cause) in ("403"):
                with (_sem := self.vid_dl.info_dl["fromplns"]["ALL"]["sem"]):
                    logger.debug(f"{self.premsg}:RESET[{self.n_reset}] in sem")

                    _first_all = False
                    if _sem._initial_value == 1:
                        _first_all = True

                    with (_sem2 := self.vid_dl.info_dl["fromplns"][self.fromplns]["sem"]):
                        logger.debug(f"{_pre} in sem2")

                        _first = False
                        if _sem2._initial_value == 1:
                            _first = True

                        if _first_all:
                            NakedSwordBaseIE.API_LOGOUT(msg="[resetdl]")
                            time.sleep(5)
                            NakedSwordBaseIE.API_LOGIN(msg="[resetdl]")
                            time.sleep(2)

                        _listreset = [
                            int(index)
                            for index in list(self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"])
                        ]
                        _aux = {
                            "indexdl": self.vid_dl.index,
                            "args": {"nakedswordmovie": {"listreset": _listreset}},
                        }

                        _plns_url = smuggle_url(self.info_dict["original_url"], _aux)

                        _resinfo = self.get_reset_info(_plns_url, plns=True, first=_first)

                        if "error" in _resinfo:
                            raise _resinfo["error"]
                        if "res" in _resinfo:
                            if _first:
                                _sem2._initial_value = 100
                                _sem2.release(50)
                            if _first_all:
                                _sem._initial_value = 100
                                _sem.release(50)

                            time.sleep(1)

                        self.check_stop()

            else:
                if self.special_extr:
                    _webpage_url = smuggle_url(self.info_dict["webpage_url"], {"indexdl": self.vid_dl.index})
                else:
                    _webpage_url = self.info_dict["webpage_url"]

                self.get_reset_info(_webpage_url, plns=False)

        except StatusStop:
            logger.error(f"{_pre} stop_event")
        except Exception as e:
            logger.exception(
                f"{_pre} stop_event:[{self.vid_dl.stop_event.is_set()}] outer Exception {repr(e)}"
            )
            raise
        finally:
            if cause == "403":
                try:
                    AsyncHLSDownloader._INRESET_403.remove(self.info_dict["id"])
                except Exception:
                    logger.warning(
                        "".join([
                            f'{_pre} error when removing[{self.info_dict["id"]}] ',
                            f'from [{AsyncHLSDownloader._INRESET_403}]'
                        ])
                    )

            if self.fromplns and cause == "403":
                logger.debug(f"{_pre} stop_event[{self.vid_dl.stop_event.is_set()}] FINALLY")

                with AsyncHLSDownloader._CLASSLOCK:
                    _inreset = self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]
                    try:
                        _inreset.remove(self.vid_dl.info_dict["_index_scene"])
                    except Exception:
                        logger.warning(
                            "".join([
                                f'{_pre} error when removing[{self.vid_dl.info_dict["_index_scene"]}] ',
                                f"from inreset[{self.fromplns}] {_inreset}"
                            ])
                        )

                    if not self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]:
                        logger.debug(f"{_pre} end of resets fromplns [{self.fromplns}]")

                        self.vid_dl.info_dl["fromplns"][self.fromplns]["reset"].set()
                        self.vid_dl.info_dl["fromplns"][self.fromplns]["sem"] = threading.BoundedSemaphore(
                            value=1
                        )

                if self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]:
                    logger.info(
                        f"{_pre} waits for rest scenes in [{self.fromplns}] to start DL "
                        + f"[{self.vid_dl.info_dl['fromplns'][self.fromplns]['in_reset']}]"
                    )

                    wait_for_either(
                        [self.vid_dl.info_dl["fromplns"][self.fromplns]["reset"], self.vid_dl.stop_event]
                    )

                with AsyncHLSDownloader._CLASSLOCK:
                    try:
                        self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"].remove(self.fromplns)
                    except Exception:
                        pass

                    if not self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"]:
                        logger.debug(f"{_pre} end for all plns ")

                        self.vid_dl.info_dl["fromplns"]["ALL"]["reset"].set()
                        self.vid_dl.info_dl["fromplns"]["ALL"]["sem"] = threading.BoundedSemaphore(value=1)
                        self.n_reset += 1
                        if _pasres_cont:
                            FrontEndGUI.pasres_continue()
                        logger.debug(f"{_pre}  exit reset")
                        return

                if self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"]:
                    logger.info(
                        "".join([
                            f"{_pre} all scenes in [{self.fromplns}], waiting for scenes in other plns ",
                            f"[{self.vid_dl.info_dl['fromplns']['ALL']['in_reset']}]"
                        ])
                    )

                    wait_for_either([self.vid_dl.info_dl["fromplns"]["ALL"]["reset"], self.vid_dl.stop_event])

                    self.n_reset += 1
                    if _pasres_cont:
                        FrontEndGUI.pasres_continue()
                    logger.debug(f"{_pre} exit reset")
                    return
            else:
                self.n_reset += 1
                logger.debug(f"{_pre} exit reset")

    async def upt_status(self):
        _timer = ProgressTimer()
        while not self.vid_dl.end_tasks.is_set():
            if _timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):
                if self.down_size and not self.check_any_event_is_set():
                    _down_size = self.down_size
                    _speed_meter = self.speedometer(_down_size)

                    self.upt.update({"speed_meter": _speed_meter, "down_size": _down_size})

                    if _speed_meter and self.filesize:
                        _est_time = (self.filesize - _down_size) / _speed_meter
                        _est_time_smooth = self.smooth_eta(_est_time)
                        self.upt.update({"est_time": _est_time, "est_time_smooth": _est_time_smooth})

                    self._speed.append((datetime.now(), copy.deepcopy(self.upt)))
                    self._test.append((time.monotonic(), copy.deepcopy(self.upt)))

            await asyncio.sleep(0)

    async def event_handle(self, msg):
        _res = {}
        if self.vid_dl.pause_event.is_set():
            logger.debug(f"{msg}[handle] pause detected")
            _res["pause"] = True
            async with self._event_lock:
                if self.vid_dl.pause_event.is_set():
                    _res = _res | await async_wait_for_any(
                        [self.vid_dl.resume_event, self.vid_dl.reset_event, self.vid_dl.stop_event]
                    )
                    logger.debug(f"{msg}[handle] after wait pause: {_res}")
                    if "resume" in _res["event"]:
                        _res["event"].remove("resume")
                    if not _res["event"]:
                        _res.pop("event", None)
                    self.vid_dl.resume_event.clear()
                    self.vid_dl.pause_event.clear()
                    await asyncio.sleep(0)
                    return _res
                logger.debug(f"{msg}[handle] after wait pause: {_res}")
        if _event := [_ev.name for _ev in (self.vid_dl.reset_event, self.vid_dl.stop_event) if _ev.is_set()]:
            _res["event"] = _event
        return _res

    async def fetch(self, nco):

        async def _decrypt(data, cipher):
            if cipher:
                return await sync_to_async(cipher.decrypt)(data)
            return data

        async def _update_counters(_bytes_dl, _old):
            _dif = -1
            if (_iter_bytes := _bytes_dl - _old) > 0:
                async with self._asynclock:
                    self.down_size += _iter_bytes
                    if self.filesize and (_dif := self.down_size - self.filesize) > 0:
                        self.filesize += _dif
                async with self.vid_dl.alock:
                    self.vid_dl.info_dl["down_size"] += _iter_bytes
                    if _dif > 0:
                        self.vid_dl.info_dl["filesize"] += _dif
            return _bytes_dl

        async def _clean_frag(index: int, _exc: BaseException):
            self.info_frag[index - 1]["error"].append(repr(_exc))
            self.info_frag[index - 1]["downloaded"] = False
            _fpath = Path(self.info_frag[index - 1]["file"])
            if await os.path.exists(_fpath):
                _sizefile = (await os.stat(_fpath)).st_size
                await os.remove(_fpath)
                async with self._asynclock:
                    self.down_size -= _sizefile
                async with self.vid_dl.alock:
                    self.vid_dl.info_dl["down_size"] -= _sizefile

        @on_503
        async def _download_frag(index: int):
            _premsg = f"{self.premsg}:[worker-{nco}]:[frag-{index}]"
            url = self.info_frag[index - 1]["url"]
            filename = Path(self.info_frag[index - 1]["file"])
            cipher = self.info_frag[index - 1].get("cipher")
            headers = {}
            if byte_range := self.info_frag[index - 1].get("byterange"):
                headers["range"] = f"bytes={byte_range['start']}-{byte_range['end'] - 1}"

            while self.info_frag[index - 1]["n_retries"] < self._MAX_RETRIES:
                try:
                    if self.check_any_event_is_set(incpause=False):
                        return

                    if self._interv:
                        async with self._limit:
                            await asyncio.sleep(self._interv)

                    async with (
                        aiofiles.open(filename, mode="ab") as fileobj,
                        client.stream("GET", url, headers=headers) as res,
                    ):
                        if res.status_code == 403:
                            if self.fromplns:
                                _wait_tasks = await self.vid_dl.reset_plns(
                                    "403", plns=self.fromplns)
                            else:
                                _wait_tasks = await self.vid_dl.reset("403")

                            if _wait_tasks:
                                done, pending = await asyncio.wait(_wait_tasks)
                                logger.debug(
                                    f"{_premsg}[wait_tasks] DONE\n{done}\nPENDING\n{pending}")
                            return

                        if res.status_code == 503:
                            self.info_frag[index - 1]["n_retries"] = 0
                            raise StatusError503(f"{_premsg}")

                        if res.status_code >= 400:
                            raise AsyncHLSDLError(f"Frag:{str(index)} resp code:{str(res)}")

                        if not (_hsize := self.info_frag[index - 1]["headersize"]):
                            _hsize = self.info_frag[index - 1]["headersize"] = int_or_none(
                                res.headers.get("content-length"))

                        if self.info_frag[index - 1]["downloaded"]:
                            _size = self.info_frag[index - 1]["size"] = (
                                await os.stat(filename)).st_size
                            if (_hsize and (_hsize - 100 <= _size <= _hsize + 100)) or not _hsize:
                                break

                            await fileobj.truncate(0)
                            self.info_frag[index - 1]["downloaded"] = False
                            self.info_frag[index - 1]["size"] = None
                            async with self._asynclock:
                                self.n_dl_fragments -= 1
                                self.down_size -= _size
                            async with self.vid_dl.alock:
                                self.vid_dl.info_dl["down_size"] -= _size

                        num_bytes_downloaded = res.num_bytes_downloaded
                        _timer = ProgressTimer()
                        _timer2 = ProgressTimer()
                        _buffer = b""
                        _tasks_chunks = []

                        if (
                            not (_chunk_size := self.info_frag[index - 1]["headersize"])
                            or _chunk_size > self._CHUNK_SIZE
                        ):
                            _chunk_size = self._CHUNK_SIZE

                        async for chunk in res.aiter_bytes(chunk_size=_chunk_size):
                            _check = await self.event_handle(_premsg)
                            if _ev := traverse_obj(_check, "event"):
                                if _tasks_chunks:
                                    _tasks_chunks[-1].cancel()
                                raise AsyncHLSDLErrorFatal(_ev)
                            if traverse_obj(_check, "pause"):
                                _timer.reset()
                                _timer2.reset()

                            if _data := await _decrypt(chunk, cipher):
                                _buffer += _data

                            if _timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):
                                num_bytes_downloaded = await _update_counters(
                                    res.num_bytes_downloaded, num_bytes_downloaded
                                )
                            if _timer2.has_elapsed(seconds=5 * CONF_INTERVAL_GUI):
                                if _tasks_chunks:
                                    await asyncio.wait(_tasks_chunks[-1:])
                                if _buffer:
                                    _tasks_chunks.append(
                                        self.add_task(
                                            fileobj.write(_buffer),
                                            name=f"write_chunks[{len(_tasks_chunks)}]",
                                        )
                                    )
                                    _buffer = b""

                        num_bytes_downloaded = await _update_counters(
                            res.num_bytes_downloaded, num_bytes_downloaded
                        )
                        if _tasks_chunks:
                            await asyncio.wait(_tasks_chunks[-1:])
                        if _buffer:
                            _tasks_chunks.append(
                                self.add_task(
                                    fileobj.write(_buffer),
                                    name=f"write_chunks[{len(_tasks_chunks)}]",
                                )
                            )
                            _buffer = b""
                            await asyncio.wait(_tasks_chunks[-1:])

                    _nsize = (await os.stat(filename)).st_size
                    _nhsize = self.info_frag[index - 1]["headersize"]

                    if (_nhsize and _nhsize - 100 <= _nsize <= _nhsize + 100) or not _nhsize:
                        self.info_frag[index - 1]["downloaded"] = True
                        self.info_frag[index - 1]["size"] = _nsize
                        async with self._asynclock:
                            self.n_dl_fragments += 1
                        break

                    logger.warning(
                        "".join([
                            f"{_premsg}: end of streaming. Fragment not completed\n",
                            f"{self.info_frag[index - 1]}"
                        ])
                    )
                    raise AsyncHLSDLError(f"fragment not completed frag[{index}]")

                except StatusError503 as e:
                    logger.debug(f"{_premsg}: Error: {repr(e)}")
                    await _clean_frag(index, e)
                    raise
                except (asyncio.CancelledError, RuntimeError, AsyncHLSDLErrorFatal) as e:
                    logger.debug(f"{_premsg}: Error: {repr(e)}")
                    await _clean_frag(index, e)
                    raise
                except Exception as e:
                    logger.debug(f"{_premsg}: Error: {repr(e)}")
                    await _clean_frag(index, e)
                    self.info_frag[index - 1]["n_retries"] += 1
                    if self.info_frag[index - 1]["n_retries"] >= self._MAX_RETRIES:
                        self.info_frag[index - 1]["error"].append("MaxLimitRetries")
                        logger.warning(f"{_premsg}: MaxLimitRetries:skip")
                        self.info_frag[index - 1]["skipped"] = True
                        break

        logger.debug(f"{self.premsg}:[worker-{nco}]: init worker")

        client = httpx.AsyncClient(
            proxies=cast(ProxiesTypes, self._proxy),
            limits=self.limits,
            follow_redirects=True,
            timeout=self.timeout,
            verify=self.verifycert,
            headers=self.info_dict["http_headers"],
        )

        try:
            while True:
                try:
                    if (qindex := self.frags_queue.get_nowait()) is None:
                        await asyncio.sleep(0)
                        continue
                    if qindex == kill_token:
                        logger.debug(f"{self.premsg}:[worker-{nco}] {kill_token}")
                        return

                    await _download_frag(cast(int, qindex))

                except (asyncio.CancelledError, RuntimeError, AsyncHLSDLErrorFatal, asyncio.QueueEmpty) as e:
                    logger.debug(f"{self.premsg}:[worker-{nco}] Error: {repr(e)}")
                    break
                except BaseException as e:
                    logger.exception(f"{self.premsg}:[worker-{nco}] inner exception {repr(e)}")

        except BaseException as e:
            logger.exception(f"{self.premsg}:[worker-{nco}] outer exception {repr(e)}")
        finally:
            async with self._asynclock:
                self.count -= 1
            logger.debug(f"{self.premsg}:[worker-{nco}] bye worker")
            await client.aclose()

    async def fetch_async(self):

        n_frags_dl = 0
        self.premsg = f"[{self.vid_dl.index}]{self.premsg}"
        AsyncHLSDownloader._QUEUE[str(self.vid_dl.index)] = Queue()

        async with async_lock(AsyncHLSDownloader._CLASSLOCK):
            if self.fromplns:
                _event = traverse_obj(self.vid_dl.info_dl["fromplns"], ("ALL", "reset"))
                if _event:
                    _res = await async_waitfortasks(
                        events=(_event, self.vid_dl.stop_event), background_tasks=self.background_tasks
                    )
                    if _res.get("event") == "stop":
                        return
                self.vid_dl.info_dl["fromplns"][self.fromplns]["downloading"].add(
                    self.vid_dl.info_dict.get("_index_scene", self.vid_dl.info_dict["_index_scene"])
                )
                self.vid_dl.info_dl["fromplns"]["ALL"]["downloading"].add(self.fromplns)

        _tstart = time.monotonic()

        try:
            while True:
                logger.debug(f"{self.premsg} TASKS INIT")
                try:
                    empty_queue(self.frags_queue)
                    for frag in self.frags_to_dl:
                        self.frags_queue.put_nowait(frag)
                    for _ in range(self.n_workers):
                        self.frags_queue.put_nowait(kill_token)
                    self.count = self.n_workers
                    self.vid_dl.reset_event.clear()
                    self.vid_dl.pause_event.clear()
                    self.vid_dl.resume_event.clear()
                    self.vid_dl.end_tasks.clear()
                    self.speedometer.reset(initial_bytes=self.down_size)
                    self.progress_timer.reset()
                    self.smooth_eta.reset()
                    self._test.append((time.monotonic(), "starting tasks to dl"))
                    self.status = "downloading"
                    self.count_msg = ""

                    upt_task = [self.add_task(self.upt_status(), name="upt_task")]

                    self.tasks = [
                        self.add_task(self.fetch(i), name=f"{self.premsg}[{i}]")
                        for i in range(self.n_workers)
                    ]

                    done, pending = await asyncio.wait(self.tasks)

                    self.vid_dl.end_tasks.set()

                    logger.debug(
                        f"{self.premsg}[fetch_async]:done[{len(list(done))}]:pending[{len(list(pending))}]"
                    )

                    _nfragsdl = len(self.fragsdl())
                    inc_frags_dl = _nfragsdl - n_frags_dl
                    n_frags_dl = _nfragsdl

                    if n_frags_dl == len(self.info_dict["fragments"]):
                        await asyncio.wait(upt_task)
                        return

                    if self.vid_dl.stop_event.is_set():
                        self.status = "stop"
                        await asyncio.wait(upt_task)
                        return

                    if _cause := self.vid_dl.reset_event.is_set():
                        dump_init_task = [self.add_task(self.dump_init_file())]
                        self.background_tasks.add(dump_init_task[0])

                        await asyncio.wait(dump_init_task + upt_task)

                        if self.n_reset < self._MAX_RESETS:
                            if _cause in ("403", "hard"):
                                if self.fromplns:
                                    await self.vid_dl.back_from_reset_plns(
                                        self.premsg, plns=self.fromplns
                                    )
                                    if self.vid_dl.stop_event.is_set():
                                        return
                                _cause = self.vid_dl.reset_event.is_set()
                                self._speed.append((datetime.now(), _cause))
                                logger.debug(f"{self.premsg}:RESET[{self.n_reset}]:CAUSE[{_cause}]")

                            elif _cause == "manual":
                                self._speed.append((datetime.now(), _cause))
                                logger.debug(f"{self.premsg}:RESET[{self.n_reset}]:CAUSE[{_cause}]")
                                continue

                            try:
                                async with self._limit_reset:
                                    await self.areset(_cause)
                                if self.vid_dl.stop_event.is_set():
                                    return
                                if ((_t := time.monotonic()) - _tstart) < self._MIN_TIME_RESETS:
                                    _minus = self.n_workers // 4
                                    self.n_workers -= _minus
                                _tstart = _t
                                logger.debug(
                                    "".join([
                                        f"{self.premsg}:RESET[{self.n_reset}]:OK:Pending frags\n",
                                        f"{len(self.fragsnotdl())}"
                                    ])
                                )
                                await asyncio.sleep(0)
                                continue

                            except Exception as e:
                                logger.exception(
                                    "".join([
                                        f"{self.premsg}:RESET[{self.n_reset}]:",
                                        f"ERROR reset couldnt progress:[{repr(e)}]"
                                    ])
                                )
                                self.status = "error"
                                await self.clean_when_error()
                                raise AsyncHLSDLErrorFatal(
                                    f"{self.premsg}: ERROR reset couldnt progress"
                                ) from e

                        else:
                            logger.warning(
                                f"{self.premsg}:RESET[{self.n_reset}]:ERROR:Max_number_of_resets"
                            )
                            self.status = "error"
                            await self.clean_when_error()
                            await asyncio.sleep(0)
                            raise AsyncHLSDLErrorFatal(f"{self.premsg}: ERROR max resets")

                    else:
                        if inc_frags_dl > 0:
                            logger.debug(
                                "".join([
                                    f"{self.premsg}: [{n_frags_dl} -> ",
                                    f"{inc_frags_dl}] new cycle with no fatal error"
                                ])
                            )
                            try:
                                async with self._limit_reset:
                                    await self.areset("hard")
                                if self.vid_dl.stop_event.is_set():
                                    return
                                logger.debug(
                                    "".join([
                                        f"{self.premsg}:RESET new cycle",
                                        f"[{self.n_reset}]:OK:Pending frags ",
                                        f"{len(self.fragsnotdl())}"
                                    ])
                                )
                                self.n_reset -= 1
                                continue

                            except Exception as e:
                                logger.exception(
                                    "".join([
                                        f"{self.premsg}:RESET",
                                        f"[{self.n_reset}]:ERROR reset ",
                                        f"couldnt progress:[{repr(e)}]"
                                    ])
                                )
                                self.status = "error"
                                await self.clean_when_error()
                                await asyncio.sleep(0)
                                raise AsyncHLSDLErrorFatal(
                                    f"{self.premsg}: ERROR reset couldnt progress"
                                ) from e

                        else:
                            logger.debug(
                                "".join([
                                    f"{self.premsg}: [{n_frags_dl} <-> ",
                                    f"{inc_frags_dl}] no improvement, ",
                                    "lets raise an error"
                                ])
                            )
                            self.status = "error"
                            raise AsyncHLSDLErrorFatal(
                                f"{self.premsg}: no changes in number of dl frags in one cycle"
                            )

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
                with AsyncHLSDownloader._CLASSLOCK:
                    try:
                        self.vid_dl.info_dl["fromplns"][self.fromplns]["downloading"].remove(
                            self.vid_dl.info_dict["_index_scene"]
                        )
                    except Exception:
                        logger.warning(
                            "".join([
                                f"{self.premsg}[fetch_async] error when removing ",
                                f"[{self.vid_dl.info_dict['_index_scene']}] from downloading",
                                f" [{self.vid_dl.info_dl['fromplns'][self.fromplns]['downloading']}]"
                            ])
                        )

                    if not self.vid_dl.info_dl["fromplns"][self.fromplns]["downloading"]:
                        self.vid_dl.info_dl["fromplns"]["ALL"]["downloading"].remove(self.fromplns)

            self.init_client.close()
            if not self.vid_dl.stop_event.is_set() and not self.status == "error":
                logger.debug(f"{self.premsg}:Frags DL completed")
                self.status = "init_manipulating"

    async def clean_from_reset(self):
        if not self.fromplns:
            return

        try:
            try:
                self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"].remove(
                    self.vid_dl.info_dict["_index_scene"]
                )
            except Exception:
                logger.warning(
                    f"{self.premsg}[clean_from_reset] error when removing "
                    + f'[{self.vid_dl.info_dict["_index_scene"]}] from '
                    + f'{self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]}'
                )

            if not self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]:
                self.vid_dl.info_dl["fromplns"][self.fromplns]["reset"].set()
                logger.info(f"{self.premsg} end of resets fromplns [{self.fromplns}]")

                try:
                    self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"].remove(self.fromplns)

                except Exception:
                    logger.warning(
                        f"{self.premsg}[clean_from_reset] error when removing "
                        + f"[{self.fromplns}] from "
                        + f'{self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"]}'
                    )

                if not self.vid_dl.info_dl["fromplns"]["ALL"]["in_reset"]:
                    self.vid_dl.info_dl["fromplns"]["ALL"]["reset"].set()

        except Exception:
            logger.warning(
                f"{self.premsg}[clean_from_reset] error when removing "
                + f'[{self.vid_dl.info_dict["_index_scene"]}] from'
                + f'{self.vid_dl.info_dl["fromplns"][self.fromplns]["in_reset"]}'
            )

    async def dump_init_file(self):
        init_data = {el["frag"]: el["headersize"] for el in self.info_frag if el["headersize"]}

        async with aiofiles.open(self.init_file, mode="w") as finit:
            await finit.write(json.dumps(init_data))

    async def clean_when_error(self):
        for frag in self.info_frag:
            if frag["downloaded"] is False and await os.path.exists(frag["file"]):
                await os.remove(frag["file"])

    def sync_clean_when_error(self):
        for frag in self.info_frag:
            if frag["downloaded"] is False and frag["file"].exists():
                frag["file"].unlink()

    async def ensamble_file(self):
        self.status = "manipulating"
        # logger.debug(f"{self.premsg}:{self.filename} Fragments DL \n{self.fragsdl()}")
        _skipped = 0

        try:
            async with aiofiles.open(self.filename, mode="wb") as dest:
                for frag in self.info_frag:
                    if frag.get("skipped", False):
                        _skipped += 1
                        continue
                    if not frag["size"] and await os.path.exists(frag["file"]):
                        frag["size"] = (await os.stat(frag["file"])).st_size

                    if frag["size"] and (
                        (frag["headersize"] and
                         (frag["headersize"] - 100 <= frag["size"] <= frag["headersize"] + 100))
                        or not frag["headersize"]
                    ):
                        async with aiofiles.open(frag["file"], mode="rb") as source:
                            await dest.write(await source.read())
                    else:
                        raise AsyncHLSDLError(f"{self.premsg}: error when ensambling: {frag}")

        except Exception as e:
            if await os.path.exists(self.filename):
                await os.remove(self.filename)
            logger.exception(f"{self.premsg}:Exception ocurred: {repr(e)}")
            self.status = "error"
            self.sync_clean_when_error()
            raise
        finally:
            if await os.path.exists(self.filename):
                armtree = sync_to_async(
                    partial(rmtree, ignore_errors=True), thread_sensitive=False, executor=self.ex_dl
                )
                await armtree(str(self.download_path))
                self.status = "done"
                logger.debug(f"{self.premsg}: [ensamble_file] file ensambled")
                if _skipped:
                    logger.warning(f"{self.premsg}: [ensamble_file] skipped frags [{_skipped}]")
            else:
                self.status = "error"
                await self.clean_when_error()
                raise AsyncHLSDLError(f"{self.premsg}: error when ensambling parts")

    def fragsnotdl(self):
        res = []
        for frag in self.info_frag:
            if (frag["downloaded"] is False) and not frag.get("skipped", False):
                res.append(frag)
        return res

    def fragsdl(self):
        res = []
        for frag in self.info_frag:
            if (frag["downloaded"] is True) or frag.get("skipped", False):
                res.append(frag)
        return res

    def print_hookup(self):

        _filesize_str = naturalsize(self.filesize) if self.filesize else "--"
        _pre = f"[HLS][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}]"

        if self.status == "done":
            return f"{_pre} Completed\n"

        if self.status == "init":
            return "".join(
                [
                    f"{_pre}: Waiting to DL [{_filesize_str}] ",
                    f"[{self.n_dl_fragments:{self.format_frags}}/{self.n_total_fragments}]\n",
                ]
            )

        if self.status == "error":
            _rel_size_str = (
                f"{naturalsize(self.down_size)}/{naturalsize(self.filesize)}" if self.filesize else "--"
            )
            return "".join(
                [
                    f"{_pre}: ERROR [{_rel_size_str}] ",
                    f"[{self.n_dl_fragments:{self.format_frags}}/{self.n_total_fragments}]\n",
                ]
            )

        if self.status == "stop":
            _rel_size_str = (
                f"{naturalsize(self.down_size)}/{naturalsize(self.filesize)}" if self.filesize else "--"
            )
            return "".join(
                [
                    f"{_pre}: STOPPED [{_rel_size_str}] ",
                    f"[{self.n_dl_fragments:{self.format_frags}}/{self.n_total_fragments}]\n",
                ]
            )

        if self.status == "downloading":
            _temp = copy.deepcopy(self.upt)
            _dsize = _temp.get("down_size", self.down_size)
            if self.filesize:
                _progress_str = f"{(_dsize/self.filesize)*100:5.2f}%"
            else:
                _progress_str = "-----"

            if not self.check_any_event_is_set():
                if _speed_meter := _temp.get("speed_meter"):
                    _speed_meter_str = f"{naturalsize(_speed_meter)}ps"
                else:
                    _speed_meter_str = "--"

                if (_est_time_smooth := _temp.get("est_time_smooth")) and _est_time_smooth < 3600:
                    _eta_smooth_str = ":".join(
                        [
                            _item.split(".")[0]
                            for _item in f"{timedelta(seconds=_est_time_smooth)}".split(":")[1:]
                        ]
                    )
                else:
                    _eta_smooth_str = "--"

            else:
                _eta_smooth_str = "--"
                _speed_meter_str = "--"

                if self.vid_dl.reset_event.is_set():
                    try:
                        self.count_msg = AsyncHLSDownloader._QUEUE[str(self.vid_dl.index)].get_nowait()
                    except Exception:
                        pass

            return "".join(
                [
                    f"{_pre}: WK[{self.count:2d}/{self.n_workers:2d}] ",
                    f"FR[{self.n_dl_fragments:{self.format_frags}}/{self.n_total_fragments}]",
                    f"PR[{_progress_str}] DL[{_speed_meter_str}] ETA[{_eta_smooth_str}]",
                    f"\n{self.count_msg}",
                ]
            )

        if self.status == "init_manipulating":
            return f"{_pre}: Waiting for Ensambling \n"

        if self.status == "manipulating":
            if self.filename.exists():
                _size = self.filename.stat().st_size
            else:
                _size = 0
            _str = (
                f"[{naturalsize(_size)}/{naturalsize(self.filesize)}]" + f"({(_size/self.filesize)*100:.2f}%)"
                if self.filesize
                else f"[{naturalsize(_size)}]"
            )
            return f"{_pre}: Ensambling {_str}\n"
