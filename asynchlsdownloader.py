import asyncio
import binascii
import contextlib
import json
import logging
import math
import random
import time
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from queue import Queue
from shutil import rmtree
from threading import BoundedSemaphore, Lock

import aiofiles
import httpx
import m3u8
from aiofiles import os
from Cryptodome.Cipher import AES
from Cryptodome.Cipher._mode_cbc import CbcMode
from httpx._types import ProxiesTypes
from yt_dlp.extractor.nakedsword import NakedSwordBaseIE

from utils import (
    CONF_HLS_RESET_403_TIME,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    Coroutine,
    CountDowns,
    FrontEndGUI,
    InfoDL,
    LimitContextDecorator,
    List,
    MySyncAsyncEvent,
    Optional,
    ProgressTimer,
    ReExtractInfo,
    SmoothETA,
    SpeedometerMA,
    StatusError503,
    StatusStop,
    Token,
    Union,
    _for_print,
    async_lock,
    async_waitfortasks,
    await_for_any,
    cast,
    change_status_nakedsword,
    empty_queue,
    get_format_id,
    get_host,
    getter_basic_config_extr,
    int_or_none,
    limiter_0_1,
    limiter_non,
    load_config_extractors,
    my_dec_on_exception,
    myYTDL,
    naturalsize,
    print_delta_seconds,
    print_norm_time,
    put_sequence,
    send_http_request,
    smuggle_url,
    str_or_none,
    sync_to_async,
    traverse_obj,
    try_call,
    try_get,
    wait_for_either,
)

logger = logging.getLogger("async_HLS_DL")

kill_token = Token("kill")


def check_is_dl(filepath, hsize):
    is_dl = False
    size = 0
    dec_size = 0
    if filepath.exists():
        size = filepath.stat().st_size
        is_dl = True
        if not size or (hsize and not hsize - 100 <= size <= hsize + 100):
            is_dl = False
            filepath.unlink()
            dec_size = size
            size = 0
    return (is_dl, size, dec_size)


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


retry = my_dec_on_exception(
    AsyncHLSDLErrorFatal, max_tries=5, raise_on_giveup=True, interval=5)

on_exception = my_dec_on_exception(
    (TimeoutError, AsyncHLSDLError, ReExtractInfo),
    max_tries=5, raise_on_giveup=False, interval=5
)

on_503 = my_dec_on_exception(
    StatusError503, max_time=360, raise_on_giveup=False, interval=20)


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
    _PLNS = {}
    _CHUNK_SIZE = 16384  # 1024  # 102400
    _MAX_RETRIES = 5
    _MAX_RESETS = 10
    _CONFIG = load_config_extractors()
    _CLASSLOCK = Lock()
    _COUNTDOWNS = None
    _QUEUE = {}
    _INPUT = Queue()
    _INRESET_403 = InReset403()
    _qproxies = None

    def __init__(self, args: Namespace, ytdl: myYTDL, video_dict: dict, info_dl: InfoDL) -> None:
        try:
            self.background_tasks = set()
            self.tasks = []
            self.info_dict = video_dict
            self._vid_dl = info_dl
            self.args = args
            self._pos = None
            self.n_workers: int = self.args.parts
            self.count: int = 0
            self.ytdl = ytdl
            self.base_download_path = Path(self.info_dict["download_path"])
            self.download_path = Path(
                self.base_download_path, self.info_dict["format_id"])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.init_file = Path(
                self.base_download_path, f"init_file.{self.info_dict['format_id']}")
            _filename = Path(
                self.info_dict.get("_filename", self.info_dict.get("filename")))
            self.fragments_base_path = Path(
                self.download_path,
                f'{_filename.stem}.{self.info_dict["format_id"]}.{self.info_dict["ext"]}')
            self.filename = Path(
                self.base_download_path,
                f'{_filename.stem}.{self.info_dict["format_id"]}.ts')
            self.key_cache = {}
            self.n_reset = 0
            self._limit_reset = limiter_0_1.ratelimit("resetdl", delay=True)
            self.down_size = 0
            self.status = "init"
            self.error_message = ""
            self.upt = {}
            self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_hlsdl")
            self.sync_to_async = partial(
                sync_to_async, thread_sensitive=False, executor=self.ex_dl)
            self.special_extr: bool = False

            self.totalduration = cast(int, self.info_dict.get("duration", 0))
            self.filesize = cast(int, traverse_obj(self.info_dict, "filesize", "filesize_approx", default=0))  # type: ignore

            self.premsg = "".join(
                [
                    f'[{self.info_dict["id"]}]',
                    f'[{self.info_dict["title"]}]',
                    f'[{self.info_dict["format_id"]}]',
                ])

            self.count_msg = ""
            self._proxy = {}
            if _proxy := cast(str, self.args.proxy):
                self._proxy = {"http://": _proxy, "https://": _proxy}
            elif self.args.enproxy:
                with AsyncHLSDownloader._CLASSLOCK:
                    if not AsyncHLSDownloader._qproxies:
                        _seq = zip(
                            random.sample(
                                range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST),
                            random.sample(
                                range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST))

                        AsyncHLSDownloader._qproxies = put_sequence(Queue(), _seq)

                el1, el2 = cast(tuple, AsyncHLSDownloader._qproxies.get())
                _proxy_port = CONF_PROXIES_BASE_PORT + el1 * 100 + el2
                _proxy = f"http://127.0.0.1:{_proxy_port}"
                self._proxy = {"http://": _proxy, "https://": _proxy}
                try:
                    _gvd_pl = True
                    if "gvdblog" not in (_url := self.info_dict["original_url"]):
                        _gvd_pl = False
                        _url = self.info_dict["webpage_url"]
                    if info := self.multi_extract_info(_url, proxy=_proxy):
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
                                    ))
                            info = info["entries"][_pl_index - 1]
                        if new_info := get_format_id(info, self.info_dict["format_id"]):
                            _info = {key: new_info[key] for key in ("url", "manifest_url") if key in new_info}
                            self.info_dict.update(_info)
                except Exception as e:
                    logger.exception("[init info proxy] %s", str(e))

            if self.filename.exists() and self.filename.stat().st_size > 0:
                self.status = "done"

            self.smooth_eta = SmoothETA()
            self.progress_timer = ProgressTimer()
            self.speedometer = SpeedometerMA()
            self.frags_queue = asyncio.Queue()
            self._asynclock = asyncio.Lock()
            self._sem = asyncio.Semaphore()
            self.areset = self.sync_to_async(self.resetdl)

            self.timeout: httpx.Timeout = httpx.Timeout(15)
            self.limits: httpx.Limits = httpx.Limits(keepalive_expiry=30)

            self.config_httpx = lambda: {
                'proxies': cast(ProxiesTypes, self._proxy),
                'limits': self.limits,
                'follow_redirects': True,
                'timeout': self.timeout,
                'verify': False,
                'headers': self.info_dict["http_headers"]}

            self.init_client: httpx.Client

            self.auto_pasres = False
            self.fromplns = None
            self._extractor = cast(
                str, try_get(self.info_dict.get("extractor_key"), lambda x: x.lower()))
            self.m3u8_doc = ""
            self._host = get_host(self.info_dict["url"])
            self.frags_to_dl = []
            self.info_frag = []
            self.info_init_section = {}
            self.n_dl_fragments = 0

            def getter(name: Union[str, None]) -> tuple[int, Union[int, float], LimitContextDecorator]:
                if not name:
                    self.special_extr = False
                    return (self.n_workers, 0, limiter_non.ratelimit("transp", delay=True))
                if "nakedsword" in name:
                    self.auto_pasres = True
                    if all(
                        _ not in self.info_dict.get("playlist_title", "")
                        for _ in ("MostWatchedScenes", "Search")
                    ):
                        self.fromplns = str_or_none(self.info_dict.get("_id_movie"))
                value, key_text = getter_basic_config_extr(name, AsyncHLSDownloader._CONFIG) or (None, None)
                self.special_extr = False
                if value and key_text:
                    self.special_extr = True
                    if "nakedsword" in key_text:
                        key_text = "nakedsword"
                    return (
                        value["maxsplits"],
                        value["interval"],
                        value["ratelimit"].ratelimit(key_text, delay=True))
                return (self.n_workers, 0, limiter_non.ratelimit("transp", delay=True))

            _nworkers, self._interv, self._limit = getter(self._extractor)
            self.n_workers = max(self.n_workers, _nworkers) if _nworkers >= 16 else min(self.n_workers, _nworkers)

            self.init()

        except Exception as e:
            logger.exception(repr(e))

    @property
    def pos(self):
        return self._pos

    @pos.setter
    def pos(self, value):
        self._pos = value
        self.premsg = f"[{value}]{self.premsg}"
        if self.fromplns:
            self.upt_plns()

    def add_task(self, coro: Union[Coroutine, asyncio.Task], *,
                 name: Optional[str] = None) -> asyncio.Task:

        if not isinstance(coro, asyncio.Task):
            _task = asyncio.create_task(coro, name=name)
        else:
            _task = coro

        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    @on_503
    @on_exception
    def download_key(self, key_uri: str) -> Optional[bytes]:
        return try_get(
            send_http_request(key_uri, client=self.init_client, new_e=AsyncHLSDLError),
            lambda x: x.content if x else None)

    @on_503
    @on_exception
    def get_m3u8_doc(self) -> Optional[str]:
        if not (
            res := send_http_request(
                self.info_dict["url"],
                client=self.init_client,
                new_e=AsyncHLSDLError,
            )
        ):
            raise AsyncHLSDLError(
                f"{self.premsg}:[get_m3u8_doc] couldnt get init section")
        if isinstance(res, dict):
            raise AsyncHLSDLError(
                f"{self.premsg}:[get_m3u8_doc] {res['error']}")
        return res.content.decode("utf-8", "replace")

    def init(self):

        self.n_reset = 0
        self.frags_to_dl = []
        self.init_client = httpx.Client(**self.config_httpx())

        try:
            if not self.m3u8_doc:
                self.m3u8_doc = self.get_m3u8_doc()

            self.info_dict["fragments"] = self.get_info_fragments()

            self.n_total_fragments = len(self.info_dict["fragments"])
            self.format_frags = f"{(int(math.log(self.n_total_fragments, 10)) + 1)}d"

            if (_initfrag := self.info_dict["fragments"][0].init_section):
                if not self.info_init_section or not self.info_init_section['downloaded'] or not self.info_init_section['file'].exists():
                    _file_path = Path(str(self.fragments_base_path) + ".Frag0")
                    _url = _initfrag.absolute_uri
                    if "&hash=" in _url and _url.endswith("&="):
                        _url += "&="
                    _cipher = None
                    if hasattr(_initfrag, 'key') and _initfrag.key.method == "AES-128" and _initfrag.key.iv:
                        if (_key := self.download_key(cast(str, _initfrag.key.absolute_uri))):
                            _cipher = AES.new(_key, AES.MODE_CBC, binascii.unhexlify(_initfrag.key.iv[2:]))
                    self.info_init_section.update(
                        {"frag": 0, "url": _url, "file": _file_path, "cipher": _cipher, "downloaded": False})

                if not self.info_init_section['downloaded']:
                    self.get_init_section()

            init_data = {}
            if self.init_file.exists():
                with open(self.init_file, "rt", encoding='utf-8') as finit:
                    init_data = json.loads(finit.read())
                init_data = {int(k): v for k, v in init_data.items()}

            _mode_init = False
            if not self.info_frag:
                _mode_init = True

            byte_range = {}
            for i, fragment in enumerate(self.info_dict["fragments"]):
                if not fragment.uri and fragment.parts:
                    fragment.uri = fragment.parts[0].uri

                if fragment.byterange:
                    splitted_byte_range = fragment.byterange.split("@")
                    if (sub_range_start := (
                            try_get(splitted_byte_range, lambda x: int(x[1])) or byte_range.get("end"))):
                        byte_range = {
                            "start": sub_range_start,
                            "end": sub_range_start + int(splitted_byte_range[0])}
                else:
                    byte_range = {}

                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="

                # hlsdl first step
                if _mode_init:
                    hsize = init_data.get(i + 1)
                    _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i + 1}")
                    is_dl, size, _ = check_is_dl(_file_path, hsize)
                    cipher = None
                    if fragment.key:
                        cipher = traverse_obj(self.key_cache, (fragment.key.uri, "cipher"))
                    _frag = {
                        "frag": i + 1, "url": _url, "key": fragment.key, "cipher": cipher,
                        "file": _file_path, "byterange": byte_range, "downloaded": is_dl,
                        "headersize": hsize, "size": size, "n_retries": 0, "error": []}

                    if is_dl:
                        self.down_size += size
                        self.n_dl_fragments += 1
                    if not is_dl or not hsize:
                        self.frags_to_dl.append(i + 1)

                    self.info_frag.append(_frag)

                # reinit
                else:
                    if not self.info_frag[i]["headersize"]:
                        if _hsize := init_data.get(i + 1):
                            self.info_frag[i]["headersize"] = _hsize
                    if self.info_frag[i]["downloaded"]:
                        is_dl, _, dec_size = check_is_dl(self.info_frag[i]["file"], self.info_frag[i]["headersize"])
                        if not is_dl:
                            self.info_frag[i]["downloaded"] = False
                            self.n_dl_fragments -= 1
                            self.down_size -= dec_size
                            self._vid_dl.total_sizes["down_size"] -= dec_size
                    elif self.info_frag[i]["file"].exists():
                        self.info_frag[i]["file"].unlink()

                    if not self.info_frag[i]["downloaded"] or not self.info_frag[i]["headersize"]:
                        self.info_frag[i]["url"] = _url
                        self.info_frag[i]["n_retries"] = 0
                        self.info_frag[i]["byterange"] = byte_range
                        self.info_frag[i]["key"] = fragment.key
                        cipher = None
                        if fragment.key:
                            cipher = traverse_obj(self.key_cache, (fragment.key.uri, "cipher"))
                        self.info_frag[i]["cipher"] = cipher

                        self.frags_to_dl.append(i + 1)

            logger.debug(
                "".join([
                    f"{self.premsg}: Frags already DL: {len(self.fragsdl())}, ",
                    f"Frags not DL: {len(self.fragsnotdl())}, ",
                    f"Frags to request: {len(self.frags_to_dl)}"
                ])
            )

            if not self.totalduration:
                self.totalduration = self.calculate_duration()
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
                ]))

            if not self.frags_to_dl:
                self.status = "init_manipulating"
        except Exception as e:
            logger.exception(f"{self.premsg}[init] {str(e)}")
            self.status = "error"
            self.init_client.close()

    def upt_plns(self):
        with AsyncHLSDownloader._CLASSLOCK:
            if "ALL" not in AsyncHLSDownloader._PLNS:
                AsyncHLSDownloader._PLNS["ALL"] = {
                    "sem": BoundedSemaphore(),
                    "downloading": set(),
                    "in_reset": set(),
                    "reset": MySyncAsyncEvent(
                        name="fromplns[ALL]", initset=True),
                }
            if self.fromplns not in AsyncHLSDownloader._PLNS:
                AsyncHLSDownloader._PLNS[self.fromplns] = {
                    "downloaders": {self.info_dict["_index_scene"]: self},
                    "downloading": set(),
                    "in_reset": set(),
                    "reset": MySyncAsyncEvent(
                        name=f"fromplns[{self.fromplns}]", initset=True),
                    "sem": BoundedSemaphore(),
                }
            else:
                AsyncHLSDownloader._PLNS[self.fromplns]["downloaders"].update(
                    {self.info_dict["_index_scene"]: self})

        _downloaders = AsyncHLSDownloader._PLNS[self.fromplns]["downloaders"]
        logger.debug(
            "".join([
                f"{self.premsg}: ",
                f"added new dl to plns [{self.fromplns}], ",
                f"count [{len(_downloaders)}] ",
                f"members[{list(_downloaders.keys())}]"
            ])
        )

    def calculate_duration(self) -> int:
        return sum(fragment.duration for fragment in self.info_dict["fragments"])

    def calculate_filesize(self) -> Optional[int]:
        return (
            int(self.totalduration * 1000 * _bitrate / 8)
            if (
                _bitrate := cast(float, traverse_obj(self.info_dict, "tbr", "abr"))
            )
            else None
        )

    def get_info_fragments(self) -> Union[list, m3u8.SegmentList]:

        try:
            m3u8_obj = m3u8.loads(self.m3u8_doc, self.info_dict["url"])

            if not m3u8_obj or not m3u8_obj.segments:
                raise AsyncHLSDLError("couldnt get m3u8 file")

            if m3u8_obj.keys:
                for _key in m3u8_obj.keys:
                    if _key and _key.method == "AES-128" and _key.iv:
                        if not (_cipher := traverse_obj(self.key_cache, (_key.uri, "cipher"))):
                            if (_valkey := self.download_key(cast(str, _key.absolute_uri))):
                                _cipher = AES.new(_valkey, AES.MODE_CBC, binascii.unhexlify(_key.iv[2:]))
                                self.key_cache[_key.uri] = {"key": _valkey, "cipher": _cipher}

            if _start_time := self.info_dict.get("_start_time"):
                logger.info(f"{self.premsg}[get_info_fragments] start time {_start_time}")
                _duration = try_get(
                    getattr(m3u8_obj, "target_duration", None), lambda x: float(x) if x else None)
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

    def check_any_event_is_set(self, incpause=True) -> List[Optional[str]]:
        _events = [self._vid_dl.reset_event, self._vid_dl.stop_event]
        if incpause:
            _events.append(self._vid_dl.pause_event)
        return [_ev.name for _ev in _events if _ev.is_set()]

    def check_stop(self):
        if self._vid_dl.stop_event.is_set():
            raise StatusStop("stop event")

    @on_503
    @on_exception
    def get_init_section(self):

        _res = [self.info_init_section.get(_key) for _key in ("url", "file", "cipher")]
        uri: str = cast(str, _res[0])
        file: Path = cast(Path, _res[1])
        if not uri or not file:
            raise AsyncHLSDLError(f"{self.premsg}:[get_init_section] not url or file")
        cipher: Optional[CbcMode] = cast(Optional[CbcMode], _res[2])
        try:
            if (res := send_http_request(uri, client=self.init_client, new_e=AsyncHLSDLError)):
                if isinstance(res, dict):
                    raise AsyncHLSDLError(
                        f"{self.premsg}:[get_init_section] {res['error']}")

                _data = cipher.decrypt(res.content) if cipher else res.content
                with open(file, "wb") as fsect:
                    fsect.write(_data)
                self.info_init_section["downloaded"] = True
            else:
                raise AsyncHLSDLError(
                    f"{self.premsg}:[get_init_section] couldnt get init section")
        except Exception as e:
            logger.exception(f"{self.premsg}:[get_init_section] {repr(e)}")
            raise

    def multi_extract_info(
            self, url: str, proxy: Optional[str] = None, msg: Optional[str] = None) -> dict:

        premsg = "[multi_extract_info]"
        if msg:
            premsg += msg

        try:
            self.check_stop()

            if not self.args.proxy and proxy:
                with myYTDL(params=self.ytdl.params, proxy=proxy, silent=True) as proxy_ytdl:
                    _info_video = proxy_ytdl.sanitize_info(
                        proxy_ytdl.extract_info(url, download=False))
            else:
                # if proxy was included in args main program, ytdl will have this proxy in its params
                _info_video = self.ytdl.sanitize_info(
                    self.ytdl.extract_info(url, download=False))

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

    def prep_reset(self, info_reset: dict):

        self.info_dict.update({
            "url": info_reset["url"], "formats": info_reset["formats"],
            "http_headers": info_reset["http_headers"]})
        self._host = get_host(self.info_dict["url"])

        try:
            self.init_client.close()
        except Exception as e:
            logger.error(
                f"{self.premsg}:RESET[{self.n_reset}]:prep_reset error {repr(e)}")

        self.init_client = httpx.Client(**self.config_httpx())

        self.info_dict["fragments"] = self.get_info_fragments()

        self.frags_to_dl = []
        init_data = {}
        if self.init_file.exists():
            with open(self.init_file, "rt", encoding='utf-8') as finit:
                init_data = json.loads(finit.read())
            init_data = {int(_key): _value for _key, _value in init_data.items()}

        byte_range = {}
        for i, fragment in enumerate(self.info_dict["fragments"]):
            try:
                if not fragment.uri and fragment.parts:
                    fragment.uri = fragment.parts[0].uri

                if fragment.byterange:
                    splitted_byte_range = fragment.byterange.split("@")
                    if (sub_range_start := (
                            try_get(splitted_byte_range, lambda x: int(x[1])) or byte_range.get("end"))):
                        byte_range = {
                            "start": sub_range_start,
                            "end": sub_range_start + int(splitted_byte_range[0])}
                else:
                    byte_range = {}

                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="

                if not self.info_frag[i]["headersize"]:
                    if _hsize := init_data.get(i + 1):
                        self.info_frag[i]["headersize"] = _hsize
                if self.info_frag[i]["downloaded"]:
                    is_dl, _, dec_size = check_is_dl(self.info_frag[i]["file"], self.info_frag[i]["headersize"])
                    if not is_dl:
                        self.info_frag[i]["downloaded"] = False
                        self.n_dl_fragments -= 1
                        self.down_size -= dec_size
                        self._vid_dl.total_sizes["down_size"] -= dec_size
                elif self.info_frag[i]["file"].exists():
                    self.info_frag[i]["file"].unlink()

                if not self.info_frag[i]["downloaded"] or not self.info_frag[i]["headersize"]:
                    self.info_frag[i]["url"] = _url
                    self.info_frag[i]["n_retries"] = 0
                    self.info_frag[i]["byterange"] = byte_range
                    self.info_frag[i]["key"] = fragment.key
                    cipher = None
                    if fragment.key:
                        cipher = traverse_obj(self.key_cache, (fragment.key.uri, "cipher"))
                    self.info_frag[i]["cipher"] = cipher

                    self.frags_to_dl.append(i + 1)

            except Exception as e:
                logger.debug(
                    "".join([
                        f"{self.premsg}:RESET[{self.n_reset}]:prep_reset:error {str(e)}",
                        f"with i = [{i}] \n\ninfo_dict['fragments'] ",
                        f"{len(self.info_dict['fragments'])}\n\n",
                        f"{[str(frag) for frag in self.info_dict['fragments']]}\n\n",
                        f"info_frag {len(self.info_frag)}"
                    ]))

                raise

        if not self.frags_to_dl:
            self.status = "init_manipulating"
        else:
            logger.debug(
                "".join([
                    f"{self.premsg}:RESET[{self.n_reset}]:prep_reset:OK ",
                    f"{self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}"
                ]))

    @retry
    def get_reset_info(self, _reset_url: str, plns=True, first=False) -> dict:
        _base = f"{self.premsg}[get_reset_inf]"
        _proxy = self._proxy.get('http://')
        _print_proxy = lambda: (
            f":proxy[{_proxy.split(':')[-1]}]"
            if _proxy else "")
        _print_plns = lambda: (
            f":PLNS[{self.fromplns}]"
            if self.fromplns else "")
        _pre = lambda: "".join([
            f"{_base}:RESET[{self.n_reset}]:first[{first}]",
            f"{_print_plns()}{_print_proxy()}"])

        logger.debug(_pre())

        _info = None
        info_reset = None

        try:
            if plns and self.fromplns:
                if first:
                    _info = self.multi_extract_info(_reset_url, proxy=_proxy, msg=_pre())
                    self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), _info)

                else:
                    _info = self.ytdl.cache.load("nakedswordmovie", str(self.fromplns))
                    if not _info:
                        _info = self.multi_extract_info(_reset_url, proxy=_proxy, msg=_pre())
                        self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), _info)

                if _info:
                    info_reset = try_get(
                        traverse_obj(_info, ("entries", int(self.info_dict["_index_scene"]) - 1)),
                        lambda x: get_format_id(x, self.info_dict["format_id"]) if x else None)

            elif _info := self.multi_extract_info(
                _reset_url, proxy=_proxy, msg=_pre()
            ):
                if _info.get("entries") and (_pl_index := self.info_dict["playlist_index"]):
                    _info = _info["entries"][_pl_index - 1]
                info_reset = get_format_id(_info, self.info_dict["format_id"])

            if not info_reset:
                raise AsyncHLSDLErrorFatal(f"{_pre()} fails no descriptor")

            logger.debug(f"{_pre()} format extracted info video ok\n{_for_print(info_reset)}")

            self.prep_reset(info_reset)
            return {"res": "ok"}

        except StatusStop as e:
            logger.debug(f"{_pre()} check stop {repr(e)}")
            raise
        except AsyncHLSDLErrorFatal as e:
            logger.debug(f"{_pre()} {repr(e)}")
            raise
        except Exception as e:
            logger.error(f"{_pre()} fails when extracting reset info {repr(e)}")
            raise

    def resetdl(self, cause: Optional[str] = None):
        _base = f"{self.premsg}[resetdl]"
        _proxy = self._proxy.get('http://')
        _print_proxy = lambda: (
            f":proxy[{_proxy.split(':')[-1]}]"
            if _proxy else "")
        _print_plns = lambda: (
            f":PLNS[{self.fromplns}]"
            if self.fromplns else "")
        _pre = lambda: "".join([
            f"{_base}:RESET[{self.n_reset}]",
            f"{_print_plns()}{_print_proxy()}"])

        logger.debug(_pre())

        _pasres_cont = False

        try:
            if self.fromplns and str(cause) == "403":
                AsyncHLSDownloader._INRESET_403.add(self.info_dict["id"])
                with AsyncHLSDownloader._CLASSLOCK:
                    _pasres_cont = FrontEndGUI.pasres_break()
                    if not AsyncHLSDownloader._COUNTDOWNS:
                        AsyncHLSDownloader._COUNTDOWNS = CountDowns(
                            AsyncHLSDownloader, logger=logger)
                        logger.debug(
                            f"{self.premsg}:RESET[{self.n_reset}] new COUNTDOWN")

                AsyncHLSDownloader._COUNTDOWNS.add(
                    CONF_HLS_RESET_403_TIME,
                    index=str(self.pos),
                    event=self._vid_dl.stop_event,
                    msg=self.premsg)

                self.check_stop()

                logger.info(f"{_pre()} fin wait in reset cause 403")

            # if self.fromplns and str(cause) == "403":
            if self.fromplns:
                with (_sem := AsyncHLSDownloader._PLNS["ALL"]["sem"]):
                    logger.debug(f"{_pre()} in sem")

                    _first_all = _sem._initial_value == 1
                    with (_sem2 := AsyncHLSDownloader._PLNS[self.fromplns]["sem"]):
                        logger.debug(f"{_pre()} in sem2")

                        _first = _sem2._initial_value == 1
                        if _first_all and str(cause) == "403":
                            NakedSwordBaseIE.API_REFRESH(msg="[resetdl]")
                            NakedSwordBaseIE.API_LOGOUT(msg="[resetdl]")
                            time.sleep(5)

                        _listreset = [
                            int(index)
                            for index in list(
                                AsyncHLSDownloader._PLNS[self.fromplns]["in_reset"])]
                        _aux = {
                            "indexdl": self.pos,
                            "args": {self.fromplns: {"listreset": _listreset}},
                        }

                        _plns_url = smuggle_url(self.info_dict["original_url"], _aux)

                        if (_resinfo := self.get_reset_info(
                                _plns_url, plns=True, first=_first)) and "res" in _resinfo:

                            if _first:
                                _sem2._initial_value = 100
                                _sem2.release(50)
                            if _first_all:
                                _sem._initial_value = 100
                                _sem.release(50)

                            time.sleep(1)

            else:
                if self.special_extr:
                    _webpage_url = smuggle_url(
                        self.info_dict["webpage_url"], {"indexdl": self.pos})
                else:
                    _webpage_url = self.info_dict["webpage_url"]

                self.get_reset_info(_webpage_url, plns=False)

        except StatusStop as e:
            logger.debug(f"{_pre()} stop_event")
            raise e
        except Exception as e:
            logger.exception(
                f"{_pre()} stop_event:[{self._vid_dl.stop_event}] " +
                f"outer Exception {repr(e)}")
            raise
        finally:
            if self.fromplns and str(cause) == "403":
                try_call(lambda: AsyncHLSDownloader._INRESET_403.remove(self.info_dict["id"]))

            # if self.fromplns and str(cause) == "403":
            if self.fromplns:
                logger.debug(
                    f"{_pre()} stop_event[{self._vid_dl.stop_event}] FINALLY")

                with AsyncHLSDownloader._CLASSLOCK:
                    _inreset = AsyncHLSDownloader._PLNS[self.fromplns]["in_reset"]
                    try_call(lambda: _inreset.remove(self.info_dict["_index_scene"]))
                    if not _inreset:
                        logger.debug(f"{_pre()} end of resets fromplns [{self.fromplns}]")

                        AsyncHLSDownloader._PLNS[self.fromplns]["reset"].set()
                        AsyncHLSDownloader._PLNS[self.fromplns]["sem"] = BoundedSemaphore()

                if _inreset:

                    if self._vid_dl.stop_event.is_set():
                        return
                    logger.info(
                        f"{_pre()} waits for rest scenes in [{self.fromplns}] to start DL "
                        + f"[{_inreset}]")

                    wait_for_either(
                        [AsyncHLSDownloader._PLNS[self.fromplns]["reset"], self._vid_dl.stop_event], timeout=300)

                with AsyncHLSDownloader._CLASSLOCK:

                    try_call(lambda: AsyncHLSDownloader._PLNS["ALL"]["in_reset"].remove(self.fromplns))

                    if not (_inreset := AsyncHLSDownloader._PLNS["ALL"]["in_reset"]):
                        logger.debug(f"{_pre()} end for all plns ")

                        AsyncHLSDownloader._PLNS["ALL"]["reset"].set()
                        AsyncHLSDownloader._PLNS["ALL"]["sem"] = BoundedSemaphore()
                        self.n_reset += 1
                        if _pasres_cont:
                            FrontEndGUI.pasres_continue()
                        logger.debug(f"{_pre()}  exit reset")
                        return

                if _inreset:
                    if self._vid_dl.stop_event.is_set():
                        return

                    self.n_reset += 1
                    if _pasres_cont:
                        FrontEndGUI.pasres_continue()
                    logger.debug(f"{_pre()} exit reset")
                    return
            else:
                self.n_reset += 1
                logger.debug(f"{_pre()} exit reset")

    async def upt_status(self):
        _timer = ProgressTimer()
        while not self._vid_dl.end_tasks.is_set():
            if _timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2) and (self.down_size and not self.check_any_event_is_set()):
                async with self._asynclock:
                    _down_size = self.down_size
                    _n_dl_frag = self.n_dl_fragments
                    _filesize = self.filesize
                _speed_meter = self.speedometer(_down_size)
                _est_time = None
                _est_time_smooth = None
                if _speed_meter and _filesize:
                    _est_time = (_filesize - _down_size) / _speed_meter
                    _est_time_smooth = self.smooth_eta(_est_time)
                self.upt.update({
                    "n_dl_frag": _n_dl_frag,
                    "speed_meter": _speed_meter,
                    "down_size": _down_size,
                    "filesize": _filesize,
                    "est_time": _est_time,
                    "est_time_smooth": _est_time_smooth})

            await asyncio.sleep(0)

    async def event_handle(self, msg: str) -> dict:
        _res = {}
        if self._vid_dl.pause_event.is_set():
            logger.debug(f"{msg}[handle] pause detected")
            _res["pause"] = True
            async with self._sem:
                logger.debug(f"{msg}[handle] through sem, waiting for resume")
                if self._vid_dl.pause_event.is_set():
                    _res |= await await_for_any([
                        self._vid_dl.resume_event, self._vid_dl.reset_event,
                        self._vid_dl.stop_event], timeout=300)
                    logger.debug(f"{msg}[handle] after wait pause: {_res}")
                    if traverse_obj(_res, "event") == "resume":
                        _res.pop("event")
                    self._vid_dl.resume_event.clear()
                    self._vid_dl.pause_event.clear()
                    await asyncio.sleep(0)
                    return _res
                logger.debug(f"{msg}[handle] after wait pause: {_res}")
        if _event := self.check_any_event_is_set(incpause=False):
            _res["event"] = _event
        return _res

    async def _reset(self, cause: Union[str, None] = None, wait=True):
        if self.status != "downloading":
            return
        if not self._vid_dl.reset_event.is_set():
            self._vid_dl.reset_event.set(cause)
            if self.tasks:
                if _wait_tasks := [_task for _task in self.tasks if _task is not asyncio.current_task()]:
                    list(map(lambda task: task.cancel(), _wait_tasks))
                    if wait:
                        await asyncio.wait(_wait_tasks)
                    return _wait_tasks
        else:
            self._vid_dl.reset_event.set(cause)

    @classmethod
    async def reset_plns(cls, cause: Optional[str] = "403", plns: Optional[str] = None, wait=True):

        AsyncHLSDownloader._PLNS["ALL"]["reset"].clear()
        plid_total = [plns] if plns else AsyncHLSDownloader._PLNS["ALL"]["downloading"]
        _wait_all_tasks = []

        for plid in plid_total:
            dict_dl = traverse_obj(AsyncHLSDownloader._PLNS, (plid, "downloaders"))
            list_dl = traverse_obj(AsyncHLSDownloader._PLNS, (plid, "downloading"))
            list_reset = traverse_obj(AsyncHLSDownloader._PLNS, (plid, "in_reset"))

            if list_dl and dict_dl:
                AsyncHLSDownloader._PLNS["ALL"]["in_reset"].add(plid)
                AsyncHLSDownloader._PLNS[plid]["reset"].clear()
                plns = [dl for key, dl in dict_dl.items() if key in list_dl]  # type: ignore
                for dl, key in zip(plns, list_dl):  # type: ignore
                    if _tasks := await dl._reset(cause, wait=False):
                        _wait_all_tasks.extend(_tasks)
                    list_reset.add(key)  # type: ignore
                    await asyncio.sleep(0)

            await asyncio.sleep(0)

        if wait and _wait_all_tasks:
            await asyncio.wait(_wait_all_tasks)

        return _wait_all_tasks

    async def back_from_reset_plns(self, premsg, plns=None):
        _tasks_all = []
        plid_total = [plns] if plns else AsyncHLSDownloader._PLNS["ALL"]["in_reset"]
        for plid in plid_total:
            dict_dl = traverse_obj(AsyncHLSDownloader._PLNS, (plid, "downloaders"))
            list_reset = traverse_obj(AsyncHLSDownloader._PLNS, (plid, "in_reset"))
            if list_reset and dict_dl:
                plns = [dl for key, dl in dict_dl.items() if key in list_reset]  # type: ignore
                _tasks_all.extend([
                    self.add_task(
                        dl._vid_dl.end_tasks.async_wait(timeout=300),
                        name=f'await_end_tasks_{dl.premsg}') for dl in plns])

        logger.debug(f"{premsg} endtasks {_tasks_all}")

        if _tasks_all:
            await async_waitfortasks(
                _tasks_all, events=self._vid_dl.stop_event,
                background_tasks=self.background_tasks)

    async def _decrypt(self, data: bytes, cipher: Optional[CbcMode]) -> bytes:
        return await self.sync_to_async(cipher.decrypt)(data) if cipher else data

    async def _update_counters(self, _bytes_dl: int, _old: int) -> int:
        if (_iter_bytes := _bytes_dl - _old) > 0:
            async with self._asynclock:
                self.down_size += _iter_bytes
                self._vid_dl.total_sizes["down_size"] += _iter_bytes
                if self.filesize and (_dif := self.down_size - self.filesize) > 0:
                    self.filesize += _dif
                    self._vid_dl.total_sizes["filesize"] += _dif
        return _bytes_dl

    async def _clean_frag(self, _info_frag: dict, _exc: BaseException) -> None:
        _info_frag["error"].append(repr(_exc))
        _info_frag["downloaded"] = False
        _fpath = Path(_info_frag["file"])
        if await os.path.exists(_fpath):
            _sizefile = (await os.stat(_fpath)).st_size
            await os.remove(_fpath)
            async with self._asynclock:
                self.down_size -= _sizefile
                self._vid_dl.total_sizes["down_size"] -= _sizefile

    @on_503
    async def _download_frag(self, index: int, msg: str, client: httpx.AsyncClient) -> bool:
        _premsg = f"{msg}:[frag-{index}]:[dl]"
        info_frag = self.info_frag[index - 1]
        url = info_frag["url"]
        filename = Path(info_frag["file"])
        cipher = info_frag.get("cipher")
        headers = {}
        if byte_range := info_frag.get("byterange"):
            headers["range"] = f"bytes={byte_range['start']}-{byte_range['end'] - 1}"

        while info_frag["n_retries"] < self._MAX_RETRIES:
            try:

                if self._interv:
                    async with self._limit:
                        await asyncio.sleep(self._interv)

                if _ev := self.check_any_event_is_set(incpause=False):
                    raise AsyncHLSDLErrorFatal(f"{_premsg} {_ev}")

                async with (aiofiles.open(filename, mode="ab") as fileobj, client.stream("GET", url, headers=headers) as response):

                    if response.status_code == 403:

                        if self.fromplns:
                            await AsyncHLSDownloader.reset_plns(cause="403", plns=self.fromplns)
                        else:
                            await self._reset(cause="403")

                        raise AsyncHLSDLErrorFatal(f"{_premsg}:Frag:{index} resp code:{str(response)}")

                    if response.status_code == 503:
                        info_frag["n_retries"] = 0
                        raise StatusError503(f"{_premsg}")

                    if response.status_code >= 400:
                        raise AsyncHLSDLError(f"{_premsg}:Frag:{index} resp code:{str(response)}")

                    if not (_hsize := info_frag["headersize"]):
                        _hsize = info_frag["headersize"] = int_or_none(
                            response.headers.get("content-length"))

                    if info_frag["downloaded"]:
                        _size = info_frag["size"] = (
                            await os.stat(filename)).st_size
                        if (_hsize and (_hsize - 100 <= _size <= _hsize + 100)) or not _hsize:
                            break

                        await fileobj.truncate(0)
                        info_frag["downloaded"] = False
                        info_frag["size"] = None
                        async with self._asynclock:
                            self.n_dl_fragments -= 1
                            self.down_size -= _size
                            self._vid_dl.total_sizes["down_size"] -= _size

                    num_bytes_downloaded = response.num_bytes_downloaded
                    _timer = ProgressTimer()
                    _timer2 = ProgressTimer()
                    _buffer = b""
                    _tasks_chunks = []

                    async for chunk in response.aiter_bytes(chunk_size=self._CHUNK_SIZE):
                        if _data := await self._decrypt(chunk, cipher):
                            _buffer += _data

                        if _timer.has_elapsed(CONF_INTERVAL_GUI / 2):
                            num_bytes_downloaded = await self._update_counters(
                                response.num_bytes_downloaded, num_bytes_downloaded)
                        if _timer2.has_elapsed(5 * CONF_INTERVAL_GUI):
                            if _tasks_chunks:
                                await asyncio.wait(_tasks_chunks[-1:])
                            if _buffer:
                                _tasks_chunks.append(
                                    self.add_task(
                                        fileobj.write(_buffer),
                                        name=f"{_premsg}[write_chunks][{len(_tasks_chunks)}]"))

                                _buffer = b""

                        _check = await self.event_handle(_premsg)
                        if _ev := traverse_obj(_check, "event"):
                            if _tasks_chunks:
                                _tasks_chunks[-1].cancel()
                            raise AsyncHLSDLErrorFatal(_ev)

                        if traverse_obj(_check, "pause"):
                            _timer.reset()
                            _timer2.reset()

                        await asyncio.sleep(0)

                    await self._update_counters(
                        response.num_bytes_downloaded, num_bytes_downloaded)
                    if _tasks_chunks:
                        await asyncio.wait(_tasks_chunks[-1:])
                    if _buffer:
                        _tasks_chunks.append(
                            self.add_task(
                                fileobj.write(_buffer),
                                name=f"{_premsg}[write_chunks][{len(_tasks_chunks)}]"))

                        _buffer = b""
                        await asyncio.wait(_tasks_chunks[-1:])

                _nsize = (await os.stat(filename)).st_size
                _nhsize = info_frag["headersize"]

                if (_nhsize and _nhsize - 100 <= _nsize <= _nhsize + 100) or not _nhsize:
                    info_frag["downloaded"] = True
                    info_frag["size"] = _nsize
                    async with self._asynclock:
                        self.n_dl_fragments += 1
                    return True

                logger.warning(
                    "".join([
                        f"{_premsg}: end of streaming. Fragment not completed\n",
                        f"{info_frag}"]))

                raise AsyncHLSDLError(f"fragment not completed frag[{index}]")

            except StatusError503 as e:
                logger.error(f"{_premsg}: Error: {repr(e)}")
                await self._clean_frag(info_frag, e)
                raise e
            except (asyncio.CancelledError, RuntimeError, AsyncHLSDLErrorFatal, httpx.ReadTimeout) as e:
                logger.debug(f"{_premsg}: Error: {repr(e)}")
                await self._clean_frag(info_frag, e)
                raise e
            except Exception as e:
                logger.error(f"{_premsg}: Error: {repr(e)}")
                await self._clean_frag(info_frag, e)
                info_frag["n_retries"] += 1
                if info_frag["n_retries"] >= self._MAX_RETRIES:
                    info_frag["error"].append("MaxLimitRetries")
                    logger.warning(f"{_premsg}: MaxLimitRetries:skip")
                    info_frag["skipped"] = True
        return False

    async def fetch(self, nco: int):
        premsg = f"{self.premsg}:[worker-{nco}]:[fetch]"
        logger.debug(f"{premsg} init worker")

        client = httpx.AsyncClient(**self.config_httpx())

        try:
            while True:
                try:
                    qindex = self.frags_queue.get_nowait()
                    if qindex == kill_token:
                        logger.debug(f"{premsg} killtoken {qindex}")
                        return

                    await self._download_frag(qindex, premsg, client)
                    await asyncio.sleep(0)

                except asyncio.QueueEmpty:
                    await asyncio.sleep(0)
                    continue
                except httpx.ReadTimeout:
                    await asyncio.sleep(0)
                    continue

        except Exception as e:
            logger.debug(f"{premsg} outer exception {repr(e)}")

        finally:
            async with self._asynclock:
                self.count -= 1
            await client.aclose()
            logger.debug(f"{premsg} bye worker")

    async def fetch_async(self):
        n_frags_dl = 0
        _premsg = f"{self.premsg}[fetch_async]"
        AsyncHLSDownloader._QUEUE[str(self.pos)] = Queue()

        if self.fromplns:
            if (_event := cast(MySyncAsyncEvent, traverse_obj(
                    AsyncHLSDownloader._PLNS, (self.fromplns, "reset")))):
                if not _event.is_set():
                    logger.info(f"{_premsg} waiting at start")
                    _res = await await_for_any(
                        [_event, self._vid_dl.stop_event], timeout=300)
                    if traverse_obj(_res, "event") == "stop":
                        self.status = "stop"
                        return
            async with async_lock(AsyncHLSDownloader._CLASSLOCK):
                AsyncHLSDownloader._PLNS[self.fromplns]["downloading"].add(self.info_dict["_index_scene"])
                AsyncHLSDownloader._PLNS["ALL"]["downloading"].add(self.fromplns)

        try:
            while True:
                logger.debug(f"{self.premsg} TASKS INIT")

                empty_queue(self.frags_queue)
                for frag in self.frags_to_dl:
                    self.frags_queue.put_nowait(frag)
                for _ in range(self.n_workers):
                    self.frags_queue.put_nowait(kill_token)
                self.count = self.n_workers
                self._vid_dl.clear()
                self.speedometer.reset(initial_bytes=self.down_size)
                self.progress_timer.reset()
                self.smooth_eta.reset()
                self.status = "downloading"
                self.count_msg = ""

                upt_task = [
                    self.add_task(
                        self.upt_status(),
                        name=f"{self.premsg}[upt_task]")]

                self.tasks = [
                    self.add_task(self.fetch(i), name=f"{self.premsg}[{i}]")
                    for i in range(self.n_workers)]

                try:

                    done, pending = await asyncio.wait(self.tasks)

                    logger.debug(
                        f"{_premsg} done[{len(done)}] pending[{len(pending)}]")

                    self._vid_dl.end_tasks.set()

                    await self.dump_init_file()

                    _nfragsdl = len(self.fragsdl())
                    inc_frags_dl = _nfragsdl - n_frags_dl
                    n_frags_dl = _nfragsdl

                    # succeed, all fragments dl
                    if n_frags_dl == len(self.info_dict["fragments"]):
                        return

                    # manually stopped
                    if self._vid_dl.stop_event.is_set():
                        self.status = "stop"
                        return

                    # reset event was set
                    if _cause := self._vid_dl.reset_event.is_set():
                        if self.n_reset < self._MAX_RESETS:
                            if _cause in ("403", "hard"):
                                if self.fromplns:
                                    await self.back_from_reset_plns(
                                        self.premsg, plns=self.fromplns)
                                    if self._vid_dl.stop_event.is_set():
                                        return
                                _cause = self._vid_dl.reset_event.is_set()
                                logger.debug(f"{_premsg}:RESET[{self.n_reset}]:CAUSE[{_cause}]")

                            elif _cause == "manual":  # change num workers
                                logger.debug(f"{_premsg}:RESET[{self.n_reset}]:CAUSE[{_cause}]")
                                continue

                            try:
                                async with self._limit_reset:
                                    await self.areset(_cause)
                                if self._vid_dl.stop_event.is_set():
                                    return

                                await asyncio.sleep(0)
                                continue

                            except StatusStop:
                                return

                            except Exception as e:
                                logger.exception(
                                    "".join([
                                        f"{_premsg}:RESET[{self.n_reset}]:",
                                        f"ERROR reset couldnt progress:[{str(e)}]"
                                    ]))
                                self.status = "error"
                                await self.clean_when_error()
                                raise AsyncHLSDLErrorFatal(
                                    f"{_premsg} ERROR reset couldnt progress") from e

                        else:
                            logger.warning(
                                f"{_premsg}:RESET[{self.n_reset}]:ERROR:Max_number_of_resets")
                            self.status = "error"
                            await self.clean_when_error()
                            await asyncio.sleep(0)
                            raise AsyncHLSDLErrorFatal(f"{_premsg} ERROR max resets")

                    elif inc_frags_dl > 0:
                        try:
                            logger.info(
                                "".join([
                                    f"{_premsg}:RESET:new cycle ",
                                    f"with pending frags [{len(self.fragsnotdl())}]"
                                ]))
                            async with self._limit_reset:
                                await self.areset("hard")
                            if self._vid_dl.stop_event.is_set():
                                return
                            logger.debug(
                                "".join([
                                    f"{_premsg}:RESET:OK new cycle ",
                                    f"with pending frags [{len(self.fragsnotdl())}]"
                                ]))
                            self.n_reset -= 1
                            continue

                        except Exception as e:
                            logger.exception(
                                "".join([
                                    f"{_premsg}:RESET",
                                    f"[{self.n_reset}]:ERROR reset ",
                                    f"couldnt progress:[{repr(e)}]"
                                ]))
                            self.status = "error"
                            await self.clean_when_error()
                            await asyncio.sleep(0)
                            raise AsyncHLSDLErrorFatal(
                                f"{_premsg} ERROR reset couldnt progress") from e

                    else:
                        logger.debug(
                            "".join([
                                f"{_premsg} [{n_frags_dl} <-> ",
                                f"{inc_frags_dl}] no improvement, ",
                                "lets raise an error"
                            ]))
                        self.status = "error"
                        raise AsyncHLSDLErrorFatal(
                            f"{_premsg} no changes in number of dl frags in one cycle")

                except AsyncHLSDLErrorFatal:
                    raise
                except BaseException as e:
                    logger.exception(f"{_premsg} error {repr(e)}")
                    if isinstance(e, KeyboardInterrupt):
                        raise
                finally:
                    await asyncio.wait(upt_task)
                    await asyncio.sleep(0)

        except Exception as e:
            logger.error(f"{_premsg} error {repr(e)}")
            self.status = "error"
        finally:
            await self.dump_init_file()
            self.init_client.close()
            if self.fromplns:
                async with async_lock(AsyncHLSDownloader._CLASSLOCK):
                    try_call(lambda: AsyncHLSDownloader._PLNS[self.fromplns]["downloading"].remove(
                        self.info_dict["_index_scene"]))

                    if not AsyncHLSDownloader._PLNS[self.fromplns]["downloading"]:
                        try_call(lambda: AsyncHLSDownloader._PLNS["ALL"]["downloading"].remove(self.fromplns))

            if self._vid_dl.stop_event.is_set():
                self.status = "stop"
            elif self.status not in ("error"):
                logger.debug(f"{_premsg} Frags DL completed")
                self.status = "init_manipulating"

    async def dump_init_file(self):
        init_data = {
            el["frag"]: el["headersize"]
            for el in self.info_frag if el["headersize"]
        }
        async with aiofiles.open(self.init_file, mode="w") as finit:
            await finit.write(json.dumps(init_data))

    async def clean_when_error(self):
        for frag in self.info_frag:
            if frag["downloaded"] is False and await os.path.exists(frag["file"]):
                await os.remove(frag["file"])

    async def ensamble_file(self):
        self.status = "manipulating"
        _skipped = 0
        try:
            async with aiofiles.open(self.filename, mode="wb") as dest:
                for frag in self.info_frag:
                    if frag.get("skipped", False):
                        _skipped += 1
                        continue
                    if not frag["size"] and await os.path.exists(frag["file"]):
                        frag["size"] = (await os.stat(frag["file"])).st_size

                    if (
                        not frag["size"]
                        or (
                            not frag["headersize"]
                            or not frag["headersize"] - 100
                            <= frag["size"]
                            <= frag["headersize"] + 100
                        )
                        and frag["headersize"]
                    ):
                        raise AsyncHLSDLError(f"{self.premsg}: error when ensambling: {frag}")

                    async with aiofiles.open(frag["file"], mode="rb") as source:
                        await dest.write(await source.read())
        except Exception as e:
            if await os.path.exists(self.filename):
                await os.remove(self.filename)
            logger.exception(f"{self.premsg} Error {str(e)}")
            self.status = "error"
            await self.clean_when_error()
            raise
        finally:
            if await os.path.exists(self.filename):
                armtree = self.sync_to_async(partial(rmtree, ignore_errors=True))
                await armtree(str(self.download_path))
                self.status = "done"
                logger.debug(f"{self.premsg}: [ensamble_file] file ensambled")
                if _skipped:
                    logger.warning(f"{self.premsg}: [ensamble_file] skipped frags [{_skipped}]")
            else:
                self.status = "error"
                await self.clean_when_error()
                raise AsyncHLSDLError(f"{self.premsg}: error when ensambling parts")

    def fragsnotdl(self) -> list:
        return [
            frag
            for frag in self.info_frag
            if (frag["downloaded"] is False) and not frag.get("skipped", False)
        ]

    def fragsdl(self) -> list:
        return [
            frag
            for frag in self.info_frag
            if (frag["downloaded"] is True) or frag.get("skipped", False)
        ]

    def print_hookup(self):

        _pre = f"[HLS][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}]"

        if self.status == "done":
            return f"{_pre}: Completed\n"

        if self.status == "init_manipulating":
            return f"{_pre}: Waiting for Ensambling\n"

        if self.status == "manipulating":
            _size = self.filename.stat().st_size if self.filename.exists() else 0
            _str = (
                f"[{naturalsize(_size)}/{naturalsize(self.filesize)}]({(_size/self.filesize)*100:.2f}%)"
                if self.filesize
                else f"[{naturalsize(_size)}]")
            return f"{_pre}: Ensambling {_str}\n"

        if self.status in ("init", "error", "stop"):
            _filesize_str = naturalsize(self.filesize) if self.filesize else "--"
            _rel_size_str = f"{naturalsize(self.down_size)}/{naturalsize(self.filesize)}" if self.filesize else "--"
            _prefr = f"[{self.n_dl_fragments:{self.format_frags}}/{self.n_total_fragments}]"

            if self.status == "init":
                return f"{_pre}: Waiting to DL [{_filesize_str}] {_prefr}\n"

            if self.status == "error":
                return f"{_pre}: ERROR [{_rel_size_str}] {_prefr}\n"

            if self.status == "stop":
                return f"{_pre}: STOPPED [{_rel_size_str}] {_prefr}\n"

        if self.status == "downloading":

            return self._print_hookup_downloading(_pre)

    def _print_hookup_downloading(self, _pre):
        _temp = self.upt.copy()
        _dsize = _temp.get("down_size", 0)
        _filesize = _temp.get("filesize")
        _n_dl_frag = _temp.get("n_dl_frag", 0)
        _prefr = f"[{_n_dl_frag:{self.format_frags}}/{self.n_total_fragments}]"
        _progress_str = f"{_dsize / _filesize * 100:5.2f}%" if _filesize else "-----"
        if not self.check_any_event_is_set():
            _speed_meter_str = (
                f"{naturalsize(_speed_meter)}ps"
                if (_speed_meter := _temp.get("speed_meter"))
                else "--"
            )
            _eta_smooth_str = (
                print_delta_seconds(_est_time_smooth)
                if (_est_time_smooth := _temp.get("est_time_smooth"))
                and _est_time_smooth < 3600
                else "--"
            )
        else:
            _eta_smooth_str = "--"
            _speed_meter_str = "--"

            if self._vid_dl.reset_event.is_set():
                with contextlib.suppress(Exception):
                    self.count_msg = AsyncHLSDownloader._QUEUE[str(self.pos)].get_nowait()
        return "".join(
            [
                f"{_pre}: WK[{self.count:2d}/{self.n_workers:2d}] ",
                f"FR{_prefr}",
                f"PR[{_progress_str}] DL[{_speed_meter_str}] ETA[{_eta_smooth_str}]",
                f"\n{self.count_msg}",
            ])
