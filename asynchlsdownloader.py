import asyncio
import binascii
import contextlib
import json
import logging
import math
import random
import shutil
import subprocess
import time
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from queue import Queue
from threading import BoundedSemaphore, Lock
from typing import AsyncIterator

import aiofiles
import aiofiles.os
import httpx
import m3u8
from Cryptodome.Cipher import AES
from Cryptodome.Cipher._mode_cbc import CbcMode

from utils import (
    CLIENT_CONFIG,
    CONF_HLS_RESET_403_TIME,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    Coroutine,
    CountDowns,
    InfoDL,
    List,
    MyRetryManager,
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
    async_suppress,
    async_waitfortasks,
    await_for_any,
    empty_queue,
    get_cookies_jar,
    get_format_id,
    get_host,
    getter_basic_config_extr,
    int_or_none,
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
    variadic,
    wait_for_either,
)

logger = logging.getLogger("asynchlsdl")

kill_token = Token("kill")


@dataclass
class DownloadFragContext:
    info_frag: dict
    file: str | Path
    timer: ProgressTimer
    data: bytes
    resp: AsyncIterator | None
    num_bytes_downloaded: int
    isok: bool
    size: int
    url: str
    headers_range: dict

    def __init__(self, info_frag: dict):
        self.info_frag = info_frag
        self.file = info_frag["file"]
        self.timer = ProgressTimer()
        self.data = b""
        self.resp = None
        self.num_bytes_downloaded = 0
        self.isok = False
        self.size = info_frag["size"]
        self.url = info_frag["url"]
        self.headers_range = info_frag["headers_range"]


class AsyncHLSDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info

    def __str__(self):
        return f"{self.__class__.__name__}{self.args[0]}"


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
    max_tries=5,
    raise_on_giveup=False,
    interval=10)

on_503_hsize = my_dec_on_exception(
    StatusError503, max_time=360, raise_on_giveup=False, interval=10)

on_503 = my_dec_on_exception(
    StatusError503, max_time=360, raise_on_giveup=True, interval=10)

network_exceptions = (httpx.ReadTimeout, httpx.RemoteProtocolError)

on_network_exception = my_dec_on_exception(
    network_exceptions, max_tries=10, raise_on_giveup=True, interval=5)


class AsyncHLSDownloader:
    _PLNS = {}
    _CHUNK_SIZE = (
        8196  # 16384  # 65536  # 10485760  # 16384  # 1024  # 102400 #10485760
    )
    _MAX_RETRIES = 5
    _MAX_RESETS = 20
    _CONFIG = load_config_extractors()
    _CLASSLOCK = Lock()

    _COUNTDOWNS = None
    _QUEUE = {}
    _INPUT = Queue()
    _INRESET_403 = set()
    _qproxies = None

    def __init__(
        self, args: Namespace, ytdl: myYTDL, video_dict: dict, info_dl: InfoDL
    ):
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
            self.config_file = Path(
                self.base_download_path, f"config_file.{self.info_dict['format_id']}")
            _filename = Path(self.info_dict.get("filename"))
            self.fragments_base_path = Path(
                self.download_path,
                f'{_filename.stem}.{self.info_dict["format_id"]}.{self.info_dict["ext"]}')
            self.filename = Path(
                self.base_download_path,
                f'{_filename.stem}.{self.info_dict["format_id"]}.ts')

            self.premsg = (
                f'[{self.info_dict["id"]}]'
                + f'[{self.info_dict["title"]}]'
                + f'[{self.info_dict["format_id"]}]')

            self.count_msg = ""
            self.key_cache = {}
            self.n_reset = 0
            self.down_size = 0
            self.status = "init"
            self.error_message = ""
            self.upt = {}
            self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_hlsdl")
            self.sync_to_async = partial(
                sync_to_async, thread_sensitive=False, executor=self.ex_dl)

            self.totalduration = traverse_obj(self.info_dict, "duration", default=0)
            self.filesize = traverse_obj(self.info_dict, "filesize", default=0)

            self._proxy = {}
            if _proxy := self.args.proxy:
                self._proxy = {"http://": _proxy, "https://": _proxy}
            # elif self.args.enproxy:
            #     self.prepare_proxy()

            self.smooth_eta = SmoothETA()
            self.progress_timer = ProgressTimer()
            self.speedometer = SpeedometerMA()
            self.frags_queue = asyncio.Queue()
            self.comm = asyncio.Queue()
            self._asynclock = asyncio.Lock()
            self.areset = self.sync_to_async(self.resetdl)

            self.config_httpx = lambda: {
                "proxies": self._proxy,
                "limits": httpx.Limits(keepalive_expiry=30),
                "follow_redirects": True,
                "timeout": httpx.Timeout(30),
                "verify": False,
                "cookies": try_get(
                    self.info_dict,
                    lambda x: get_cookies_jar(
                        x.get("cookies") or x["formats"][0]["cookies"]
                    ),
                ),
                "headers": try_get(
                    self.info_dict,
                    lambda x: x.get("http_headers") or x["formats"][0]["http_headers"],
                )
                or CLIENT_CONFIG["http_headers"],
            }

            self.clients = {}

            self.init_client: httpx.Client

            self._sem = asyncio.Semaphore()

            self.special_extr = False
            self.auto_pasres = False
            self.fromplns = None
            self._extractor = try_get(
                self.info_dict.get("extractor_key"), lambda x: x.lower())

            def getter(name: Optional[str]) -> tuple:
                if not name:
                    return (self.n_workers, 0, contextlib.nullcontext())
                if "nakedsword" in name:
                    self.auto_pasres = True
                    if self.info_dict.get("playlist_id"):
                        if all(
                            _ not in self.info_dict.get("playlist_title", "")
                            for _ in ("MostWatchedScenes", "Search")
                        ):
                            self.fromplns = str_or_none(self.info_dict.get("_id_movie"))
                value, key_text = getter_basic_config_extr(
                    name, AsyncHLSDownloader._CONFIG
                ) or (None, None)
                if value and key_text:
                    self.special_extr = True
                    if "nakedsword" in key_text:
                        self._extractor = key_text = "nakedsword"
                    return (
                        value["maxsplits"],
                        value["interval"],
                        value["ratelimit"].ratelimit(key_text, delay=True),
                    )

                return (self.n_workers, 0, contextlib.nullcontext())

            _nworkers, self._interv, self._limit = getter(self._extractor)

            self.n_workers = (
                max(self.n_workers, _nworkers)
                if _nworkers >= 16
                else min(self.n_workers, _nworkers)
            )

            self.m3u8_doc = None
            self._host = get_host(self.info_dict["url"])

            self.frags_to_dl = []
            self.info_frag = []
            self.info_init_section = {}
            self.n_dl_fragments = 0

            if self.filename.exists() and (_dsize := self.filename.stat().st_size) > 0:
                self.status = "done"
                self.down_size = _dsize
                return

            self.init()

        except Exception as e:
            logger.exception(repr(e))

    def prepare_proxy(self):
        with AsyncHLSDownloader._CLASSLOCK:
            if not AsyncHLSDownloader._qproxies:
                _seq = zip(
                    random.sample(
                        range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST
                    ),
                    random.sample(
                        range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST
                    ),
                )

                AsyncHLSDownloader._qproxies = put_sequence(Queue(), _seq)

        el1, el2 = AsyncHLSDownloader._qproxies.get()
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
                            )
                        )
                    info = info["entries"][_pl_index - 1]
                if new_info := get_format_id(info, self.info_dict["format_id"]):
                    _info = {
                        key: new_info[key]
                        for key in ("url", "manifest_url")
                        if key in new_info
                    }
                    self.info_dict.update(_info)
        except Exception as e:
            logger.exception("[init info proxy] %s", str(e))

    def upt_plns(self):
        with AsyncHLSDownloader._CLASSLOCK:
            if "ALL" not in AsyncHLSDownloader._PLNS:
                AsyncHLSDownloader._PLNS["ALL"] = {
                    "sem": BoundedSemaphore(),
                    "downloading": set(),
                    "in_reset": set(),
                    "reset": MySyncAsyncEvent(name="fromplns[ALL]", initset=True),
                }
            if self.fromplns not in AsyncHLSDownloader._PLNS:
                AsyncHLSDownloader._PLNS[self.fromplns] = {
                    "downloaders": {self.info_dict["_index_scene"]: self},
                    "downloading": set(),
                    "in_reset": set(),
                    "reset": MySyncAsyncEvent(
                        name=f"fromplns[{self.fromplns}]", initset=True
                    ),
                    "sem": BoundedSemaphore(),
                    "asem": asyncio.Semaphore(),
                }
            else:
                AsyncHLSDownloader._PLNS[self.fromplns]["downloaders"].update(
                    {self.info_dict["_index_scene"]: self})

        _downloaders = AsyncHLSDownloader._PLNS[self.fromplns]["downloaders"]
        logger.debug(
            f"{self.premsg}: "
            + f"added new dl to plns [{self.fromplns}], "
            + f"count [{len(_downloaders)}] "
            + f"members[{list(_downloaders.keys())}]")

    @property
    def pos(self):
        return self._pos

    @pos.setter
    def pos(self, value):
        self._pos = value
        self.premsg = f"[{value}]{self.premsg}"
        AsyncHLSDownloader._QUEUE[str(self._pos)] = Queue()
        if self.fromplns:
            self.upt_plns()

    def add_task(
        self, coro: Union[Coroutine, asyncio.Task], *, name: Optional[str] = None
    ) -> asyncio.Task:
        _task = coro
        if not isinstance(coro, asyncio.Task):
            _task = asyncio.create_task(coro, name=name)
        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    @on_503
    @on_exception
    def download_key(self, key_uri: str) -> Optional[bytes]:
        with self._limit:
            return try_get(
                send_http_request(
                    key_uri,
                    client=self.init_client,
                    logger=logger.debug,
                    new_e=AsyncHLSDLError,
                ),
                lambda x: x.content if x else None,
            )

    @on_503
    @on_exception
    def get_m3u8_doc(self) -> Optional[str]:
        _error = ""
        with self._limit:
            if (
                not (
                    res := send_http_request(
                        self.info_dict["url"],
                        client=self.init_client,
                        logger=logger.debug,
                        new_e=AsyncHLSDLError,
                    )
                )
                or isinstance(res, dict)
                and (_error := res.get("error"))
            ):
                raise AsyncHLSDLError(
                    f"{self.premsg}:[get_m3u8_doc] error downloading m3u8 doc {_error}"
                )
            return res.content.decode("utf-8", "replace")

    @on_503_hsize
    @on_exception
    def get_headersize(self, ctx: DownloadFragContext) -> Optional[int]:
        pre = f"{self.premsg}[get_headersize][frag-{ctx.info_frag['frag']}]"
        if ctx.headers_range:
            logger.warning(f"{pre} NOK - fragments wth header range")
            return

        _error = ""
        if (
            not (
                res := send_http_request(
                    ctx.url,
                    _type="HEAD",
                    headers={"Range": "bytes=0-100"},
                    client=self.init_client,
                    logger=logger.debug,
                    new_e=AsyncHLSDLError,
                )
            )
            or isinstance(res, dict)
            and (_error := res.get("error"))
        ):
            logger.warning(f"{pre} NOK - couldnt get res to head {_error}")
            return

        logger.debug(
            f"{pre} REQUEST: {res.request} headers: {res.request.headers}\n"
            + f"RESPONSE: {res} headers: {res.headers}\nINFO_FRAG: {ctx.info_frag}"
        )
        if not (
            hsize := try_get(
                res.headers.get("content-range"), lambda x: int(x.split("/")[1])
            )
        ):
            logger.warning(f"{pre} NOK - not content range in res")
            return
        else:
            logger.debug(f"{pre} {hsize}")
            ctx.info_frag["headersize"] = hsize
            return hsize

    @on_503
    @on_exception
    def download_init_section(self):
        with self._limit:
            try:
                if res := send_http_request(
                    self.info_init_section['url'],
                    client=self.init_client,
                    headers=self.info_init_section['headers_range'],
                    logger=logger.debug,
                    new_e=AsyncHLSDLError,
                ):
                    if isinstance(res, dict):
                        raise AsyncHLSDLError(
                            f"{self.premsg}:[get_init_section] {res['error']}"
                        )
                    _data = res.content
                    with open(self.info_init_section['file'], "wb") as fsect:
                        fsect.write(_data)
                    self.info_init_section["downloaded"] = True
                else:
                    raise AsyncHLSDLError(
                        f"{self.premsg}:[get_init_section] couldnt get init section"
                    )
            except Exception as e:
                logger.exception(f"{self.premsg}:[get_init_section] {repr(e)}")
                raise

    def get_init_section(self, _initfrag):
        _file_path = Path(f"{str(self.fragments_base_path)}.Frag0")
        _url = _initfrag.absolute_uri
        if "&hash=" in _url and _url.endswith("&="):
            _url += "&="
        byte_range = {}
        headers_range = {}
        if _initfrag.byterange:
            el = _initfrag.byterange.split('@')
            byte_range = {"start": int(el[1]), "end": int(el[0])}
            headers_range = {
                "range": f"bytes={byte_range['start']}-{byte_range['end'] - 1}"
            }
        self.info_init_section = {
            "frag": 0,
            "url": _url,
            "file": _file_path,
            # "key": _initfrag.key,
            # "cipher": traverse_obj(self.key_cache, (try_call(lambda: _initfrag.key.uri), "cipher")),
            "byte_range": byte_range,
            "headers_range": headers_range,
            "downloaded": _file_path.exists(),
        }
        if not self.info_init_section["downloaded"]:
            self.download_init_section()

    def get_info_fragments(self) -> Union[list, m3u8.SegmentList]:
        def _load_keys(keys):
            if not keys:
                return
            for _key in variadic(keys):
                if (
                    _key
                    and _key.method == "AES-128"
                    and _key.iv
                    and not (
                        _cipher := traverse_obj(self.key_cache, (_key.uri, "cipher"))
                    )
                    and (_valkey := self.download_key(_key.absolute_uri))
                ):
                    _cipher = AES.new(
                        _valkey, AES.MODE_CBC, binascii.unhexlify(_key.iv[2:])
                    )
                    self.key_cache[_key.uri] = {"key": _valkey, "cipher": _cipher}

        def _get_segments_interval_time(_start_time, _duration, _list_segments):
            logger.debug(
                f"{self.premsg}[get_info_fragments] start time: {_start_time} duration: {_duration}"
            )
            _start_segment = int(_start_time // _duration) - 1
            logger.debug(
                f"{self.premsg}[get_info_fragments] start seg {_start_segment}"
            )
            if _end_time := self.info_dict.get("_end_time"):
                _last_segment = min(int(_end_time // _duration), len(_list_segments))
            else:
                _last_segment = len(_list_segments)
            logger.debug(f"{self.premsg}[get_info_fragments] last seg {_last_segment}")
            return _list_segments[_start_segment:_last_segment]

        def _prepare_segments(_list_segments):
            byte_range = {}
            for fragment in _list_segments:
                if not fragment.uri and fragment.parts:
                    fragment.uri = fragment.parts[0].uri
                headers_range = {}
                if fragment.byterange:
                    splitted_byte_range = fragment.byterange.split("@")
                    sub_range_start = (
                        try_get(splitted_byte_range, lambda x: int(x[1]))
                        or byte_range.get("end") or 0)
                    byte_range = {
                        "start": sub_range_start,
                        "end": sub_range_start + int(splitted_byte_range[0]),
                    }
                    headers_range = {
                        "range": f"bytes={byte_range['start']}-{byte_range['end'] - 1}"
                    }
                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="
                fragment.__dict__["url"] = _url
                fragment.__dict__["byte_range"] = byte_range
                fragment.__dict__["headers_range"] = headers_range
                fragment.__dict__["cipher"] = traverse_obj(
                    self.key_cache, (try_call(lambda: fragment.key.uri), "cipher")
                )

            return _list_segments

        try:
            if (
                not self.m3u8_doc
                or not (
                    m3u8_obj := m3u8.loads(self.m3u8_doc, uri=self.info_dict["url"])
                )
                or not m3u8_obj.segments
            ):
                raise AsyncHLSDLError("couldnt get m3u8 file")

            _load_keys(m3u8_obj.keys)

            if _initfrag := m3u8_obj.segments[0].init_section:
                if not self.info_init_section:
                    self.get_init_section(_initfrag)

            _duration = try_get(
                getattr(m3u8_obj, "target_duration", None), lambda x: float(x)
            )
            _start_time = self.info_dict.get("_start_time")

            if _duration and _start_time:
                m3u8_obj.data["segments"] = _get_segments_interval_time(
                    _start_time, _duration, m3u8_obj.data["segments"]
                )
                m3u8_obj._initialize_attributes()

            return _prepare_segments(m3u8_obj.segments)
        except Exception as e:
            logger.error(f"{self.premsg}[get_info_fragments] - {repr(e)}")
            raise AsyncHLSDLErrorFatal("error get info fragments") from e

    def get_config_data(self) -> dict:
        _data = {}
        if self.config_file.exists():
            with open(self.config_file, "rt", encoding="utf-8") as finit:
                if _content := json.loads(finit.read()):
                    _data = {int(k): v for k, v in _content.items()}
        return _data

    def _check_is_dl(self, ctx: DownloadFragContext, hsize=None) -> bool:
        is_ok = False
        if not hsize:
            hsize = try_call(lambda: ctx.info_frag["headersize"])
            if not hsize and ctx.resp:
                if not ctx.resp.headers.get("content-encoding"):
                    hsize = try_call(
                        lambda: int_or_none(ctx.resp.headers["content-length"])
                    )  # or self.get_headersize(ctx)
        size = try_call(lambda: ctx.file.stat().st_size) or -1
        if size == 0 or (
            size > 0 and hsize and not (hsize - 100 <= size <= hsize + 100)
        ):
            try_call(lambda: ctx.file.unlink())
            size = -1
        if size != -1:
            is_ok = True
        ctx.isok = is_ok
        ctx.size = size
        ctx.info_frag |= {"headersize": hsize, "size": size, "downloaded": is_ok}

        return ctx.isok

    async def _async_check_is_dl(self, ctx, **kwargs) -> bool:
        return await self.sync_to_async(self._check_is_dl)(ctx, **kwargs)

    def _create_info_frag(self, i, frag, hsize):
        _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i + 1}")
        _info_frag = {
            "frag": i + 1,
            "url": frag.url,
            "key": frag.key,
            "cipher": frag.cipher,
            "file": _file_path,
            "headers_range": frag.headers_range,
            "downloaded": False,
            "skipped": False,
            "size": -1,
            "headersize": hsize,
            "error": [],
        }
        ctx = DownloadFragContext(_info_frag)
        if self._check_is_dl(ctx, hsize=hsize):
            self.down_size += ctx.size
            self.n_dl_fragments += 1
        else:
            self.frags_to_dl.append(i + 1)
        self.info_frag.append(ctx.info_frag)

    def _update_info_frag(self, i, frag, hsize):
        if self.info_frag[i]["downloaded"]:
            return
        ctx = DownloadFragContext(self.info_frag[i])
        ctx.info_frag["skipped"] = False
        if self._check_is_dl(ctx, hsize=hsize):
            self.down_size += ctx.size
            self.n_dl_fragments += 1
        else:
            ctx.info_frag |= {
                "url": frag.url,
                "headers_range": frag.headers_range,
                "key": frag.key,
                "cipher": frag.cipher,
            }
            self.frags_to_dl.append(i + 1)

    def init(self):
        self.n_reset = 0
        self.frags_to_dl = []
        self.init_client = httpx.Client(**self.config_httpx())

        _mode_init = not bool(self.info_frag)

        try:
            if not self.m3u8_doc:
                self.m3u8_doc = self.get_m3u8_doc()

            self.info_dict["fragments"] = self.get_info_fragments()

            if _mode_init:
                self.n_total_fragments = len(self.info_dict["fragments"])
                self.format_frags = (
                    f"{(int(math.log(self.n_total_fragments, 10)) + 1)}d"
                )
                if not self.totalduration:
                    self.totalduration = self.calculate_duration()
                self._avg_size = False
                if not self.filesize:
                    self._avg_size = True
                    self.filesize = self.calculate_filesize()

            config_data = self.get_config_data()

            for i, fragment in enumerate(self.info_dict["fragments"]):
                if _mode_init:
                    self._create_info_frag(i, fragment, config_data.get(i + 1))
                else:
                    self._update_info_frag(i, fragment, config_data.get(i + 1))

            if not self.frags_to_dl:
                self.status = "init_manipulating"

            logger.debug(
                f"{self.premsg}: Frags already DL: {len(self.fragsdl())}, "
                + f"Frags not DL: {len(self.fragsnotdl())}, "
                + f"Frags to request: {len(self.frags_to_dl)}\n{self.info_frag}"
            )

            logger.debug(
                f"{self.premsg}: total duration "
                + f"{print_norm_time(self.totalduration)} -- "
                + f"estimated filesize {naturalsize(self.filesize) if self.filesize else 'NA'} -- already downloaded "
                + f"{naturalsize(self.down_size)} -- total fragments "
                + f"{self.n_total_fragments} -- fragments already dl "
                + f"{self.n_dl_fragments}"
            )

        except Exception as e:
            logger.exception(f"{self.premsg}[init] {str(e)}")
            self.status = "error"
            self.init_client.close()

    def calculate_duration(self) -> int:
        return sum(fragment.duration for fragment in self.info_dict["fragments"])

    def calculate_filesize(self) -> Optional[int]:
        return (
            int(self.totalduration * 1000 * _bitrate / 8)
            if (_bitrate := self.info_dict.get("tbr"))
            else None
        )

    def check_any_event_is_set(self, incpause=True) -> List[Optional[str]]:
        _events = [self._vid_dl.reset_event, self._vid_dl.stop_event]
        if incpause:
            _events.append(self._vid_dl.pause_event)
        return [_ev.name for _ev in _events if _ev.is_set()]

    def check_stop(self):
        if self._vid_dl.stop_event.is_set():
            raise StatusStop("stop event")

    def multi_extract_info(
        self, url: str, proxy: Optional[str] = None, msg: Optional[str] = None
    ) -> dict:
        premsg = f"[multi_extract_info]{msg or ''}"

        try:
            self.check_stop()
            if not self.args.proxy and proxy:
                logger.warning(f"{premsg} ojo entrnado en myTDL")
                with myYTDL(
                    params=self.ytdl.params, proxy=proxy, silent=True
                ) as proxy_ytdl:
                    _info_video = proxy_ytdl.sanitize_info(
                        proxy_ytdl.extract_info(url, download=False)
                    )
            else:
                # if proxy was included in args main program, ytdl will have this proxy in its params
                _info_video = self.ytdl.sanitize_info(
                    self.ytdl.extract_info(url, download=False)
                )

            self.check_stop()
            if not _info_video:
                raise AsyncHLSDLErrorFatal("no info video")
            return _info_video

        except (StatusStop, AsyncHLSDLErrorFatal):
            raise
        except Exception as e:
            logger.error(f"{premsg} fails when extracting info {repr(e)}")
            raise AsyncHLSDLErrorFatal("error extracting info video") from e

    def prep_reset(self, info_reset: Optional[dict] = None):
        if info_reset:
            self.info_dict |= info_reset
            self._host = get_host(self.info_dict["url"])

            try:
                self.init_client.close()
            except Exception as e:
                logger.error(
                    f"{self.premsg}:RESET[{self.n_reset}]:prep_reset error {repr(e)}"
                )

            self.init_client = httpx.Client(**self.config_httpx())

            self.info_dict["fragments"] = self.get_info_fragments()

        self.frags_to_dl = []
        config_data = self.get_config_data()

        for i, fragment in enumerate(self.info_dict["fragments"]):
            self._update_info_frag(i, fragment, config_data.get(i + 1))

        if not self.frags_to_dl:
            self.status = "init_manipulating"
        else:
            logger.debug(
                f"{self.premsg}:RESET[{self.n_reset}]:prep_reset:OK "
                + f"{self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}"
            )

    @retry
    def get_reset_info(self, _reset_url: str, first=False) -> dict:
        _base = f"{self.premsg}[get_reset_inf]"
        _proxy = self._proxy.get("http://")
        _print_proxy = lambda: (f":proxy[{_proxy.split(':')[-1]}]" if _proxy else "")  # noqa: E731
        _print_plns = lambda: (f":PLNS[{self.fromplns}]" if self.fromplns else "")  # noqa: E731
        _pre = lambda: "".join(  # noqa: E731
            [
                f"{_base}:RESET[{self.n_reset}]:first[{first}]",
                f"{_print_plns()}{_print_proxy()}",
            ]
        )

        logger.debug(_pre())

        _info = None
        info_reset = None

        try:
            if self.fromplns:
                if first:
                    if _info := self.multi_extract_info(
                        _reset_url, proxy=_proxy, msg=_pre()
                    ):
                        self.ytdl.cache.store(
                            "nakedswordmovie", str(self.fromplns), _info
                        )
                else:
                    if not (
                        _info := self.ytdl.cache.load(
                            "nakedswordmovie", str(self.fromplns)
                        )
                    ):
                        if _info := self.multi_extract_info(
                            _reset_url, proxy=_proxy, msg=_pre()
                        ):
                            self.ytdl.cache.store(
                                "nakedswordmovie", str(self.fromplns), _info
                            )
                if _info:
                    info_reset = try_get(
                        traverse_obj(
                            _info, ("entries", int(self.info_dict["_index_scene"]) - 1)
                        ),
                        lambda x: get_format_id(x, self.info_dict["format_id"])
                        if x
                        else None,
                    )

            elif _info := self.multi_extract_info(_reset_url, proxy=_proxy, msg=_pre()):
                if _len_entries := len(_info.get("entries", [])):
                    if _len_entries == 1:
                        _pl_index = 1
                    else:
                        _pl_index = traverse_obj(
                            self.info_dict,
                            "playlist_index",
                            "playlist_autonumber",
                            default=1,
                        )
                    _info = _info["entries"][_pl_index - 1]
                info_reset = get_format_id(_info, self.info_dict["format_id"])

            if not info_reset:
                raise AsyncHLSDLErrorFatal(f"{_pre()} fails no descriptor")

            logger.debug(
                f"{_pre()} format extracted info video ok\n{_for_print(info_reset)}"
            )

            self.prep_reset(info_reset)
            return {"res": "ok"}

        except StatusStop as e:
            logger.debug(f"{_pre()} check stop {repr(e)}")
            raise
        except AsyncHLSDLErrorFatal as e:
            logger.debug(f"{_pre()} {repr(e)}")
            if self.fromplns and not first:
                self.ytdl.cache.store("nakedswordmovie", str(self.fromplns), {})
            raise
        except Exception as e:
            logger.error(f"{_pre()} fails when extracting reset info {repr(e)}")
            raise

    def resetdl(self, cause: Optional[str] = None):
        def _pre():
            _proxy = self._proxy.get("http://")
            _print_proxy = f":proxy[{_proxy.split(':')[-1]}]" if _proxy else ""
            _print_plns = f":PLNS[{self.fromplns}]" if self.fromplns else ""
            return "".join(
                [
                    f"{self.premsg}[resetdl]:RESET[{self.n_reset}]",
                    f"{_print_plns}{_print_proxy}",
                ]
            )

        logger.debug(_pre())

        def _handle_wait_plns_403():
            AsyncHLSDownloader._INRESET_403.add(self.info_dict["id"])
            with AsyncHLSDownloader._CLASSLOCK:
                if not AsyncHLSDownloader._COUNTDOWNS:
                    AsyncHLSDownloader._COUNTDOWNS = CountDowns(
                        AsyncHLSDownloader, logger=logger
                    )

            self.check_stop()
            AsyncHLSDownloader._COUNTDOWNS.add(
                n=CONF_HLS_RESET_403_TIME,
                index=str(self.pos),
                event=self._vid_dl.stop_event,
                msg=self.premsg,
            )

            self.check_stop()
            logger.debug(f"{_pre()} fin wait in reset cause 403")

        def _handle_reset_for_plns():
            if str(cause) == "403":
                _handle_wait_plns_403()
            with (_sem := AsyncHLSDownloader._PLNS["ALL"]["sem"]):
                logger.debug(f"{_pre()} in sem")
                _first_all = _sem._initial_value == 1
                with (_sem2 := AsyncHLSDownloader._PLNS[self.fromplns]["sem"]):
                    logger.debug(f"{_pre()} in sem2")
                    _first = _sem2._initial_value == 1
                    if _first_all and str(cause) == "403":
                        time.sleep(5)

                    _listreset = [
                        int(index)
                        for index in list(
                            AsyncHLSDownloader._PLNS[self.fromplns]["in_reset"]
                        )
                    ]
                    _aux = {
                        "indexdl": self.pos,
                        "args": {self.fromplns: {"listreset": _listreset}},
                    }
                    _plns_url = smuggle_url(self.info_dict["original_url"], _aux)
                    if (
                        _resinfo := self.get_reset_info(_plns_url, first=_first)
                    ) and "res" in _resinfo:
                        if _first:
                            _sem2._initial_value = 100
                            _sem2.release(50)
                        if _first_all:
                            _sem._initial_value = 100
                            _sem.release(50)
                        time.sleep(1)

        def _handle_reset_simple():
            _webpage_url = self.info_dict["webpage_url"]
            if self.special_extr:
                _webpage_url = smuggle_url(_webpage_url, {"indexdl": self.pos})
            self.get_reset_info(_webpage_url)

        try:
            if self.fromplns:
                _handle_reset_for_plns()
            else:
                _handle_reset_simple()
        except StatusStop:
            logger.debug(f"{_pre()} stop_event")
            raise
        except Exception as e:
            logger.exception(
                f"{_pre()} stop_event:[{self._vid_dl.stop_event}] "
                + f"outer Exception {repr(e)}"
            )
            raise
        finally:
            if self.fromplns:
                if str(cause) == "403":
                    AsyncHLSDownloader._INRESET_403.discard(self.info_dict["id"])
                logger.debug(f"{_pre()} stop_event[{self._vid_dl.stop_event}] FINALLY")

                with AsyncHLSDownloader._CLASSLOCK:
                    _inreset = AsyncHLSDownloader._PLNS[self.fromplns]["in_reset"]
                    _inreset.discard(self.info_dict["_index_scene"])
                    if not _inreset:
                        logger.debug(
                            f"{_pre()} end of resets fromplns [{self.fromplns}]"
                        )

                        AsyncHLSDownloader._PLNS[self.fromplns]["reset"].set()
                        AsyncHLSDownloader._PLNS[self.fromplns]["sem"] = (
                            BoundedSemaphore()
                        )

                if _inreset:
                    if self._vid_dl.stop_event.is_set():
                        return
                    logger.debug(
                        f"{_pre()} waits for scenes in [{self.fromplns}] to start DL [{_inreset}]"
                    )
                    wait_for_either(
                        [
                            AsyncHLSDownloader._PLNS[self.fromplns]["reset"],
                            self._vid_dl.stop_event,
                        ],
                        timeout=300,
                    )

                with AsyncHLSDownloader._CLASSLOCK:
                    AsyncHLSDownloader._PLNS["ALL"]["in_reset"].discard(self.fromplns)
                    if not (_inreset := AsyncHLSDownloader._PLNS["ALL"]["in_reset"]):
                        logger.debug(f"{_pre()} end for all plns ")
                        AsyncHLSDownloader._PLNS["ALL"]["reset"].set()
                        AsyncHLSDownloader._PLNS["ALL"]["sem"] = BoundedSemaphore()
                        self.n_reset += 1
                        logger.debug(f"{_pre()}  exit reset")
                        return

                if _inreset:
                    if self._vid_dl.stop_event.is_set():
                        return
                    self.n_reset += 1
                    logger.debug(f"{_pre()} exit reset")
                    return
            else:
                self.n_reset += 1
                logger.debug(f"{_pre()} exit reset")

    async def event_handle(self, msg: str) -> dict:
        _res = {}
        if self._vid_dl.pause_event.is_set():
            logger.debug(f"{msg}:[handle] pause detected")
            _res["pause"] = True
            async with self._sem:
                logger.debug(f"{msg}:[handle] through sem, waiting for resume")
                if self._vid_dl.pause_event.is_set():
                    _res |= await await_for_any(
                        [
                            self._vid_dl.resume_event,
                            self._vid_dl.reset_event,
                            self._vid_dl.stop_event,
                        ],
                        timeout=300,
                    )
                    logger.debug(f"{msg}:[handle] after wait pause: {_res}")
                    if "resume" in _res.get("event", ""):
                        _res.pop("event")
                    self._vid_dl.resume_event.clear()
                    self._vid_dl.pause_event.clear()
                    await asyncio.sleep(0)
                    # return _res
                logger.debug(f"{msg}:[handle] after wait pause: {_res}")
        if _event := self.check_any_event_is_set(incpause=False):
            _res["event"] = _event
        return _res

    async def _handle_reset(
        self, cause: Optional[str] = None, nworkers: Optional[int] = None
    ):
        _tasks = []
        if self.fromplns:
            _asem = AsyncHLSDownloader._PLNS[self.fromplns]["asem"]
            _reset = AsyncHLSDownloader._PLNS[self.fromplns]["reset"]
            if not _asem.locked() and _reset.is_set():
                async with _asem:
                    _tasks = await AsyncHLSDownloader.reset_plns(
                        self.fromplns, cause=cause, wait=False, nworkers=nworkers
                    )
            else:
                logger.debug(
                    f"[handle_reset] PLNS[{self.fromplns}] reset process already ongoing"
                )
                if cause == "403":
                    self._vid_dl.reset_event.set("403")
        else:
            _tasks = await self._reset(cause=cause, wait=False)
        return _tasks

    async def _reset(self, cause: Optional[str] = None, wait=True):
        if self.status != "downloading":
            return []
        _wait_tasks = []

        if not self._vid_dl.reset_event.is_set():
            self._vid_dl.reset_event.set(cause)
            await asyncio.sleep(0)
        elif cause == "403":
            self._vid_dl.reset_event.set(cause)

        if self.tasks:
            if _wait_tasks := [
                _task
                for _task in self.tasks
                if _task not in [asyncio.current_task()] and _task.cancel()
            ]:
                if wait:
                    await asyncio.wait(_wait_tasks)
        return _wait_tasks

    @classmethod
    async def reset_plns(
        cls,
        plns: str,
        cause: Optional[str] = "403",
        wait=True,
        nworkers: Optional[int] = None,
    ):
        if plns not in AsyncHLSDownloader._PLNS:
            logger.error(f"[reset_plns] PLNS[{plns}] not yet handled")
            return []

        _THIS_PLNS = AsyncHLSDownloader._PLNS[plns]
        _ALL_PLNS = AsyncHLSDownloader._PLNS["ALL"]

        _wait_all_tasks = []

        dict_dl = _THIS_PLNS["downloaders"]
        list_dl = _THIS_PLNS["downloading"]
        list_reset = _THIS_PLNS["in_reset"]

        if (
            list_dl
            and dict_dl
            and (
                _to_reset := {dl: key for key, dl in dict_dl.items() if key in list_dl}
            )
        ):
            _ALL_PLNS["reset"].clear()
            _THIS_PLNS["reset"].clear()
            _ALL_PLNS["in_reset"].add(plns)
            for dl, key in _to_reset.items():  # type: ignore
                list_reset.add(key)
                if nworkers:
                    dl.n_workers = nworkers
                if _tasks := await dl._reset(cause=cause, wait=False):
                    _wait_all_tasks.extend(_tasks)
                await asyncio.sleep(0)

        if wait and _wait_all_tasks:
            await asyncio.wait(_wait_all_tasks)
        return _wait_all_tasks

    async def back_from_reset_plns(self, plns: str, premsg: str):
        _tasks_all = []
        dict_dl = traverse_obj(AsyncHLSDownloader._PLNS, (plns, "downloaders"))
        list_reset = traverse_obj(AsyncHLSDownloader._PLNS, (plns, "in_reset"))
        if list_reset and dict_dl:
            _to_wait = [dl for key, dl in dict_dl.items() if key in list_reset]  # type: ignore
            _tasks_all.extend(
                [
                    self.add_task(
                        dl._vid_dl.end_tasks.async_wait(timeout=300),
                        name=f"await_end_tasks_{dl.premsg}",
                    )
                    for dl in _to_wait
                ]
            )

        logger.debug(f"{premsg} endtasks {_tasks_all}")

        if _tasks_all:
            await async_waitfortasks(fs=_tasks_all, events=self._vid_dl.stop_event)

    @on_503
    @on_network_exception
    async def _download_frag(self, index: int, msg: str, nco: int):
        _premsg = f"{msg}:[frag-{index}]:[dl]"

        async def _clean_frag(ctx: DownloadFragContext, exc: Exception):
            ctx.info_frag["error"].append(repr(exc))
            ctx.info_frag["downloaded"] = False
            ctx.info_frag["skipped"] = False
            ctx.info_frag["size"] = -1
            ctx.data = b""
            async with async_suppress(OSError):
                await aiofiles.os.remove(ctx.file)
            if ctx.num_bytes_downloaded:
                async with self._asynclock:
                    self.down_size -= ctx.num_bytes_downloaded
                    self._vid_dl.total_sizes["down_size"] -= ctx.num_bytes_downloaded
                ctx.num_bytes_downloaded = 0

        async def _initial_checkings_ok(
            ctx: DownloadFragContext, response: AsyncIterator
        ):
            ctx.resp = response
            if ctx.resp.status_code == 403:
                if _wait_tasks := await self._handle_reset(cause="403"):
                    await asyncio.wait(_wait_tasks)
                raise AsyncHLSDLErrorFatal(
                    f"{_premsg} resp code:{str(ctx.resp.status_code)}"
                )

            elif ctx.resp.status_code in (500, 502, 503, 520, 521):
                raise StatusError503(
                    f"{_premsg} error status code {ctx.resp.status_code}"
                )

            elif ctx.resp.status_code >= 400:
                raise AsyncHLSDLError(
                    f"{_premsg} resp code:{str(ctx.resp.status_code)}"
                )

            if _res := await self._async_check_is_dl(ctx):
                async with self._asynclock:
                    self.down_size += ctx.size
                    self.n_dl_fragments += 1

            return _res

        async def _check_frag(ctx: DownloadFragContext):
            _nsize = -1
            async with async_suppress(OSError):
                _nsize = await aiofiles.os.path.getsize(ctx.file)
            if _nsize == -1:
                logger.warning(f"{_premsg} no frag file\n{ctx.info_frag}")
                raise AsyncHLSDLError(f"{_premsg} no frag file")
            ctx.size = ctx.info_frag["size"] = _nsize
            if not (_nhsize := ctx.info_frag["headersize"]):
                _nhsize = _nsize
            if _nhsize - 100 <= _nsize <= _nhsize + 100:
                ctx.info_frag["downloaded"] = True
                async with self._asynclock:
                    self.n_dl_fragments += 1
            else:
                logger.warning(f"{_premsg} frag not completed\n{ctx.info_frag}")
                raise AsyncHLSDLError(f"{_premsg} frag not completed")

        def _update_counters(bytes_dl: int, old_bytes_dl: int) -> int:
            if (inc_bytes := bytes_dl - old_bytes_dl) > 0:
                self.comm.put_nowait(inc_bytes)
            return bytes_dl

        async def _decrypt(data: bytes, cipher: Optional[CbcMode]) -> bytes:
            return await self.sync_to_async(cipher.decrypt)(data) if cipher else data

        async def _handle_iter(ctx: DownloadFragContext, data: Optional[bytes]):
            if data:
                ctx.data += await _decrypt(data, ctx.info_frag.get("cipher"))
                ctx.num_bytes_downloaded = _update_counters(
                    ctx.resp.num_bytes_downloaded, ctx.num_bytes_downloaded
                )
            if ctx.timer.has_elapsed(CONF_INTERVAL_GUI / 2):
                if _check := await self.event_handle(_premsg):
                    if "pause" in _check:
                        ctx.timer.reset()
                    if "event" in _check:
                        return _check

        async for retry in MyRetryManager(self._MAX_RETRIES, self._limit):
            _ctx = DownloadFragContext(self.info_frag[index - 1])
            try:
                if _ev := self.check_any_event_is_set(incpause=False):
                    raise AsyncHLSDLErrorFatal(f"{_premsg} {_ev}")

                async with self.clients[nco].stream(
                    "GET", _ctx.url, headers=_ctx.headers_range
                ) as resp:
                    logger.debug(
                        f"{_premsg}: {resp.request} {resp} content-encoding [{resp.headers.get('content-encoding')}] hsize: [{resp.headers.get('content-length')}]"
                    )

                    if await _initial_checkings_ok(_ctx, resp):
                        return
                    else:
                        async for chunk in resp.aiter_bytes(
                            chunk_size=self._CHUNK_SIZE
                        ):
                            if _ev := await _handle_iter(_ctx, chunk):
                                raise AsyncHLSDLErrorFatal(f"{_premsg} {_ev}")
                if _ctx.data:
                    async with aiofiles.open(_ctx.file, "wb") as fileobj:
                        await fileobj.write(_ctx.data)
                    _ctx.data = b""
                return await _check_frag(_ctx)

            except (
                asyncio.CancelledError,
                RuntimeError,
                StatusError503,
                AsyncHLSDLErrorFatal,
            ) as e:
                logger.debug(f"{_premsg}: Error: {repr(e)}")
                if e.__class__.__name__ == "StatusError503":
                    logger.warning(f"{_premsg}: Error: {repr(e)}")
                await _clean_frag(_ctx, e)
                raise
            except network_exceptions as e:
                logger.warning(f"{_premsg}: Error: {repr(e)}")
                await _clean_frag(_ctx, e)
                if _cl := self.clients.pop(nco, None):
                    await _cl.aclose()
                self.clients[nco] = httpx.AsyncClient(**self.config_httpx())
                raise
            except (AsyncHLSDLError, Exception) as e:
                logger.error(f"{_premsg}: Error: {repr(e)}")
                await _clean_frag(_ctx, e)
                if retry.attempt == retry.retries:
                    _ctx.info_frag["error"].append("MaxLimitRetries")
                    _ctx.info_frag["skipped"] = True
                    logger.warning(f"{_premsg}: frag download skipped mmax retries")

    async def fetch(self, nco: int):
        premsg = f"{self.premsg}:[worker-{nco}]:[fetch]"
        logger.debug(f"{premsg} init worker")

        self.clients[nco] = httpx.AsyncClient(**self.config_httpx())

        try:
            while True:
                try:
                    qindex = self.frags_queue.get_nowait()
                    if qindex == kill_token:
                        return
                    await self._download_frag(qindex, premsg, nco)
                except asyncio.QueueEmpty as e:
                    logger.debug(f"{premsg} inner exception {repr(e)}")
                    await asyncio.sleep(0)
        except Exception as e:
            _msg_error = f"{premsg} outer exception - {repr(e).replace(premsg, '')}"
            logger.debug(_msg_error) if isinstance(
                e, AsyncHLSDLErrorFatal
            ) else logger.error(_msg_error)
        finally:
            async with self._asynclock:
                self.count -= 1
            if _cl := self.clients.pop(nco, None):
                await _cl.aclose()
            logger.debug(f"{premsg} bye worker")

    async def fetch_async(self):
        _premsg = f"{self.premsg}[fetch_async]"
        n_frags_dl = 0

        def avg_filesize():
            if not self._avg_size or self.n_dl_fragments < 2:
                return self.filesize
            else:
                return self.n_total_fragments * (self.down_size / self.n_dl_fragments)

        async def upt_status():
            _timer = ProgressTimer()
            self._inc_bytes = 0

            async def _upt():
                while True:
                    try:
                        self._inc_bytes += self.comm.get_nowait()
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(0)
                        break
                if not self.check_any_event_is_set():
                    async with self._asynclock:
                        self.down_size += self._inc_bytes
                        self._vid_dl.total_sizes["down_size"] += self._inc_bytes
                        self.filesize = self._vid_dl.total_sizes["filesize"] = (
                            avg_filesize()
                        )
                    self._inc_bytes = 0
                    _speed_meter = self.speedometer(self.down_size)
                    _est_time = None
                    _est_time_smooth = None
                    if _speed_meter and self.filesize:
                        _est_time = (self.filesize - self.down_size) / _speed_meter
                        _est_time_smooth = self.smooth_eta(_est_time)
                    self.upt.update(
                        {
                            "n_dl_fragments": self.n_dl_fragments,
                            "speed_meter": _speed_meter,
                            "down_size": self.down_size,
                            "est_time": _est_time,
                            "est_time_smooth": _est_time_smooth,
                        }
                    )

            while not self._vid_dl.end_tasks.is_set():
                if _timer.has_elapsed(seconds=CONF_INTERVAL_GUI):
                    await _upt()
                await asyncio.sleep(0)
            await _upt()

        async def init_plns():
            if not self.fromplns:
                return
            if _event := traverse_obj(
                AsyncHLSDownloader._PLNS, (self.fromplns, "reset")
            ):
                if not _event.is_set():
                    logger.debug(f"{_premsg} waiting at start")
                    _res = await await_for_any(
                        [_event, self._vid_dl.stop_event], timeout=300
                    )
                    if _res.get("event") == "stop":
                        self.status = "stop"
                        return
            async with async_lock(AsyncHLSDownloader._CLASSLOCK):
                AsyncHLSDownloader._PLNS[self.fromplns]["downloading"].add(
                    self.info_dict["_index_scene"]
                )
                AsyncHLSDownloader._PLNS["ALL"]["downloading"].add(self.fromplns)

        def _setup():
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

        await init_plns()

        try:
            while True:
                _setup()
                upt_task = self.add_task(upt_status(), name=f"{self.premsg}[upt_task]")
                self.tasks = [
                    self.add_task(self.fetch(i), name=f"{self.premsg}[{i}]")
                    for i in range(self.n_workers)
                ]

                try:
                    await asyncio.wait(self.tasks)
                    self._vid_dl.end_tasks.set()
                    await asyncio.sleep(0)

                    if self._vid_dl.stop_event.is_set():
                        self.status = "stop"
                        return

                    if (_nfragsdl := len(self.fragsdl())) == len(
                        self.info_dict["fragments"]
                    ):
                        self.status = "init_manipulating"
                        logger.debug(f"{_premsg} Frags DL completed")
                        return

                    inc_frags_dl = _nfragsdl - n_frags_dl
                    n_frags_dl = _nfragsdl

                    # reset event was set
                    if _cause := self._vid_dl.reset_event.is_set():
                        if self.n_reset < self._MAX_RESETS:
                            try:
                                logger.debug(
                                    f"{_premsg}:RESET[{self.n_reset}]:CAUSE[{_cause}]"
                                )
                                if self.fromplns:
                                    await self.back_from_reset_plns(
                                        self.fromplns, self.premsg
                                    )
                                    self.check_stop()
                                _final_cause = self._vid_dl.reset_event.is_set()
                                logger.debug(
                                    f"{_premsg}:RESET[{self.n_reset}]:CAUSE[original-{_cause}:final-{_final_cause}]"
                                )
                                await self.areset(_final_cause)
                                self.check_stop()
                                if _cause == "hard":
                                    self.n_reset -= 1
                                continue
                            except StatusStop:
                                logger.debug(
                                    f"{_premsg}:RESET[{self.n_reset}]:STOP event"
                                )
                                self.status = "stop"
                                return
                            except Exception as e:
                                logger.exception(
                                    f"ERROR reset couldnt progress:[{str(e)}]"
                                )
                                self.status = "error"
                                raise AsyncHLSDLErrorFatal(
                                    f"{_premsg} ERROR reset"
                                ) from e
                        else:
                            logger.warning(
                                f"{_premsg}:RESET[{self.n_reset}]:ERROR:Max_number_of_resets"
                            )
                            self.status = "error"
                            raise AsyncHLSDLErrorFatal(f"{_premsg} ERROR max resets")
                    elif inc_frags_dl > 0:
                        await self.sync_to_async(self.prep_reset)()
                        continue
                    else:
                        self.status = "error"
                        raise AsyncHLSDLErrorFatal(
                            f"{_premsg} no inc dlfrags in one cycle"
                        )

                except Exception as e:
                    logger.error(f"{_premsg} inner error while loop {repr(e)}")
                    self.status = "error"
                    self._vid_dl.end_tasks.set()
                    await self.clean_when_error()
                    if isinstance(e, AsyncHLSDLErrorFatal):
                        return
                finally:
                    await self.dump_config_file()
                    await asyncio.wait([upt_task])

        except Exception as e:
            logger.error(f"{_premsg} outer error while loop {repr(e)}")
            self.status = "error"
        finally:
            logger.debug(f"{_premsg} Final report info_frag:\n{self.info_frag}")
            await self.dump_config_file(to_log=True)
            self.init_client.close()
            if self.fromplns:
                async with async_lock(AsyncHLSDownloader._CLASSLOCK):
                    _downloading = AsyncHLSDownloader._PLNS[self.fromplns][
                        "downloading"
                    ]
                    _downloading.discard(self.info_dict["_index_scene"])
                    if not _downloading:
                        AsyncHLSDownloader._PLNS["ALL"]["downloading"].discard(
                            self.fromplns
                        )

    async def dump_config_file(self, to_log=False):
        _data = {
            el["frag"]: el["headersize"] for el in self.info_frag if el["headersize"]
        }
        async with aiofiles.open(self.config_file, mode="w") as finit:
            await finit.write(json.dumps(_data))
        if to_log:
            logger.debug(f"{self.premsg}[config_file]\n{json.dumps(_data)}")

    async def clean_when_error(self):
        for frag in self.info_frag:
            if frag["downloaded"] is False and await aiofiles.os.path.exists(
                frag["file"]
            ):
                await aiofiles.os.remove(frag["file"])

    async def ensamble_file(self):
        def _concat_files():
            _glob = str(Path(self.download_path, "*.Frag*"))
            cmd = (
                f'files=(); for file in $(eza -1 {_glob}); do files+=("$file"); done; '
                + f'cat "${{files[@]}}" > {str(self.filename)}'
            )
            return subprocess.run(
                cmd, capture_output=True, shell=True, encoding="utf-8"
            )

        self.status = "manipulating"
        _skipped = 0

        try:
            for frag in self.info_frag:
                if frag.get("skipped"):
                    _skipped += 1
                    continue
                async with async_suppress(OSError):
                    if frag["size"] < 0:
                        frag["size"] = await aiofiles.os.path.getsize(frag["file"])
                if frag["size"] < 0:
                    raise AsyncHLSDLError(
                        f"{self.premsg}: error when ensambling: {frag}"
                    )

            proc = _concat_files()
            logger.debug(f"{self.premsg}[ensamble] proc [rc] {proc.returncode}")
            if proc.returncode:
                raise AsyncHLSDLError(
                    f"{self.premsg}[ensamble_file] proc [stdout]\n"
                    + f"{proc.stdout}\nproc [stderr]\n{proc.stderr}"
                )
        except Exception as e:
            if await aiofiles.os.path.exists(self.filename):
                await aiofiles.os.remove(self.filename)
            logger.exception(f"{self.premsg} Error {str(e)}")
            self.status = "error"
            await self.clean_when_error()
            raise
        finally:
            if await aiofiles.os.path.exists(self.filename):
                logger.debug(f"{self.premsg}: [ensamble_file] ensambled{self.filename}")
                if not self.args.keep_videos:
                    armtree = self.sync_to_async(
                        partial(shutil.rmtree, ignore_errors=True)
                    )
                    await armtree(str(self.download_path))
                self.status = "done"
                if _skipped:
                    logger.warning(
                        f"{self.premsg}: [ensamble_file] skipped frags [{_skipped}]"
                    )
            else:
                self.status = "error"
                await self.clean_when_error()
                raise AsyncHLSDLError(f"{self.premsg}: error when ensambling parts")

    def fragsnotdl(self) -> list:
        return [
            frag
            for frag in self.info_frag
            if not frag["downloaded"] and not frag.get("skipped")
        ]

    def fragsdl(self) -> list:
        return [
            frag for frag in self.info_frag if frag["downloaded"] or frag.get("skipped")
        ]

    def print_hookup(self):
        _pre = f"[HLS][{self.info_dict['format_id']}]"

        if self.status == "done":
            return f"{_pre}: Completed\n"

        if self.status == "init_manipulating":
            return f"{_pre}: Waiting for Ensambling\n"

        if self.status == "manipulating":
            return f"{_pre}: Ensambling\n"

        if self.status in ("init", "error", "stop"):
            _filesize_str = naturalsize(self.filesize) if self.filesize else "--"
            _rel_size_str = f"{naturalsize(self.down_size)}/{_filesize_str}"
            _prefr = f"[{self.n_dl_fragments:{self.format_frags}}/{self.n_total_fragments:{self.format_frags}}]"
            _pre_with_host = f"{_pre}: HOST[{self._host.split('.')[0]}]"

            if self.status == "init":
                return f"{_pre_with_host}: Waiting to DL [{_filesize_str}] {_prefr}\n"

            if self.status == "error":
                return f"{_pre_with_host}: ERROR [{_rel_size_str}] {_prefr}\n"

            if self.status == "stop":
                return f"{_pre}: STOPPED [{_rel_size_str}] {_prefr}\n"

        if self.status == "downloading":
            return f"{_pre}: {self._print_hookup_downloading()}"

    def _print_hookup_downloading(self):
        _temp = self.upt
        _dsize = _temp.get("down_size", 0)
        _n_dl_frag = _temp.get("n_dl_fragments", 0)
        _prefr = f"[{_n_dl_frag:{self.format_frags}}/{self.n_total_fragments:{self.format_frags}}]"
        _progress_str = (
            f"{_dsize / self.filesize * 100:5.2f}%" if self.filesize else "-----"
        )
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
                    self.count_msg = AsyncHLSDownloader._QUEUE[
                        str(self.pos)
                    ].get_nowait()
        return (
            f"WK[{self.count:2d}/{self.n_workers:2d}] FR{_prefr} "
            + f"PR[{_progress_str}] DL[{_speed_meter_str}] ETA[{_eta_smooth_str}]\n{self.count_msg}"
        )
