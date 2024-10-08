import asyncio
import binascii
import contextlib
import json
import logging
import math
import random
import shutil
import struct
import subprocess
import aiofiles
import aiofiles.os
import httpx
import m3u8
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from queue import Queue
from threading import Lock
from typing import AsyncIterator
from collections import deque

from aiofiles.threadpool.binary import AsyncBufferedIOBase
from Cryptodome.Cipher import AES

from utils import (
    CLIENT_CONFIG,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    Coroutine,
    InfoDL,
    List,
    MyRetryManager,
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
    async_suppress,
    await_for_any,
    empty_queue,
    get_cookies_jar,
    get_format_id,
    get_host,
    getter_basic_config_extr,
    load_config_extractors,
    my_dec_on_exception,
    mytry_call,
    myYTDL,
    naturalsize,
    print_delta_seconds,
    print_norm_time,
    put_sequence,
    send_http_request,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    try_get,
    variadic,
)

logger = logging.getLogger("asynchlsdl")

kill_token = Token("kill")


@dataclass
class DownloadFragContext:
    info_frag: dict
    file: str | Path
    temp_file: str | Path
    timer: ProgressTimer
    data: bytes
    resp: AsyncIterator | None
    num_bytes_downloaded: int
    cipher_info: dict
    isok: bool
    size: int
    url: str
    headers_range: list
    init: bool
    temp_file_mode: str
    stream: AsyncBufferedIOBase | None
    tasks: dict
    coros: dict
    tasks_done: dict
    waiting: deque
    running: deque
    tasks_id: int
    alock: asyncio.Lock

    def __init__(self, info_frag: dict):
        self.info_frag = info_frag
        self.file = info_frag["file"]
        self.temp_file = info_frag["temp_file"]
        self.timer = ProgressTimer()
        self.data = b""
        self.resp = None
        self.num_bytes_downloaded = 0
        self.cipher_info = info_frag["cipher_info"]
        self.isok = False
        self.size = info_frag["size"]
        self.url = info_frag["url"]
        self.headers_range = info_frag["headers_range"]
        self.init = info_frag["init"]
        self.temp_file_mode = 'wb'
        self.stream = None
        self.tasks = {}
        self.coros = {}
        self.tasks_done = {}
        self.waiting = deque()
        self.running = deque()
        self.tasks_id = 0
        self.alock = asyncio.Lock()

    def update(self, upt_frag: dict):
        self.info_frag.update(upt_frag)
        self.timer = ProgressTimer()
        self.data = b""
        self.resp = None
        self.num_bytes_downloaded = 0
        self.url = self.info_frag["url"]
        self.headers_range = self.info_frag["headers_range"]
        self.cipher_info = self.info_frag["cipher_info"]
        self.init = self.info_frag["init"]

    async def add_task(self, data):
        async with self.alock:
            await self._add_task(data=data)

    async def _add_task(self, data=None, task_id=None):
        if not task_id and data:
            self.tasks_id += 1
            task_id = self.tasks_id
            if task_id not in self.coros:
                self.coros[task_id] = data
        elif not task_id and not data and self.waiting:
            task_id = self.waiting.popleft()
            data = self.coros.get(task_id)
        if not task_id or not data:
            return
        if not self.running:
            self.running.append(task_id)
            _task = asyncio.create_task(self._task(task_id, data), name=f'upt_stream_{task_id}')
            self.tasks[_task] = task_id
        elif task_id not in self.waiting:
            self.waiting.append(task_id)
            self.waiting = deque(sorted(self.waiting))

    async def _remove_task(self, task_id):
        async with self.alock:
            self.running.remove(task_id)
            await self._add_task()

    async def _task(self, task_id, data):
        try:
            await self.stream.write(data)
        except Exception as e:
            logger.error(f"[stream_write]: Error: {repr(e)}")
        await self._remove_task(task_id)


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


def my_jitter(value: float):
    return int(random.uniform(value * 0.75, value * 1.25))

retry = my_dec_on_exception(
    AsyncHLSDLErrorFatal, max_tries=5, raise_on_giveup=True, interval=5)

on_exception = my_dec_on_exception(
    (AsyncHLSDLError, ReExtractInfo), max_tries=5, raise_on_giveup=True, interval=10)

on_503 = my_dec_on_exception(
    StatusError503, max_time=360, raise_on_giveup=True, jitter=my_jitter, interval=10)

CommonHTTPErrors = (httpx.NetworkError, httpx.TimeoutException, httpx.ProtocolError)

on_network_exception = my_dec_on_exception(
    CommonHTTPErrors, max_tries=5, raise_on_giveup=True, jitter=my_jitter, interval=10)


class AsyncHLSDownloader:
    _CHUNK_SIZE = 8196  # 16384  # 65536  # 10485760  # 16384  # 1024  # 102400 #10485760
    _MAX_RETRIES = 5
    _MAX_RESETS = 20
    _CONFIG = load_config_extractors()
    _CLASSLOCK = Lock()
    _qproxies = None
    # frm plns configuration, on observaton to remove
    _QUEUE = {}
    _INPUT = Queue()
    _COUNTDOWNS = None
    _INRESET_403 = set()

    def __init__(
        self, args: Namespace, ytdl: myYTDL, video_dict: dict, info_dl: InfoDL
    ):
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
        self.speedometer = SpeedometerMA(ave_time=2.0, smoothing=0.3)
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
        self._extractor = try_get(
            self.info_dict.get("extractor_key"), lambda x: x.lower())

        def getter(name: Optional[str]) -> tuple:
            if not name:
                return (self.n_workers, 0, contextlib.nullcontext())
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
            if "gvdblog" not in (_url := self.info_dict["original_url"]):
                _url = self.info_dict["webpage_url"]
            if info := self.multi_extract_info(_url, proxy=_proxy):
                if _len := len(info.get("entries", [])):
                    if _len == 1:
                        _pl_index = 1
                    else:
                        _pl_index = (
                            self.info_dict.get("playlist_index")
                            or self.info_dict.get("playlist_autonumber")
                            or 1
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

    @property
    def pos(self):
        return self._pos

    @pos.setter
    def pos(self, value):
        self._pos = value
        self.premsg = f"[{value}]{self.premsg}"
        AsyncHLSDownloader._QUEUE[str(self._pos)] = Queue()

    def add_task(self, coro: Union[Coroutine, asyncio.Task], *, name: Optional[str] = None) -> asyncio.Task:
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
                lambda x: x.content,
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
            headers_range = {"range": f"bytes={byte_range['start']}-{byte_range['end'] - 1}"}
        self.info_init_section = {
            "frag": 0,
            "url": _url,
            "file": _file_path,
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
                    and _key.uri not in self.key_cache
                ):
                    if (_valkey := self.download_key(_key.absolute_uri)):
                        self.key_cache[_key.uri] = _valkey


        def _get_segments_interval_time(_start_time, _duration, _list_segments):
            logger.debug(f"{self.premsg}[get_info_fragments] start time: {_start_time} duration: {_duration}")
            _start_segment = int(_start_time // _duration) - 1
            logger.debug(f"{self.premsg}[get_info_fragments] start seg {_start_segment}")
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
                    headers_range = {"range": f"bytes={byte_range['start']}-{byte_range['end'] - 1}"}
                _url = fragment.absolute_uri
                if "&hash=" in _url and _url.endswith("&="):
                    _url += "&="
                fragment.__dict__["url"] = _url
                fragment.__dict__["byte_range"] = byte_range or {'start': 0}
                fragment.__dict__["headers_range"] = headers_range

                cipher_info = None
                if decrypt_info := fragment.key:
                    if decrypt_info.method == 'AES-128':
                        if _val_key := self.key_cache.get(decrypt_info.uri):
                            if _iv := decrypt_info.iv:
                                iv = binascii.unhexlify(_iv[2:])
                                cipher_info = {'key': _val_key, 'iv': iv}
                            else:
                                iv = struct.pack('>8xq', fragment.media_sequence)
                                cipher_info = {'key': _val_key, 'iv': iv}

                fragment.__dict__["cipher_info"] = cipher_info

            return _list_segments

        try:
            if (
                not self.m3u8_doc
                or not (m3u8_obj := m3u8.loads(self.m3u8_doc, uri=self.info_dict["url"]))
                or not m3u8_obj.segments
            ):
                raise AsyncHLSDLError("couldnt get m3u8 file")

            _load_keys(m3u8_obj.keys)

            if _initfrag := m3u8_obj.segments[0].init_section:
                if not self.info_init_section:
                    self.get_init_section(_initfrag)

            _duration = try_get(getattr(m3u8_obj, "target_duration", None), lambda x: float(x))
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
        size = mytry_call(lambda: ctx.file.stat().st_size)
        if size:
            is_ok = True
        else:
            if not hsize:
                hsize = mytry_call(lambda: ctx.info_frag["headersize"])
                if not hsize and ctx.resp:
                    if not ctx.resp.headers.get("content-encoding"):
                        _hsize = mytry_call(lambda: int(ctx.resp.headers["content-length"]))
                        if _hsize:
                            hsize = _hsize + (ctx.size if ctx.size > 0 else 0)
                    
            size = mytry_call(lambda: ctx.temp_file.stat().st_size) or -1
            if size == 0 or (
                size > 0 and hsize and hsize + 100 < size
            ):
                mytry_call(lambda: ctx.temp_file.unlink())
                size = -1
            if size != -1:
                if hsize:
                    is_ok = True
                elif not ctx.resp:
                    _upt_range = {"range": f"bytes={ctx.info_frag['byte_range']['start'] + size}-{mytry_call(lambda: ctx.info_frag['byte_range']['end'] - 1) or ''}"}
                    ctx.headers_range.append(_upt_range)
                    ctx.info_frag['headers_range'].append(_upt_range)
                else:
                    ctx.init = False
                    ctx.temp_file_mode = 'ab'
        ctx.isok = is_ok
        ctx.size = size
        ctx.info_frag.update({"headersize": hsize, "size": size, "downloaded": is_ok})
        return ctx.isok

    async def _async_check_is_dl(self, ctx, **kwargs) -> bool:
        return await self.sync_to_async(self._check_is_dl)(ctx, **kwargs)

    def _create_info_frag(self, i, frag, hsize):
        _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i + 1}")
        _temp_file_path = Path(f"{str(self.fragments_base_path)}.Frag{i + 1}.temp")
        _info_frag = {
            "init": True,
            "frag": i + 1,
            "url": frag.url,
            "cipher_info": frag.cipher_info,
            "file": _file_path,
            "temp_file": _temp_file_path,
            "byte_range": frag.byte_range,
            "headers_range": [frag.headers_range],
            "downloaded": False,
            "skipped": False,
            "size": -1,
            "headersize": hsize,
            "error": [],
        }
        ctx = DownloadFragContext(_info_frag)
        if self._check_is_dl(ctx, hsize=hsize):
            self.down_size += ctx.size
            self._vid_dl.total_sizes["down_size"] += ctx.size
            self.n_dl_fragments += 1
        else:
            self.frags_to_dl.append(i + 1)
        self.info_frag.append(ctx.info_frag)

    def _update_info_frag(self, i, frag, hsize):
        if self.info_frag[i]["downloaded"]:
            return
        ctx = DownloadFragContext(self.info_frag[i])
        _upt_frag = {
            "url": frag.url,
            "cipher_info": frag.cipher_info,
            "byte_range": frag.byte_range,
            "headers_range": [frag.headers_range],
            "skipped": False
        }
        ctx.update(_upt_frag)
        if self._check_is_dl(ctx, hsize=hsize):
            self.down_size += ctx.size
            self._vid_dl.total_sizes["down_size"] += ctx.size
            self.n_dl_fragments += 1
        else:
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

    def multi_extract_info(self, url: str, proxy: Optional[str] = None, msg: Optional[str] = None) -> dict:
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
                logger.error(f"{self.premsg}:RESET[{self.n_reset}]:prep_reset error {repr(e)}")

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
        _pre = lambda: "".join(  # noqa: E731
            [
                f"{_base}:RESET[{self.n_reset}]:first[{first}]",
                f"{_print_proxy()}",
            ]
        )

        logger.debug(_pre())

        _info = None
        info_reset = None

        try:
            if _info := self.multi_extract_info(_reset_url, proxy=_proxy, msg=_pre()):
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
            raise
        except Exception as e:
            logger.error(f"{_pre()} fails when extracting reset info {repr(e)}")
            raise

    def resetdl(self, cause: Optional[str] = None):
        def _pre():
            _proxy = self._proxy.get("http://")
            _print_proxy = f":proxy[{_proxy.split(':')[-1]}]" if _proxy else ""
            return "".join(
                [
                    f"{self.premsg}[resetdl]:RESET[{self.n_reset}]",
                    f"{_print_proxy}",
                ]
            )

        logger.debug(_pre())

        def _handle_reset_simple():
            _webpage_url = self.info_dict["webpage_url"]
            if self.special_extr:
                _webpage_url = smuggle_url(_webpage_url, {"indexdl": self.pos})
            self.get_reset_info(_webpage_url)

        try:
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

    async def _handle_reset(self, cause: Optional[str] = None, nworkers: Optional[int] = None):
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
    
    async def _clean_frag(self, ctx: DownloadFragContext, exc: Exception, reset=False):
        ctx.info_frag["error"].append(repr(exc))
        ctx.info_frag["downloaded"] = False
        ctx.info_frag["skipped"] = False
        ctx.info_frag["size"] = -1
        ctx.data = b""
        ctx.resp = None
        if ctx.waiting or ctx.running:
            while True:
                await asyncio.sleep(0)
                if not ctx.waiting and not ctx.running:
                    break
        
        if ctx.stream:
            async with async_suppress(OSError):
                await ctx.stream.close()
        ctx.stream = None
        ctx.num_bytes_downloaded = 0

        if reset:
            _dl_size = -1
            async with async_suppress(OSError):
                _dl_size = await aiofiles.os.path.getsize(ctx.temp_file)
            async with async_suppress(OSError):
                await aiofiles.os.remove(ctx.temp_file)
            if _dl_size > 0:
                async with self._asynclock:
                    self.down_size -= _dl_size
                    self._vid_dl.total_sizes["down_size"] -= _dl_size

    async def _check_frag(self, ctx: DownloadFragContext, _premsg):
        if ctx.waiting or ctx.running:
            while True:
                await asyncio.sleep(0)
                if not ctx.waiting and not ctx.running:
                    break
        if ctx.stream:
            async with async_suppress(OSError):
                await ctx.stream.close()
        ctx.stream = None
        _nsize = -1
        async with async_suppress(OSError):
            _nsize = await aiofiles.os.path.getsize(ctx.temp_file)
        if _nsize == -1:
            logger.warning(f"{_premsg} no frag file\n{ctx.info_frag}")
            raise AsyncHLSDLErrorFatal(f"{_premsg} no frag file")
        ctx.size = ctx.info_frag["size"] = _nsize
        if (
            not (_nhsize := ctx.info_frag["headersize"])
            or  (_nhsize - 100 <= _nsize <= _nhsize + 100)
        ):
            ctx.info_frag["downloaded"] = True
            async with self._asynclock:
                self.n_dl_fragments += 1
        elif _nsize > _nhsize + 100:
            logger.warning(f"{_premsg} frag inconsistent {_nsize} > {_nhsize} + 100")
            raise AsyncHLSDLErrorFatal(f"{_premsg} frag inconsistent")
        else:
            logger.warning(f"{_premsg} frag not completed")
            raise AsyncHLSDLErrorFatal(f"{_premsg} frag not completed")
        _data = await self._decrypt_frag(ctx, _premsg)
        async with aiofiles.open(ctx.file, "wb") as fileobj:
            await fileobj.write(_data)
        async with async_suppress(OSError):
            await aiofiles.os.remove(ctx.temp_file)

    async def _decrypt_frag(self, ctx: DownloadFragContext, _premsg):
        try:
            async with aiofiles.open(ctx.temp_file, 'rb') as _file:
                _data_temp = await _file.read()
        except Exception as e:
            _msg = f"{_premsg} error temp file read - {repr(e)}"
            logger.error(_msg)
            raise AsyncHLSDLErrorFatal(_msg)

        if not ctx.cipher_info or not _data_temp:
            return _data_temp
        _cipher = AES.new(ctx.cipher_info['key'], AES.MODE_CBC, ctx.ipher_info['iv'])
        return await self.sync_to_async(_cipher.decrypt)(_data_temp)

    async def _initial_checkings_frag_ok(self, ctx: DownloadFragContext, response, _premsg):
        ctx.resp = response
        _msg = f"{_premsg} resp code:{str(ctx.resp.status_code)}"
        if ctx.resp.status_code == 403:
            if _wait_tasks := await self._handle_reset(cause="403"):
                await asyncio.wait(_wait_tasks)
            raise AsyncHLSDLErrorFatal(_msg)
        elif ctx.resp.status_code in (502, 503, 504, 520, 521):
            raise StatusError503(_msg)
        elif ctx.resp.status_code >= 400:
            raise AsyncHLSDLError(_msg)

        if _res := await self._async_check_is_dl(ctx):
            async with self._asynclock:
                if ctx.size > 0 and ctx.init:
                    self.down_size += ctx.size
                    self._vid_dl.total_sizes["down_size"] += ctx.size
                self.n_dl_fragments += 1
                self.filesize = self._vid_dl.total_sizes["filesize"] = self.avg_filesize()
        elif ctx.size > 0 and ctx.init:
            async with self._asynclock:
                self.down_size += ctx.size
                self._vid_dl.total_sizes["down_size"] += ctx.size
                self.filesize = self._vid_dl.total_sizes["filesize"] = self.avg_filesize()
        return _res

    @on_503
    @on_network_exception
    async def _download_frag(self, index: int, msg: str, nco: int):
        _premsg = f"{msg}:[frag-{index}]:[dl]"


        async def _update_counters(bytes_dl: int, old_bytes_dl: int) -> int:
            if (inc_bytes := bytes_dl - old_bytes_dl) > 0:
                async with self._asynclock:
                    self.down_size += inc_bytes
                    self._vid_dl.total_sizes["down_size"] += inc_bytes
                    self.filesize = self._vid_dl.total_sizes["filesize"] = self.avg_filesize()
                #await self.comm.put(inc_bytes)
            return bytes_dl


        async def _handle_iter(ctx, data):
            if data:
                ctx.data += data
                if ctx.timer.has_elapsed(CONF_INTERVAL_GUI / 4):
                    ctx.num_bytes_downloaded = await _update_counters(
                        ctx.resp.num_bytes_downloaded, ctx.num_bytes_downloaded)
                if len(ctx.data) >= 128 * self._CHUNK_SIZE:
                    _data_pending = ctx.data
                    await ctx.add_task(_data_pending)
                    ctx.data = b''
                if ctx.timer.has_elapsed(CONF_INTERVAL_GUI / 2):
                    if _check := await self.event_handle(_premsg):
                        if "pause" in _check:
                            ctx.timer.reset()
                        if "event" in _check:
                            raise AsyncHLSDLErrorFatal(f"{_premsg} {_check}")
            else:
                ctx.num_bytes_downloaded = await _update_counters(
                    ctx.resp.num_bytes_downloaded, ctx.num_bytes_downloaded)
                if ctx.data:
                    _data_pending = ctx.data
                    await ctx.add_task(_data_pending)
                    ctx.data = b''

        def _log_response(resp):
            return (
                f"{resp.request} {resp} content-encoding [{resp.headers.get('content-encoding')}] "
                + f"hsize: [{resp.headers.get('content-length')}]"
            )

        async for retry in MyRetryManager(self._MAX_RETRIES, limiter=self._limit):
            _ctx = DownloadFragContext(self.info_frag[index - 1])
            try:
                if _check := await self.event_handle(_premsg):
                    if "pause" in _check:
                        _ctx.timer.reset()
                    if "event" in _check:
                        raise AsyncHLSDLErrorFatal(f"{_premsg} {_check}")

                async with self.clients[nco].stream("GET", _ctx.url, headers=_ctx.headers_range[-1]) as resp:
                    logger.debug(f"{_premsg}: {_log_response(resp)}")

                    if await self._initial_checkings_frag_ok(_ctx, resp, _premsg):
                        return
                    _ctx.stream = await aiofiles.open(_ctx.temp_file, _ctx.temp_file_mode)

                    async for chunk in resp.aiter_bytes(chunk_size=self._CHUNK_SIZE):
                        await _handle_iter(_ctx, chunk)

                if _ctx.data:
                    await _handle_iter(_ctx, None)
                return await self._check_frag(_ctx, _premsg)                

            except (
                asyncio.CancelledError,
                RuntimeError,
                AsyncHLSDLErrorFatal,
            ) as e:
                if isinstance(e, AsyncHLSDLErrorFatal):
                    logger.error(f"{_premsg}: Error: {repr(e)}")
                else:
                    logger.debug(f"{_premsg}: Error: {repr(e)}")
                await self._clean_frag(_ctx, e, reset=True)
                raise
            except (StatusError503, *CommonHTTPErrors) as e:
                logger.warning(f"{_premsg}: Error: {repr(e)}")
                await self._clean_frag(_ctx, e)
                if _cl := self.clients.pop(nco, None):
                    await _cl.aclose()
                self.clients[nco] = httpx.AsyncClient(**self.config_httpx())
                raise
            except (AsyncHLSDLError, Exception) as e:
                logger.exception(f"{_premsg}: Error: {repr(e)}")
                await self._clean_frag(_ctx, e)
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


    def avg_filesize(self):
        if not self._avg_size or self.n_dl_fragments < 2:
            return self.filesize
        else:
            return self.n_total_fragments * (self.down_size / self.n_dl_fragments)

    async def upt_status(self):

        _timer = ProgressTimer()
        # self._inc_bytes = 0

        async def _upt():
            # while True:
            #     try:
            #         self._inc_bytes += self.comm.get_nowait()
            #     except asyncio.QueueEmpty:
            #         # await asyncio.sleep(0)
            #         break

            # async with self._asynclock:
            #     self.down_size += self._inc_bytes
            #     self._vid_dl.total_sizes["down_size"] += self._inc_bytes
            #     self.filesize = self._vid_dl.total_sizes["filesize"] = self.avg_filesize()
            # self._inc_bytes = 0
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
            if _timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2) and not self.check_any_event_is_set():
                await _upt()
            await asyncio.sleep(CONF_INTERVAL_GUI / 4)

        await _upt()


    async def fetch_async(self):
        _premsg = f"{self.premsg}[fetch_async]"
        n_frags_dl = 0

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

        while True:
            try:
                _setup()
                upt_task = self.add_task(
                    self.upt_status(), name=f"{self.premsg}[upt_task]")
                self.tasks = [
                    self.add_task(self.fetch(i), name=f"{self.premsg}[{i}]")
                    for i in range(self.n_workers)]

                await asyncio.wait(self.tasks)
                self._vid_dl.end_tasks.set()
                await asyncio.sleep(0)

                if self._vid_dl.stop_event.is_set():
                    self.status = "stop"
                    break

                if (_nfragsdl := len(self.fragsdl())) == len(
                    self.info_dict["fragments"]
                ):
                    self.status = "init_manipulating"
                    logger.debug(f"{_premsg} Frags DL completed")
                    break

                inc_frags_dl = _nfragsdl - n_frags_dl
                n_frags_dl = _nfragsdl

                if _cause := self._vid_dl.reset_event.is_set():
                    if self.n_reset >= self._MAX_RESETS:
                        logger.warning(
                            f"{_premsg}:RESET[{self.n_reset}]:ERROR:Max_number_of_resets")
                        self.status = "error"
                        raise AsyncHLSDLErrorFatal(f"{_premsg} ERROR max resets")
                    try:
                        logger.debug(f"{_premsg}:RESET[{self.n_reset}]:CAUSE[{_cause}]")
                        _final_cause = self._vid_dl.reset_event.is_set()
                        await self.areset(_final_cause)
                        self.check_stop()
                        if _cause == "hard":
                            self.n_reset -= 1
                        continue
                    except StatusStop:
                        logger.debug(f"{_premsg}:RESET[{self.n_reset}]:STOP event")
                        self.status = "stop"
                        break
                    except Exception as e:
                        logger.exception(f"ERROR reset couldnt progress:[{str(e)}]")
                        self.status = "error"
                        raise AsyncHLSDLErrorFatal(f"{_premsg} ERROR reset") from e

                elif inc_frags_dl <= 0:
                    self.status = "error"
                    raise AsyncHLSDLErrorFatal(f"{_premsg} no inc dlfrags in one cycle")
                else:
                    await self.sync_to_async(self.prep_reset)()

            except Exception as e:
                logger.error(f"{_premsg} inner error while loop {repr(e)}")
                self.status = "error"
                self._vid_dl.end_tasks.set()
                await self.clean_when_error()
                if not isinstance(e, AsyncHLSDLErrorFatal):
                    continue
            finally:
                await self.dump_config_file()
                await asyncio.wait([upt_task])

        logger.debug(f"{_premsg} Final report info_frag:\n{self.info_frag}")
        self.init_client.close()

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
        _progress_str = f"{_dsize / self.filesize * 100:5.2f}%" if self.filesize else "-----"
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
