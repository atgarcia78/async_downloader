import asyncio
import contextlib
import logging
import random
import re
from argparse import Namespace
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Callable, Coroutine, Optional, cast
from urllib.parse import unquote, urlparse, urlunparse
from functools import partial

import aria2p
from aria2p.api import OperationResult
from requests import RequestException

from utils import (
    CONF_ARIA2C_EXTR_GROUP,
    CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED,
    CONF_ARIA2C_MIN_SIZE_SPLIT,
    CONF_ARIA2C_N_CHUNKS_CHECK_SPEED,
    CONF_AUTO_PASRES,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    CONF_PROXIES_N_GR_VIDEO,
    InfoDL,
    LockType,
    ProgressTimer,
    Token,
    async_lock,
    async_wait_for_any,
    async_waitfortasks,
    get_cookies_netscape_file,
    get_format_id,
    get_host,
    getter_basic_config_extr,
    limiter_non,
    load_config_extractors,
    my_dec_on_exception,
    myYTDL,
    naturalsize,
    put_sequence,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    mytry_call,
    try_get,
    update_url,
    variadic,
)

logger = logging.getLogger("asyncaria2dl")


@dataclass
class HookPrint:
    speed_str: str
    progress_str: str
    connections: int
    eta_str: str

    def __init__(self, dl_cont: aria2p.Download):
        self.update(dl_cont)

    def update(self, dl_cont: aria2p.Download):
        self.speed_str = f"{naturalsize(dl_cont.download_speed)}ps"

        self.progress_str = f"{dl_cont.progress:.0f}%"
        self.connections = dl_cont.connections
        self.eta_str = dl_cont.eta_string()


dataSpeed = namedtuple("dataSpeed", ["speed", "conn", "date", "progress"])


class CheckSpeed:
    def __init__(self, aria2dl):
        self._speed = []
        self._aria2dl = aria2dl
        self._index = aria2dl._n_check_speed
        self._min_check = aria2dl._min_check_speed
        self.tasks = []
        self.alock = asyncio.Lock()

    def _print_el(self, item: tuple) -> str:
        _secs = item[2].second + item[2].microsecond / 1000000
        return (
            f"({item[2].strftime('%H:%M:')}{_secs:06.3f}, "
            + f"['speed': {item[0]}, 'connec': {item[1]}])"
        )

    async def _check_speed(self, _prevtask):
        _premsg = f"{self._aria2dl.premsg}[check_speed:check]"

        try:
            if _prevtask:
                await asyncio.wait([_prevtask])
                self.tasks.remove(_prevtask)

            async with self.alock:
                if len(self._speed) <= self._min_check:
                    return
                _res_dl0 = False
                _res_ncon = False
                _sample = self._speed[-self._index:]
                _n_workers = self._aria2dl.n_workers
                if (
                    _res_dl0 := (
                        sum(el.speed for el in _sample) / len(_sample) < 500000
                    )
                ) or (
                    _res_ncon := (
                        _n_workers > 1
                        and all(
                            (el.progress < 95 and el.conn < (_n_workers - 1))
                            for el in _sample
                        )
                    )
                ):
                    logger.info(
                        f"{_premsg} reset: n_speed[{len(self._speed)}] "
                        + f"dl0[{_res_dl0}] ncon[{_res_ncon}]"
                    )
                    self._speed = []
                    await self._aria2dl._reset()
                    await asyncio.sleep(0)
                else:
                    await asyncio.sleep(0)

        except asyncio.CancelledError as e:
            logger.error(f"{_premsg} {repr(e)}")

    def __call__(self, _input_speed):
        self._speed.append(_input_speed)
        if len(self._speed) > self._min_check:
            self.tasks.append(
                self._aria2dl.add_task(
                    self._check_speed(mytry_call(lambda: self.tasks[-1])),
                    name="check_speed",
                )
            )


class AsyncARIA2CDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncARIA2CDLError(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


retry = my_dec_on_exception(
    AsyncARIA2CDLErrorFatal, max_time=60, raise_on_giveup=False, interval=5
)

kill_token = Token("kill")


def aqueue_loaded(n):
    def get_sample(j):
        return try_get(list(range(j)), lambda x: None if random.shuffle(x) else x)

    return put_sequence(asyncio.Queue(), get_sample(n))


class AsyncARIA2CDownloader:
    _CONFIG = load_config_extractors()
    _LOCK = Lock()
    _ALOCK = partial(async_lock, _LOCK)
    _HOSTS_DL = {}
    aria2_API = None

    def __init__(
        self, args: Namespace, ytdl: myYTDL, video_dict: dict, info_dl: InfoDL
    ):
        self.background_tasks = set()
        self.info_dict = video_dict
        self._vid_dl = info_dl
        self._vid_dl_events = [self._vid_dl.reset_event, self._vid_dl.stop_event]
        self.args = args
        self._pos = None

        self.ytdl = ytdl

        with AsyncARIA2CDownloader._LOCK:
            if AsyncARIA2CDownloader.aria2_API is None:
                AsyncARIA2CDownloader.aria2_API = aria2p.API(
                    aria2p.Client(port=self.args.rpcport, timeout=2)
                )

        video_url = unquote(self.info_dict["url"])
        self.uris = [video_url]
        self._extractor = try_get(
            self.info_dict.get("extractor_key"), lambda x: x.lower()
        )
        self._host = get_host(video_url, shorten=self._extractor)

        self.headers = self.info_dict.get("http_headers", {})

        self.download_path = self.info_dict["download_path"]
        self.download_path.mkdir(parents=True, exist_ok=True)

        _filename = Path(self.info_dict.get("filename"))
        self.filename = Path(
            self.download_path,
            f'{_filename.stem}.{self.info_dict["format_id"]}.{self.info_dict["ext"]}',
        )

        self.cookie_file = ""
        if _cookies_str := self.info_dict.get("cookies"):
            self.cookies_file = f"{str(self.filename)}.cookies"
            get_cookies_netscape_file(_cookies_str, self.cookies_file)

        self.dl_cont = None
        self.upt = None
        self.check_speed = None

        self.status = "init"
        self.block_init = True
        self.init_task = set()
        self.error_message = ""

        self.n_workers = self.args.parts

        self.last_progress_str = "--"

        self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")

        self.premsg = "".join(
            f"[{self.info_dict[key]}]" for key in ("id", "title", "format_id")
        )

        def getter(name):
            value, key_text = getter_basic_config_extr(
                name, AsyncARIA2CDownloader._CONFIG
            ) or (None, None)
            if value and key_text:
                self.special_extr = True
                limit = value["ratelimit"].ratelimit(key_text, delay=True)
                maxsplits = value["maxsplits"]
            else:
                self.special_extr = False
                limit = limiter_non.ratelimit("transp", delay=True)
                maxsplits = self.n_workers
            _mode = "simple"
            _sph = False
            if maxsplits < 16:
                _sph = True
                if name in CONF_ARIA2C_EXTR_GROUP:
                    _mode = "group"
            if name in CONF_AUTO_PASRES:
                self.auto_pasres = True
                self._min_check_speed = (
                    CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED // 2
                )
                self._n_check_speed = CONF_ARIA2C_N_CHUNKS_CHECK_SPEED // 2
            return (_sph, _mode, limit, maxsplits)

        self.auto_pasres = False
        self.special_extr = False
        self._min_check_speed = CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED
        self._n_check_speed = CONF_ARIA2C_N_CHUNKS_CHECK_SPEED

        self.asynclock = asyncio.Lock()

        _sem, self._mode, self._decor, self._nsplits = getter(self._extractor)
        self.sem = contextlib.nullcontext()

        if not (_proxy := self.args.proxy) and self.args.enproxy:
            with AsyncARIA2CDownloader._LOCK:
                if self._host not in AsyncARIA2CDownloader._HOSTS_DL:
                    AsyncARIA2CDownloader._HOSTS_DL |= {
                        self._host: {
                            "count": 0,
                            "queue": aqueue_loaded(CONF_PROXIES_MAX_N_GR_HOST),
                        }
                    }
        else:
            self._mode = "noproxy"
            if _sem:
                with self.ytdl.params.setdefault("lock", Lock()):
                    self.ytdl.params.setdefault("sem", {})
                    self.sem = cast(LockType, self.ytdl.params["sem"].setdefault(self._host, Lock()))

        self._proxy = _proxy

        self.n_workers = self._nsplits
        self.down_size = 0
        self.filesize = self.info_dict.get("filesize")
        if not self.filesize:
            if _res := self._get_filesize(self.uris, proxy=_proxy):
                self.filesize, self.down_size = _res[0], _res[1]
        if self.filesize:
            if (
                self.filename.exists()
                and not Path(f"{str(self.filename)}.aria2").exists()
                and (
                    (self.filesize - 100)
                    < (_dsize := self.filename.stat().st_size)
                    < (self.filesize + 100)
                )
            ):
                self.status = "done"
                self.down_size = _dsize
                return

            self.n_workers = min(self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1, self.n_workers)


        opts_dict = {
            "split": self.n_workers,
            "header": "\n".join(
                [f"{key}: {value}" for key, value in self.headers.items()]
            ),
            "dir": str(self.download_path),
            "out": self.filename.name,
            "uri-selector": "inorder",
            "min-split-size": CONF_ARIA2C_MIN_SIZE_SPLIT,
            "dry-run": "false",
            "all-proxy": self._proxy or "",
            "load-cookies": self.cookie_file,
        }

        self.opts = self.set_opts(opts_dict)
        self.progress_timer = ProgressTimer()
        self._qspeed = asyncio.Queue()
        self.n_rounds = 0
        self._index_proxy = -1

    @property
    def pos(self):
        return self._pos

    @pos.setter
    def pos(self, value):
        self._pos = value

    def add_task(
        self, coro: Coroutine | asyncio.Task, *, name: Optional[str] = None
    ) -> asyncio.Task:
        _task = coro
        if not isinstance(coro, asyncio.Task):
            _task = asyncio.create_task(coro, name=name)

        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    async def add_init_task(self):
        async with self.asynclock:
            if not self.init_task:
                self.block_init = True
                self.init_task.add(
                    self.add_task(
                        self.update_uri(), name=f"{self.premsg}[add_init_task]"
                    )
                )

    def set_opts(self, opts_dict):
        opts = AsyncARIA2CDownloader.aria2_API.get_global_options()
        for key, value in opts_dict.items():
            for _value in variadic(value):
                if not opts.set(key, _value):
                    logger.warning(f"{self.premsg} couldnt set [{key}] to [{value}]")
        return opts

    def _get_filesize(self, uris, proxy=None) -> Optional[tuple]:
        with AsyncARIA2CDownloader._LOCK:
            opts_dict = {
                "header": "\n".join(
                    [f"{key}: {value}" for key, value in self.headers.items()]
                ),
                "dry-run": "true",
                "split": 1,
                "out": self.info_dict["id"],
                "all-proxy": proxy or "",
            }
            def _callback(x, _):
                x.stop_listening()

            try:
                logger.debug(f"{self.premsg}[get_filesize] start")
                with self._decor:
                    _dl = self.aria2_API.add_uris(uris, options=self.set_opts(opts_dict))
                    try:
                        self.aria2_API.listen_to_notifications(
                            on_download_complete=_callback, on_download_error=_callback)
                        _dl.update()
                        if _dl.status == "complete":
                            return (_dl.total_length, _dl.completed_length)
                    except Exception as e:
                        logger.error(f"{self.premsg}[get_filesize] error: {str(e)}")
                    finally:
                        for func in [self.aria2_API.stop_listening, _dl.remove]:
                            try:
                                func()
                            except Exception as e:
                                logger.error(f"{self.premsg}[get_filesize] error in [{func}]: {repr(e)}")
                        logger.debug(f"{self.premsg}[get_filesize] bye")

            except Exception as e:
                logger.error(f"{self.premsg}[get_filesize] error: {str(e)}")

    async def _acall(self, func: Callable, /, *args, **kwargs):
        """
        common async executor of aria2 client requests to the server rpc
        """
        try:
            return await sync_to_async(
                func, thread_sensitive=False, executor=self.ex_dl
            )(*args, **kwargs)
        except RequestException as e:
            logger.warning(f"{self.premsg}[acall][{func.__name__}][RequestException] error: {repr(e)}")
            if "add_uris" in func.__name__:
                raise AsyncARIA2CDLErrorFatal("add uris fails") from e
            if await self.reset_aria2c():
                return {"reset": "ok"}
            return {"error": AsyncARIA2CDLErrorFatal("reset failed")}
        except aria2p.ClientException as e:
            logger.warning(f"{self.premsg}[acall][{func.__name__}][ClientException] error: {repr(e)}")
            if "add_uris" in func.__name__:
                raise AsyncARIA2CDLErrorFatal("add uris fails") from e
            return {"reset": "ok"}
        except Exception as e:
            logger.warning(f"{self.premsg}[acall][{func.__name__}][Exception] error: {repr(e)}")
            return {"error": e}

    async def reset_aria2c(self):
        async with AsyncARIA2CDownloader._ALOCK():
            try:
                AsyncARIA2CDownloader.aria2_API.get_stats()
                logger.info(f"{self.premsg}[reset_aria2c] test conn ok")
                return True
            except Exception:
                logger.info(f"{self.premsg}[reset_aria2c] test conn no ok, lets reset aria2c" )
                res, _port = try_get(
                    self._vid_dl.nwsetup.reset_aria2c(), lambda x: x or None
                ) or (None, None)
                logger.info(f"{self.premsg}[reset_aria2c] {_port} {res}")
                if _port:
                    AsyncARIA2CDownloader.aria2_API.client.port = _port
                try:
                    AsyncARIA2CDownloader.aria2_API.get_stats()
                    logger.info(f"{self.premsg}[reset_aria2c] after reset, test conn ok")
                    return True
                except Exception:
                    logger.info(f"{self.premsg}[reset_aria2c]  test conn no ok")

    async def async_pause(self, list_dl: list[aria2p.Download | None]) -> Optional[list[OperationResult]]:
        if list_dl and all(x is not None for x in list_dl):
            return cast(
                list[OperationResult],
                await self._acall(AsyncARIA2CDownloader.aria2_API.pause, list_dl),
            )

    async def async_resume(self, list_dl: list[aria2p.Download | None]) -> Optional[list[OperationResult]]:
        if list_dl and all(x is not None for x in list_dl):
            async with self._decor:
                return cast(
                    list[OperationResult],
                    await self._acall(AsyncARIA2CDownloader.aria2_API.resume, list_dl),
                )

    async def async_remove(self, list_dl: list[aria2p.Download | None]) -> Optional[list[OperationResult]]:
        if list_dl and all(x is not None for x in list_dl):
            return cast(
                list[OperationResult],
                await self._acall(AsyncARIA2CDownloader.aria2_API.remove, list_dl, clean=False),
            )

    async def add_uris(self, uris: list[str]) -> Optional[aria2p.Download]:
        async with self._decor:
            return cast(
                aria2p.Download,
                await self._acall(
                    AsyncARIA2CDownloader.aria2_API.add_uris, uris, options=self.opts
                ),
            )

    async def aupdate(self, dl_cont: Optional[aria2p.Download]):
        if dl_cont is not None:
            return await self._acall(dl_cont.update)
        else:
            return {"error": "dl_cont is None"}

    def uptpremsg(self):
        _upt = f"{self.premsg} host: {self._host} mode: {self._mode}"
        if self._mode != "noproxy":
            _upt += (
                f" proxy: {self._proxy} index_pr: {self._index_proxy}"
                + f" count: {AsyncARIA2CDownloader._HOSTS_DL[self._host]['count']}"
            )
        return _upt

    async def init(self):
        dl_cont = None
        try:
            if dl_cont := await self.add_uris(self.uris):
                if resupt := await self.aupdate(dl_cont):
                    if any(_ in resupt for _ in ("error", "reset")):
                        raise AsyncARIA2CDLError("init: error update dl_cont")
                self.dl_cont = dl_cont
                self.upt = HookPrint(dl_cont)
        except Exception as e:
            _msg_error = str(e)
            if dl_cont and dl_cont.status == "error":
                _msg_error += f" - {dl_cont.error_message}"
            self.error_message = _msg_error
            logger.exception(f"{self.uptpremsg()} [init] error: {_msg_error}")
            self.status = "error"
        finally:
            self.block_init = False

    async def _get_info(self, pytdl, url):
        return get_format_id(
            pytdl.sanitize_info(await pytdl.async_extract_info(url, download=False)),
            self.info_dict["format_id"],
        )

    async def _update_uri_noproxy(self, _init_url):
        async with myYTDL(params=self.ytdl.params, silent=True) as ytdl:
            if not (_info := await self._get_info(ytdl, _init_url)):
                raise AsyncARIA2CDLError("couldnt get video url")

        video_url = unquote(_info["url"])
        self.uris = [video_url]

        if (_host := get_host(video_url, shorten=self._extractor)) != self._host:
            self._host = _host
            if isinstance(self.sem, LockType):
                async with async_lock(self.ytdl.params["lock"]):
                    self.sem = cast(LockType, self.ytdl.params["sem"].setdefault(self._host, Lock()))

    async def get_uri(self, _init_url, _proxy_port: int, n: int = 0):
        def _get_url_proxy(url, port) -> str:
            _url_as_dict = urlparse(url)._asdict()
            _dom = re.sub(r"^(www.)", "", f"{_url_as_dict['netloc']}")
            _url_as_dict["netloc"] = f"asyncdlrouting{port}.{_dom}"
            return urlunparse(list(_url_as_dict.values()))

        _proxy = f"http://127.0.0.1:{_proxy_port}"
        logger.debug(f"{self.premsg} proxy ip{n} {_proxy}")

        try:
            async with myYTDL(
                params=self.ytdl.params, silent=True, proxy=_proxy
            ) as proxy_ytdl:
                proxy_info = await self._get_info(proxy_ytdl, _init_url)

            _url = cast(str, traverse_obj(proxy_info, ("url", {unquote})))
            logger.debug(f"{self.premsg} ip{n} {_proxy} uri{n} {_url}")

            if not _url:
                raise AsyncARIA2CDLError("couldnt get video url")

            if self._mode == "simple":
                return _url
            else:
                return _get_url_proxy(_url, _proxy_port)
        except Exception as e:
            _msg = f"host[{self._host}]pr[{n}:{_proxy}]{str(e)}"
            logger.debug(
                f"{self.premsg}[{self.info_dict.get('original_url')}] "
                + f"ERROR init uris {_msg}"
            )

    async def _update_uri_proxy_simple(self, _init_url):
        _proxy_port = CONF_PROXIES_BASE_PORT + self._index_proxy * 100
        self._proxy = f"http://127.0.0.1:{_proxy_port}"
        self.opts.set("all-proxy", self._proxy)
        return await self.get_uri(_init_url, _proxy_port)

    async def _update_uri_proxy_group(self, _init_url: str) -> Optional[list[str]]:
        self._proxy = "http://127.0.0.1:8899"
        self.opts.set("all-proxy", self._proxy)

        if self.filesize:
            _n = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
            _gr = min(_n // self._nsplits or 1, CONF_PROXIES_N_GR_VIDEO)
        else:
            _gr = CONF_PROXIES_N_GR_VIDEO

        self.n_workers = _gr * self._nsplits
        self.opts.set("split", self.n_workers)

        logger.debug(
            f"{self.premsg}enproxy[{self.args.enproxy}]mode[{self._mode}]"
            + f"proxy[{self._proxy}]_gr[{_gr}]n_workers[{self.n_workers}]"
        )

        _base_proxy_port = CONF_PROXIES_BASE_PORT + self._index_proxy * 100

        _tasks = [
            self.add_task(
                self.get_uri(_init_url, _base_proxy_port + j, n=j),
                name=f"{self.premsg}[update_uri][{j}]",
            )
            for j in range(1, _gr + 1)
        ]

        _res = await async_waitfortasks(
            fs=_tasks, events=self._vid_dl_events, get_results=True
        )

        logger.debug(f"{self.premsg} {_res}")
        if traverse_obj(_res, ("condition", "event")):
            return
        if not (_temp := traverse_obj(_res, ("results"))):
            raise AsyncARIA2CDLError(f"{self.premsg} couldnt get uris")

        return try_get(
            list(variadic(_temp)), lambda x: x.remove(None) if None in x else x
        )

    async def update_uri(self):
        async def _get_index_proxy(_host: str) -> int:
            _res = await async_waitfortasks(
                fs=AsyncARIA2CDownloader._HOSTS_DL[_host]["queue"].get(),
                events=self._vid_dl_events,
            )
            if traverse_obj(_res, ("condition", "event")):
                return -1
            if (_temp := traverse_obj(_res, ("results", 0))) is None:
                raise AsyncARIA2CDLError("couldnt get index proxy")

            async with AsyncARIA2CDownloader._ALOCK():
                AsyncARIA2CDownloader._HOSTS_DL[_host]["count"] += 1
            return cast(int, _temp)

        logger.debug(f"{self.premsg}[update_uri] start")

        _init_url = self.info_dict.get("webpage_url")
        if self._extractor == "doodstream":
            _init_url = update_url(_init_url, query_update={"check": "no"})
        if self.special_extr:
            _init_url = smuggle_url(_init_url, {"indexdl": self.pos})

        if self._mode == "noproxy":
            if self.n_rounds > 1:
                await self._update_uri_noproxy(_init_url)
        else:
            if (_temp := await _get_index_proxy(self._host)) < 0:
                return
            self._index_proxy = _temp

            if self._mode == "simple":
                if _proxy_info_url := await self._update_uri_proxy_simple(_init_url):
                    self.uris = [_proxy_info_url]

            elif self._mode == "group":
                if _temp := await self._update_uri_proxy_group(_init_url):
                    self.uris = _temp

        logger.debug(f"{self.premsg}[update_uri] end with uris:\n{self.uris}")

    def check_any_event_is_set(self, incpause=True):
        _events = self._vid_dl_events
        if incpause:
            _events.append(self._vid_dl.pause_event)
        return [_ev.name for _ev in _events if _ev.is_set()]

    async def event_handle(self):
        _res = {}
        if self._vid_dl.pause_event.is_set():
            await self.async_pause([self.dl_cont])
            await asyncio.sleep(0)
            _res = await async_wait_for_any(
                [self._vid_dl.resume_event] + self._vid_dl_events
            )
            await self.async_resume([self.dl_cont])
            self._vid_dl.pause_event.clear()
            self._vid_dl.resume_event.clear()
            if "resume" in _res["event"]:
                _res["event"].remove("resume")
                if not _res["event"]:
                    _res = {}
            await asyncio.sleep(0)
            self.progress_timer.reset()
        elif _event := self.check_any_event_is_set(incpause=False):
            _res = {"event": _event}
            self.progress_timer.reset()

        if _event := _res.get("event"):
            if "stop" in _event:
                self.status = "stop"
            if "reset" in _event:
                self._vid_dl.reset_event.clear()

            await asyncio.sleep(0)
            await self.async_remove([self.dl_cont])

        return _res

    async def error_handle(self, error):
        tout = None
        await self.async_remove([self.dl_cont])
        if error == "471":
            tout = 30
        elif error == "403":
            tout = 60
        _res = await async_wait_for_any(self._vid_dl_events, timeout=tout)
        if _event := _res.get("event"):
            if "stop" in _event:
                self.status = "stop"
            if "reset" in _event:
                self._vid_dl.reset_event.clear()
        await asyncio.sleep(0)

    async def _reset(self, cause: Optional[str] = None):
        if self.status == "downloading":
            self._vid_dl.reset_event.set(cause)

    async def fetch(self):
        def _update_counters(_bytes_dl: int):
            if (_iter_bytes := _bytes_dl - self.down_size) > 0:
                self.down_size += _iter_bytes
                self._vid_dl.total_sizes["down_size"] += _iter_bytes

        try:
            if (
                self.status in ("done", "error", "stop")
                or "event" in await self.event_handle()
            ):
                return
            if not self.uris:
                raise AsyncARIA2CDLErrorFatal("couldnt get uris")
            await self.init()
            if (
                self.status in ("done", "error", "stop")
                or "event" in await self.event_handle()
            ):
                return
            if not self.dl_cont:
                raise AsyncARIA2CDLErrorFatal("couldnt get dl_cont")

            while self.dl_cont.status in ["active", "paused", "waiting"]:
                if self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):
                    if "event" in await self.event_handle():
                        return
                    if _result := await self.aupdate(self.dl_cont):
                        if "error" in _result:
                            raise AsyncARIA2CDLError(
                                "fetch error: error update dl_cont"
                            )
                        if "reset" in _result:
                            await self._reset()
                            return
                    else:
                        _update_counters(self.dl_cont.completed_length)
                        self.upt.update(self.dl_cont)
                        if (
                            self.args.check_speed
                            and not self.block_init
                            and not self.check_any_event_is_set()
                        ):
                            _data = dataSpeed(
                                self.dl_cont.download_speed,
                                self.dl_cont.connections,
                                datetime.now(),
                                self.dl_cont.progress,
                            )
                            self.check_speed(_data)

                await asyncio.sleep(0)

            if self.dl_cont.status == "complete":
                self.status = "done"

            elif self.dl_cont.status == "error":
                error_code = try_get(
                    re.findall(
                        r"(?:status|estado)=(\d\d\d)", self.dl_cont.error_message
                    ),
                    lambda x: x[0] if x else None,
                )
                if error_code and error_code in ("471", "403"):
                    logger.warning(
                        f"{self.uptpremsg()}[fetch] error handle: {error_code}"
                    )
                    await self.error_handle(error_code)
                else:
                    raise AsyncARIA2CDLError("fetch error")

        except Exception as e:
            _msg_error = str(e)
            if self.dl_cont and self.dl_cont.status == "error":
                _msg_error += f" - {self.dl_cont.error_message}"

            logger.exception(f"{self.uptpremsg()} [fetch] error: {_msg_error}")
            self.status = "error"
            self.error_message = _msg_error

    async def fetch_async(self):
        async def _setup():
            self.n_rounds += 1
            self.dl_cont = None
            self.upt = None

            self._index_proxy = -1
            if self._mode != "noproxy":
                self._proxy = None
            self._vid_dl.clear()
            self.check_speed._speed = []
            if self.check_speed.tasks:
                list(map(lambda x: x.cancel(), self.check_speed.tasks))
                await asyncio.wait(self.check_speed.tasks)
                self.check_speed.tasks = []

        async def _handle_init_task():
            await self.add_init_task()
            _res = await async_waitfortasks(
                fs=list(self.init_task), events=self._vid_dl_events, get_results=True
            )
            self.init_task.clear()
            logger.debug(f"{self.premsg}[handle_init_task] {_res} uris[{self.uris}]")
            if _e := traverse_obj(_res, ("errors", 0)):
                raise _e
            if not self.uris:
                raise AsyncARIA2CDLError(f"{self.presmg} no uris")

        async def _clean_index():
            async with AsyncARIA2CDownloader._ALOCK():
                _host_info = AsyncARIA2CDownloader._HOSTS_DL[self._host]
                _host_info["count"] -= 1
                _host_info["queue"].put_nowait(self._index_proxy)

        self.status = "downloading"
        self.progress_timer.reset()
        if self.args.check_speed:
            self.check_speed = CheckSpeed(self)
        try:
            while True:
                await _setup()
                try:
                    await _handle_init_task()
                    async with async_lock(self.sem):
                        await self.fetch()
                    if self.status in ("done", "error", "stop"):
                        return
                except Exception as e:
                    _msg_error = str(e)
                    if self.dl_cont and self.dl_cont.status == "error":
                        _msg_error += f" - {self.dl_cont.error_message}"
                    logger.error(
                        f"{self.uptpremsg()} [fetch_async] error: {_msg_error}"
                    )
                    self.status = "error"
                    self.error_message = _msg_error
                finally:
                    if all([self._mode != "noproxy", self._index_proxy != -1]):
                        await _clean_index()
                    await asyncio.sleep(0)
        except Exception as e:
            logger.error(f"{self.premsg}[fetch_async] {str(e)}")
        finally:
            logger.debug(f"{self.premsg}[fetch_async] exiting")

    def print_hookup(self):
        def _downloading_print_hookup(premsg):
            self.last_progress_str = self.upt.progress_str
            _pre_info = (
                f"CONN[{self.upt.connections:2d}/{self.n_workers:2d}] "
                + f"DL[{self.upt.speed_str}] PR[{self.upt.progress_str}] ETA[{self.upt.eta_str}]\n")
            return f"{premsg} {_pre_info}"

        msg = ""
        _pre = f'[ARIA2C][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}]'
        _pre2 = (
            f'{naturalsize(self.down_size, format_=".2f")} '
            + f'[{naturalsize(self.filesize, format_=".2f")}]'
            if self.filesize
            else "NA"
        )

        if self.status == "done":
            msg = f"{_pre} Completed\n"
        elif self.status == "init":
            msg = f"{_pre} Waiting {_pre2}\n"
        elif self.status == "error":
            msg = f"{_pre} ERROR {_pre2}\n"
        elif self.status == "stop":
            msg = f"{_pre} STOPPED {_pre2}\n"
        elif self.status == "manipulating":
            msg = f"{_pre} Ensambling {_pre2}\n"
        elif self.status == "downloading":
            if all(
                [
                    self.dl_cont,
                    self.upt,
                    not self.block_init,
                    not self.check_any_event_is_set(),
                ]
            ):
                msg = _downloading_print_hookup(_pre)
            else:
                if self.block_init:
                    _substr = "INIT"
                elif self._vid_dl.pause_event.is_set():
                    _substr = "PAUSED"
                elif self._vid_dl.reset_event.is_set():
                    _substr = "RESET"
                elif self._vid_dl.stop_event.is_set():
                    _substr = "STOPPED"
                else:
                    _substr = "UNKNOWN"

                msg = f"{_pre} {_substr} DL PR[{self.last_progress_str}]\n"

        return msg
