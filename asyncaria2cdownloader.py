import asyncio
import contextlib
import copy
import logging
import random
import time
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Lock
from urllib.parse import unquote, urlparse, urlunparse
from typing import cast, Union, Coroutine, Callable, Optional
from functools import partial
from argparse import Namespace
import aria2p
from aria2p.api import OperationResult
from requests import RequestException
from utils import (
    CONF_AUTO_PASRES,
    CONF_ARIA2C_EXTR_GROUP,
    CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED,
    CONF_ARIA2C_MIN_SIZE_SPLIT,
    CONF_ARIA2C_N_CHUNKS_CHECK_SPEED,
    CONF_ARIA2C_SPEED_PER_CONNECTION,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    CONF_PROXIES_N_GR_VIDEO,
    load_config_extractors,
    getter_basic_config_extr,
    ProgressTimer,
    async_lock,
    get_format_id,
    limiter_non,
    naturalsize,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    try_get,
    myYTDL,
    async_waitfortasks,
    async_wait_for_any,
    put_sequence,
    my_dec_on_exception,
    get_host,
    variadic,
    LockType,
    Token,
    InfoDL
)

assert CONF_ARIA2C_SPEED_PER_CONNECTION

logger = logging.getLogger("async_ARIA2C_DL")


class AsyncARIA2CDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncARIA2CDLError(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


retry = my_dec_on_exception(
    AsyncARIA2CDLErrorFatal, max_time=60, raise_on_giveup=False, interval=5)

kill_token = Token("kill")


class AsyncARIA2CDownloader:
    _CONFIG = load_config_extractors()
    _CLASSALOCK = asyncio.Lock()
    _LOCK = Lock()
    _INIT = False
    _HOSTS_DL = {}
    aria2_API: aria2p.API

    def __init__(self, port, args: Namespace, ytdl: myYTDL, video_dict: dict, info_dl: InfoDL):
        self.background_tasks = set()
        self.info_dict = video_dict
        self._vid_dl = info_dl
        self.args = args
        self._pos = None
        self._ALOCK = partial(async_lock, AsyncARIA2CDownloader._LOCK)

        self.ytdl = ytdl

        with AsyncARIA2CDownloader._LOCK:
            if not AsyncARIA2CDownloader._INIT:
                AsyncARIA2CDownloader.aria2_API = aria2p.API(
                    aria2p.Client(port=port, timeout=2))
                AsyncARIA2CDownloader._INIT = True

        video_url = unquote(self.info_dict["url"])
        self.uris = cast(list[str], [video_url])
        self._extractor = try_get(
            self.info_dict.get("extractor_key"), lambda x: x.lower())
        self._host = get_host(video_url, shorten=self._extractor)

        self.headers = self.info_dict.get("http_headers", {})
        if _cookie := self.info_dict.get("cookies"):
            self.headers.update({"Cookie": _cookie})

        self.download_path = self.info_dict["download_path"]

        self.download_path.mkdir(parents=True, exist_ok=True)

        _filename = self.info_dict.get(
            "_filename", self.info_dict.get("filename"))
        self.filename = Path(
            self.download_path,
            f'{_filename.stem}.{self.info_dict["format_id"]}' +
            f'.aria2c.{self.info_dict["ext"]}')

        self.dl_cont = None

        self.status = "init"
        self.block_init = True
        self.error_message = ""

        self.n_workers = self.args.parts

        self.last_progress_str = "--"

        self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")

        self.premsg = f'[{self.info_dict["id"]}][{self.info_dict["title"]}][{self.info_dict["format_id"]}]'

        def getter(name):
            value, key_text = getter_basic_config_extr(
                name, AsyncARIA2CDownloader._CONFIG) or (None, None)
            if value and key_text:
                self.special_extr = True
                limit = value["ratelimit"].ratelimit(key_text, delay=True)
                maxplits = value["maxsplits"]
            else:
                self.special_extr = False
                limit = limiter_non.ratelimit("transp", delay=True)
                maxplits = self.n_workers

            _mode = "simple"
            _sph = False
            if maxplits < 16:
                _sph = True
                if name in CONF_ARIA2C_EXTR_GROUP:
                    _mode = "group"

            if name in CONF_AUTO_PASRES:
                self.auto_pasres = True
                self._min_check_speed = CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED // 2

            return (_sph, _mode, limit, maxplits)

        self.auto_pasres = False
        self.special_extr = False
        self._min_check_speed = CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED
        self._n_check_speed = CONF_ARIA2C_N_CHUNKS_CHECK_SPEED

        _sem, self._mode, self._decor, self._nsplits = getter(self._extractor)
        self.sem = contextlib.nullcontext()

        if not (_proxy := self.args.proxy) and self.args.enproxy:
            with AsyncARIA2CDownloader._LOCK:
                if not AsyncARIA2CDownloader._HOSTS_DL.get(self._host):
                    _seq = random.sample(range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST)
                    queue_ = put_sequence(asyncio.Queue(), _seq)
                    AsyncARIA2CDownloader._HOSTS_DL.update({self._host: {"count": 0, "queue": queue_}})
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
            _nsplits = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
            if _nsplits < self.n_workers:
                self.n_workers = _nsplits

        opts_dict = {
            "split": self.n_workers,
            "header": "\n".join([f"{key}: {value}" for key, value in self.headers.items()]),
            "dir": str(self.download_path),
            "out": self.filename.name,
            "uri-selector": "inorder",
            "min-split-size": CONF_ARIA2C_MIN_SIZE_SPLIT,
            "dry-run": "false",
        }

        if _proxy:
            opts_dict["all-proxy"] = _proxy

        self.config_client(opts_dict)

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
            self, coro: Union[Coroutine, asyncio.Task], *,
            name: Optional[str] = None) -> asyncio.Task:

        if not isinstance(coro, asyncio.Task):
            _task = asyncio.create_task(coro, name=name)
        else:
            _task = coro

        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    def config_client(self, opts_dict):
        self.opts = AsyncARIA2CDownloader.aria2_API.get_global_options()
        for key, value in opts_dict.items():
            values = variadic(value)
            for _value in values:
                returncode = self.opts.set(key, _value)
                if not returncode:
                    logger.warning(f"{self.premsg} couldnt set [{key}] to [{value}]")

    def _get_filesize(self, uris, proxy=None) -> Optional[tuple]:
        logger.debug(f"{self.premsg}[get_filesize] start aria2dl dry-run")

        opts_dict = {
            "header": "\n".join([f"{key}: {value}" for key, value in self.headers.items()]),
            "dry-run": "true",
            "dir": str(self.download_path),
            "out": self.filename.name,
        }
        if proxy:
            opts_dict["all-proxy"] = proxy
        opts = AsyncARIA2CDownloader.aria2_API.get_global_options()
        for key, value in opts_dict.items():
            values = variadic(value)
            for _value in values:
                opts.set(key, _value)

        filesize = None
        downsize = None
        try:
            with self.sem:
                with self._decor:
                    _res = AsyncARIA2CDownloader.aria2_API.add_uris(uris, options=opts)

                start = time.monotonic()

                while time.monotonic() - start < 10:
                    time.sleep(1)
                    _res.update()
                    if filesize := int(_res.total_length):
                        downsize = int(_res.completed_length)
                        break
                AsyncARIA2CDownloader.aria2_API.remove([_res])
                logger.debug(f"{self.premsg}[get_filesize] {filesize} {downsize}")
                if filesize:
                    return (filesize, downsize)

        except Exception as e:
            logger.exception(f"{self.premsg}[get_filesize] error: {str(e)}")

    async def _acall(self, func: Callable, /, *args, **kwargs):
        """
        common async executor of aria2 client requests to the server rpc
        """
        try:
            return await sync_to_async(
                func, thread_sensitive=False, executor=self.ex_dl)(*args, **kwargs)
        except RequestException as e:
            logger.warning(f"{self.premsg}[acall][{func}] error: {str(e)}")
            if "add_uris" in func.__name__:
                raise AsyncARIA2CDLErrorFatal("add uris fails") from e
            if await self.reset_aria2c():
                return {"reset": "ok"}
            return {"error": AsyncARIA2CDLErrorFatal("reset failed")}
        except aria2p.ClientException as e:
            logger.warning(f"{self.premsg}[acall][{func}] error: {str(e)}")
            if "add_uris" in func.__name__:
                raise AsyncARIA2CDLErrorFatal("add uris fails") from e
            return {"reset": "ok"}
        except Exception as e:
            logger.exception(f"{self.premsg}[acall][{func}] error: {str(e)}")
            return {"error": e}

    async def reset_aria2c(self):
        async with AsyncARIA2CDownloader._CLASSALOCK:
            try:
                AsyncARIA2CDownloader.aria2_API.get_stats()
                logger.info(f"{self.premsg}[reset_aria2c] test conn ok")
                return True
            except Exception:
                logger.info(f"{self.premsg}[reset_aria2c] test conn no ok, lets reset aria2c")
                res, _port = try_get(self._vid_dl.nwsetup.reset_aria2c(), lambda x: x if x else None) or (None, None)
                logger.info(f"{self.premsg}[reset_aria2c] {_port} {res}")
                if _port:
                    AsyncARIA2CDownloader.aria2_API.client.port = _port
                try:
                    AsyncARIA2CDownloader.aria2_API.get_stats()
                    logger.info(f"{self.premsg}[reset_aria2c] after reset, test conn ok")
                    return True
                except Exception:
                    logger.info(f"{self.premsg}[reset_aria2c]  test conn no ok")

    async def async_pause(
            self, list_dl: list[Optional[aria2p.Download]]) -> Optional[list[OperationResult]]:

        if list_dl:
            return cast(list[OperationResult], await self._acall(
                AsyncARIA2CDownloader.aria2_API.pause, list_dl))

    async def async_resume(
            self, list_dl: list[Optional[aria2p.Download]]) -> Optional[list[OperationResult]]:

        if list_dl:
            async with self._decor:
                return cast(list[OperationResult], await self._acall(
                    AsyncARIA2CDownloader.aria2_API.resume, list_dl))

    async def async_remove(
            self, list_dl: list[Optional[aria2p.Download]]) -> Optional[list[OperationResult]]:

        if list_dl:
            return cast(list[OperationResult], await self._acall(
                AsyncARIA2CDownloader.aria2_API.remove, list_dl, clean=False))

    async def async_restart(
            self, list_dl: list[Optional[aria2p.Download]]) -> Optional[list[OperationResult]]:

        if list_dl:
            return cast(list[OperationResult], await self._acall(
                AsyncARIA2CDownloader.aria2_API.remove, list_dl, files=True, clean=True))

    async def add_uris(self, uris: list[str]) -> Optional[aria2p.Download]:
        async with self._decor:
            return cast(aria2p.Download, await self._acall(
                AsyncARIA2CDownloader.aria2_API.add_uris, uris, options=self.opts))

    async def aupdate(self, dl_cont: Optional[aria2p.Download]):
        if dl_cont:
            return await self._acall(dl_cont.update)

    def uptpremsg(self):
        _upt = f"{self.premsg} host: {self._host}"
        if self._mode != "noproxy":
            _upt += f"proxy: {self._proxy} count: {AsyncARIA2CDownloader._HOSTS_DL[self._host]['count']}"
        return _upt

    async def init(self):
        dl_cont = None
        try:
            if dl_cont := await self.add_uris(self.uris):
                if resupt := await self.aupdate(dl_cont):
                    if any(_ in resupt for _ in ("error", "reset")):
                        raise AsyncARIA2CDLError("init: error update dl_cont")

            return dl_cont

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if dl_cont and dl_cont.status == "error":
                _msg_error = f"{str(e)} - {dl_cont.error_message}"
            else:
                _msg_error = str(e)
            self.error_message = _msg_error
            logger.exception(f"{self.uptpremsg()}[init] error: {_msg_error}")
            self.status = "error"

    async def get_uri(self, _init_url, _proxy_port: int, n: int = 0, simple=False):

        _proxy = f"http://127.0.0.1:{_proxy_port}"
        logger.debug(f"{self.premsg} proxy ip{n} {_proxy}")

        try:
            async with myYTDL(
                params=self.ytdl.params, silent=True, proxy=_proxy, executor=self.ex_dl
            ) as proxy_ytdl:
                proxy_info = get_format_id(
                    proxy_ytdl.sanitize_info(await proxy_ytdl.async_extract_info(_init_url)),
                    self.info_dict["format_id"])

            _url = cast(str, traverse_obj(proxy_info, ("url", {unquote})))

            logger.debug(f"{self.premsg} ip{n}{_proxy} uri{n} {_url}")

            if not _url:
                raise AsyncARIA2CDLError("couldnt get video url")

            if simple:
                return _url

            _url_as_dict = urlparse(_url)._asdict()
            _dom = re.sub(r"^(www.)", "", f"{_url_as_dict['netloc']}")
            _url_as_dict["netloc"] = f"asyncdlrouting{_proxy_port}.{_dom}"
            return cast(str, urlunparse(list(_url_as_dict.values())))

        except Exception as e:
            _msg = f"host[{self._host}]pr[{n}:{_proxy}]{str(e)}"
            logger.debug(
                f"{self.premsg}[{self.info_dict.get('original_url')}] " +
                f"ERROR init uris {_msg}")
            raise

    async def update_uri(self):
        _init_url = self.info_dict.get("webpage_url")
        if self.special_extr:
            _init_url = smuggle_url(_init_url, {"indexdl": self.pos})

        if self._mode == "noproxy":
            if self.n_rounds > 1:
                async with myYTDL(
                        params=self.ytdl.params, silent=True, executor=self.ex_dl) as ytdl:
                    _info = get_format_id(
                        ytdl.sanitize_info(await ytdl.async_extract_info(_init_url)),
                        self.info_dict["format_id"])

                if _url := _info.get("url"):
                    video_url = unquote(_url)
                    self.uris = [video_url]
                    if _cookie := _info.get("cookies"):
                        self.headers.update({"Cookie": _cookie})
                        self.opts.set("header", "\n".join(
                            [f"{key}: {value}" for key, value in self.headers.items()]))

                    if (_host := get_host(video_url, shorten=self._extractor)) != self._host:
                        self._host = _host
                        if isinstance(self.sem, LockType):
                            async with async_lock(self.ytdl.params["lock"]):
                                self.sem = cast(
                                    LockType, self.ytdl.params["sem"].setdefault(self._host, Lock()))
                else:
                    raise AsyncARIA2CDLError("couldnt get video url")

        else:
            _res = await async_waitfortasks(
                AsyncARIA2CDownloader._HOSTS_DL[self._host]["queue"].get(),
                events=(self._vid_dl.reset_event, self._vid_dl.stop_event),
                background_tasks=self.background_tasks)

            if "event" in _res:
                return
            if (_temp := _res.get("exception")) or (_temp := _res.get("result")) is None:
                raise AsyncARIA2CDLError(f"couldnt get index proxy: {repr(_temp)}")

            self._index_proxy = cast(int, _temp)
            async with self._ALOCK():
                AsyncARIA2CDownloader._HOSTS_DL[self._host]["count"] += 1

            _uris: list[str] = []

            if self._mode == "simple":
                _proxy_port = CONF_PROXIES_BASE_PORT + self._index_proxy * 100
                self._proxy = f"http://127.0.0.1:{_proxy_port}"
                self.opts.set("all-proxy", self._proxy)

                try:
                    _proxy_info_url = await self.get_uri(
                        _init_url, _proxy_port, simple=True)
                    _uris.append(_proxy_info_url)

                except Exception as e:
                    logger.warning(f"{self.uptpremsg()} [update_uri] {str(e)}")
                    raise AsyncARIA2CDLError("couldnt get uris") from e

            elif self._mode == "group":
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
                    "".join([
                        f"{self.premsg}enproxy[{self.args.enproxy}]mode[{self._mode}]",
                        f"proxy[{self._proxy}]_gr[{_gr}]n_workers[{self.n_workers}]"
                    ]))

                _tasks = [
                    self.add_task(
                        self.get_uri(_init_url, CONF_PROXIES_BASE_PORT + self._index_proxy * 100 + j, n=j),
                        name=f"{self.premsg}[update_uri][{j}]")
                    for j in range(1, _gr + 1)
                ]

                _res = await async_waitfortasks(
                    _tasks, events=(self._vid_dl.reset_event, self._vid_dl.stop_event),
                    background_tasks=self.background_tasks)

                logger.debug(f"{self.premsg} {_res}")

                if _res.get("event"):
                    return
                if (_temp := _res.get("exception")) or not (_temp := _res.get("result")):
                    raise AsyncARIA2CDLError(f"couldnt get uris: {str(_temp)}")
                _uris.extend(cast(list[str], variadic(_temp)))

            self.uris = _uris

    async def check_speed(self):

        def len_ap_list(_list, _eltoap):
            _list.append(_eltoap)
            return len(_list)

        # def getter(x):
        #     if x <= 2:
        #         return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 0.25
        #     if x <= (self.n_workers // 2):
        #         return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 1.25
        #     return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 5

        # def perc_below(_list, _max):
        #     total = len(_list)
        #     _below = sum(1 for el in _list if el[0] < getter(el[1]))
        #     _perc = int((_below / total) * 100)
        #     if _perc > _max:
        #         return _perc

        def _print_el(item: tuple) -> str:
            _secs = item[2].second + item[2].microsecond / 1000000
            return f"({item[2].strftime('%H:%M:')}{_secs:06.3f}, ['speed': {item[0]}, 'connec': {item[1]}])"

        _speed = []

        _index = self._n_check_speed
        _min_check = self._min_check_speed

        try:
            while True:
                _res = await async_waitfortasks(
                    self._qspeed.get(), events=self._vid_dl.stop_event,
                    background_tasks=self.background_tasks)

                if "event" in _res:
                    logger.debug(f"{self.premsg}[check_speed] stop event")
                    return
                if _temp := _res.get("exception"):
                    raise AsyncARIA2CDLError(f"error when get from qspeed: {repr(_temp)}")

                _input_speed = _res.get("result")

                if _input_speed == kill_token:
                    logger.debug(f"{self.premsg}[check_speed] {kill_token} from queue")
                    return
                if any([_input_speed is None, self.block_init, self.check_any_event_is_set()]):
                    await asyncio.sleep(0)
                    continue
                if len_ap_list(_speed, _input_speed) > _min_check:
                    _res_dl0 = False
                    _res_ncon = False
                    _res_perc = False

                    if any([
                            _res_dl0 := all(el[0] == 0 for el in _speed[-_index:]),
                            _res_ncon := all([
                                self.n_workers > 1,
                                all(el[1] < self.n_workers - 1 for el in _speed[-_index:])
                            ])
                            #  _res_perc := perc_below(_speed[-_index:], 80)
                    ]):

                        logger.info(
                            "".join([
                                f"{self.premsg}[check_speed] speed reset: n_el_speed[{len(_speed)}] ",
                                f"dl0[{_res_dl0}] ncon[{_res_ncon}] perc[{_res_perc}]"
                            ]))

                        _str_speed = ", ".join([_print_el(el) for el in _speed[-_index:]])
                        logger.debug(f"{self.premsg}[check_speed]\n{_str_speed}")

                        await self._reset()
                        _speed = []
                        await asyncio.sleep(0)

                    else:
                        _speed[0:_min_check - _index // 2 + 1] = ()
                        await asyncio.sleep(0)
                else:
                    await asyncio.sleep(0)

        except Exception as e:
            logger.exception(f"{self.premsg}[check_speed] {str(e)}")
        finally:
            logger.debug(f"{self.premsg}[check_speed] bye")

    def check_any_event_is_set(self, incpause=True):
        _events = [self._vid_dl.reset_event, self._vid_dl.stop_event]
        if incpause:
            _events.append(self._vid_dl.pause_event)
        return [_ev.name for _ev in _events if _ev.is_set()]

    async def event_handle(self):
        _res = {}
        if self._vid_dl.pause_event.is_set():
            await self.async_pause([self.dl_cont])
            await asyncio.sleep(0)
            _res = await async_wait_for_any([
                self._vid_dl.resume_event, self._vid_dl.reset_event,
                self._vid_dl.stop_event])
            await self.async_resume([self.dl_cont])
            self._vid_dl.pause_event.clear()
            self._vid_dl.resume_event.clear()
            if "resume" in _res["event"]:
                _res["event"].remove("resume")
                if not _res["event"]:
                    _res = {}
            await asyncio.sleep(0)
            self.progress_timer.reset()
        else:
            if _event := self.check_any_event_is_set(incpause=False):
                _res = {"event": _event}
                self.progress_timer.reset()

        if _event := _res.get("event"):
            if "stop" in _event:
                self.status = "stop"
            if "reset" in _event:
                self._vid_dl.reset_event.clear()

            await asyncio.sleep(0)
            await self.async_remove([self.dl_cont])
            await asyncio.sleep(0)

        return _res

    async def error_handle(self, error):
        tout = None
        await self.async_remove([self.dl_cont])
        if error == "471":
            tout = 30
        elif error == "403":
            tout = 60
        _res = await async_wait_for_any(
            [self._vid_dl.stop_event, self._vid_dl.reset_event], timeout=tout)
        if _event := _res.get("event"):
            if "stop" in _event:
                self.status = "stop"
            if "reset" in _event:
                self._vid_dl.reset_event.clear()
        await asyncio.sleep(0)

    async def _reset(self, cause: Union[str, None] = None):
        if self.status == "downloading":
            if not self._vid_dl.reset_event.is_set():
                self._vid_dl.reset_event.set(cause)
            else:
                self._vid_dl.reset_event.set(cause)

    async def fetch(self):

        async def _update_counters(_bytes_dl: int):
            if (_iter_bytes := _bytes_dl - self.down_size) > 0:
                self.down_size += _iter_bytes
                self._vid_dl.total_sizes["down_size"] += _iter_bytes

        try:
            self.dl_cont = await self.init()
            self.block_init = False
            if self.status in ("done", "error", "stop"):
                return

            if not self.dl_cont:
                raise AsyncARIA2CDLErrorFatal("could get dl_cont")

            while True:
                if self.dl_cont.status not in ["active", "paused", "waiting"]:
                    break

                if self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):
                    if traverse_obj(await self.event_handle(), "event"):
                        return

                    if _result := await self.aupdate(self.dl_cont):
                        if "error" in _result:
                            raise AsyncARIA2CDLError("fetch error: error update dl_cont")
                        if "reset" in _result:
                            await self._reset()
                    else:
                        await _update_counters(self.dl_cont.completed_length)
                        if self.args.check_speed:
                            self._qspeed.put_nowait(
                                (self.dl_cont.download_speed, self.dl_cont.connections, datetime.now()))
                else:
                    await asyncio.sleep(0)

            if self.dl_cont.status == "complete":
                self.status = "done"

            elif self.dl_cont.status == "error":
                error_code = try_get(
                    re.findall(r"(?:status|estado)=(\d\d\d)", cast(str, self.dl_cont.error_message)),
                    lambda x: x[0] if x else None)
                if error_code and error_code in ("471", "403"):
                    logger.warning(f"{self.uptpremsg()}[fetch] error handle: {error_code}")
                    await self.error_handle(error_code)
                else:
                    raise AsyncARIA2CDLError("fetch error")

        except BaseException as e:
            _msg_error = str(e)
            if self.dl_cont and self.dl_cont.status == "error":
                _msg_error += f" - {self.dl_cont.error_message}"

            logger.error(f"{self.uptpremsg()}[fetch] error: {_msg_error}")
            self.status = "error"
            self.error_message = _msg_error
            if isinstance(e, KeyboardInterrupt):
                raise

    async def fetch_async(self):

        self.progress_timer.reset()
        if self.args.check_speed:
            check_task = self.add_task(
                self.check_speed(), name=f"{self.premsg}[check_task]")
        else:
            check_task = None

        try:
            while True:
                self.n_rounds += 1
                self.dl_cont = None
                self.block_init = True
                self._index_proxy = -1
                self._proxy = None
                self.status = "downloading"
                try:
                    await self.update_uri()
                    logger.debug(f"{self.uptpremsg()} uris:\n{self.uris}")
                    async with async_lock(self.sem):
                        await self.fetch()
                    if self.status in ("done", "error", "stop"):
                        return

                except BaseException as e:
                    _msg_error = str(e)
                    if self.dl_cont and self.dl_cont.status == "error":
                        _msg_error += f" - {self.dl_cont.error_message}"

                    logger.exception(
                        f"{self.uptpremsg()}[fetch_async] error: {_msg_error}")
                    self.status = "error"
                    self.error_message = _msg_error
                    if isinstance(e, KeyboardInterrupt):
                        raise
                    return

                finally:
                    if all([self._mode != "noproxy", getattr(self, "_index_proxy", -1) != -1]):
                        async with self._ALOCK():
                            AsyncARIA2CDownloader._HOSTS_DL[self._host]["count"] -= 1
                            AsyncARIA2CDownloader._HOSTS_DL[self._host]["queue"].put_nowait(self._index_proxy)
                    await asyncio.sleep(0)

        except BaseException as e:
            logger.exception(f"{self.premsg}[fetch_async] {str(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:
            if check_task:
                self._qspeed.put_nowait(kill_token)
                await asyncio.wait([check_task])

            logger.debug(f"{self.premsg}[fetch_async] exiting")

    def print_hookup(self):
        msg = ""
        _pre = f'[ARIA2C][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}]'
        _pre2 = "".join([
            f'{naturalsize(self.down_size, format_=".2f")} ',
            f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'])

        if self.status == "done":
            msg = f'{_pre} Completed\n'
        elif self.status == "init":
            msg = f'{_pre} Waiting {_pre2}'
        elif self.status == "error":
            msg = f'{_pre} ERROR {_pre2}'
        elif self.status == "stop":
            msg = f'{_pre} STOPPED {_pre2}'
        elif self.status == "downloading":
            if (all([not self.block_init, not self._vid_dl.reset_event.is_set(),
                     not self._vid_dl.stop_event.is_set(), not self._vid_dl.pause_event.is_set(),
                    self.dl_cont])):

                _temp = cast(aria2p.Download, copy.deepcopy(self.dl_cont))
                _speed_str = f"{naturalsize(_temp.download_speed, binary=True)}ps"
                _progress_str = f"{_temp.progress:.0f}%"
                self.last_progress_str = _progress_str
                _connections = _temp.connections
                _eta_str = _temp.eta_string()
                _pre3 = "".join([
                    f"CONN[{_connections:2d}/{self.n_workers:2d}] ",
                    f"DL[{_speed_str}] PR[{_progress_str}] ETA[{_eta_str}]\n"])

                msg = f'{_pre} {_pre3}'

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

                msg = f'{_pre} {_substr} DL PR[{self.last_progress_str}]\n'

        elif self.status == "manipulating":
            msg = f'{_pre} Ensambling {_pre2}'

        return msg
