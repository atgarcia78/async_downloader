import argparse
import asyncio
import contextlib
import copy
import functools
import json
import logging
import logging.config
import os
import queue
import random
import re
import selectors
import shlex
import shutil
import signal
import subprocess
import sys
import termios
import threading
import time
import urllib.parse
from _thread import LockType
from bisect import bisect
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from concurrent.futures import wait as wait_thr
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib.machinery import SOURCE_SUFFIXES, FileFinder, SourceFileLoader
from importlib.util import module_from_spec
from ipaddress import ip_address
from itertools import zip_longest
from operator import getitem
from pathlib import Path
from queue import Empty, Queue
from statistics import median
from typing import (
    Callable,
    Coroutine,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)
from urllib.parse import urlparse

import httpx
from asgiref.sync import sync_to_async
from selenium.webdriver import Firefox

FileLock = None
try:
    from filelock import FileLock
except Exception:
    print("PLEASE INSTALL filelock")

try:
    import proxy
except Exception:
    print("PLEASE INSTALL proxy")
    proxy = None

try:
    import xattr
except Exception:
    print("PLEASE INSTALL xattr")
    xattr = None

try:
    from tabulate import tabulate
except Exception:
    tabulate = None


try:
    import psutil
    import PySimpleGUI
except Exception:
    PySimpleGUI = None

try:
    import yt_dlp
except Exception:
    yt_dlp = None

# ***********************************+
# ************************************

MAXLEN_TITLE = 150

PATH_LOGS = Path(Path.home(), "Projects/common/logs")

drm_base_path = Path(
    Path.home(),
    'Projects/dumper/key_dumps/Android Emulator 5554/private_keys/7283/2049378471')

CONF_DRM = {
    "private_key": Path(drm_base_path, 'private_key.pem'),
    "client_id": Path(drm_base_path, 'client_id.bin')
}

CONF_DASH_SPEED_PER_WORKER = 102400

CONF_FIREFOX_PROFILE = "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/b33yk6rw.selenium"
CONF_FIREFOX_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0"
CONF_HLS_SPEED_PER_WORKER = 102400 / 8  # 512000
CONF_HLS_RESET_403_TIME = 150
CONF_TORPROXIES_HTTPPORT = 7070
CONF_PROXIES_MAX_N_GR_HOST = 10  # 10
CONF_PROXIES_N_GR_VIDEO = 8  # 8
CONF_PROXIES_BASE_PORT = 12000

CONF_ARIA2C_MIN_SIZE_SPLIT = 1048576  # 1MB 10485760 #10MB
CONF_ARIA2C_SPEED_PER_CONNECTION = 102400  # 102400 * 1.5# 102400
CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED = _min = 240  # 240, 120

CONF_ARIA2C_N_CHUNKS_CHECK_SPEED = _min // 4  # 60
CONF_ARIA2C_TIMEOUT_INIT = 20
CONF_INTERVAL_GUI = 0.2

CONF_ARIA2C_EXTR_GROUP = ["doodstream", "tubeload", "redload", "highload", "embedo", "streamsb", "mixdrop"]
CONF_AUTO_PASRES = ["doodstream"]
CONF_PLAYLIST_INTERL_URLS = [
    # "GVDBlogPlaylist",
    "MyVidsterChannelPlaylistIE",
    "MyVidsterSearchPlaylistIE",
    "MyVidsterRSSPlaylistIE",
]

CONF_HTTP_DL = {
    "ARIA2C": {
        "extractors": ["mixdrop", "hungyoungbrit", "doodstream"],  # ['doodstream']
        "max_filesize": 300000000,
    }
}

CLIENT_CONFIG = {
    "timeout": httpx.Timeout(timeout=20),
    "limits": httpx.Limits(
        max_connections=None, max_keepalive_connections=None, keepalive_expiry=5.0),
    "headers": {
        "User-Agent": CONF_FIREFOX_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en,es-ES;q=0.5",
        "Sec-Fetch-Mode": "navigate",
    },
    "follow_redirects": True,
    "verify": False,
}


class AsyncDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)

        self.exc_info = exc_info


class AsyncDLError(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)

        self.exc_info = exc_info


class AsyncDLSTOP(Exception):
    pass


async def get_list_interl(entries, asyncdl, _pre):

    logger = logging.getLogger('interl')

    _temp_aldl = []
    res = []

    for _ent in entries:
        if not await asyncdl.async_check_if_aldl(_ent, test=True):
            res.append(_ent)
        else:
            _temp_aldl.append(_ent)

    def mix_lists(_http_list, _hls_list, _asyncdl):
        _final_list = []
        if _hls_list:
            if not _http_list:
                _final_list = _hls_list
            else:
                iters = len(_hls_list)
                step = int(len(_http_list) / iters) + 1
                _final_list = []
                for idx in range(iters):
                    start = step * idx
                    end = step * (idx + 1)
                    _final_list.extend(_http_list[start:end])
                    _final_list.append(_hls_list[idx])
        else:
            _final_list = _http_list

        if (_total := len(_final_list)) > 0:
            for _newidx, _el in enumerate(_final_list):
                _el["__interl_index"] = _asyncdl.max_index_playlist + _newidx + 1
                _el["__interl_total"] = _total

            _asyncdl.max_index_playlist += _total

        return _final_list

    def get_dif_interl(_dict, _interl, workers):
        """
        get dif in the interl list if distance of elements
        with same host is less than num runners dl workers of asyncdl
        """
        dif = defaultdict(lambda: [])
        for host, group in _dict.items():
            index_old = None
            _group = sorted(group, key=lambda x: _interl.index(x))
            for el in _group:
                index = _interl.index(el)
                if index_old and index - index_old < workers:
                    dif[host].append(el)
                index_old = index
        return dif

    if not res:
        return _temp_aldl
    if len(res) < 3:
        return res + _temp_aldl
    _dict = defaultdict(lambda: [])
    _hls_list = []
    _res = []
    for ent in res:
        if "hls" not in ent["format_id"]:
            _res.append(ent)
            _dict[get_domain(ent["url"])].append(ent["id"])
        else:
            _hls_list.append(ent)

    if not _dict:
        return mix_lists([], _hls_list, asyncdl) + _temp_aldl

    logger.info(
        f"{_pre}[get_list_interl] entries"
        + f"interleave: {len(list(_dict.keys()))} different hosts, "
        + f"longest with {len(max(list(_dict.values()), key=len))} entries")

    _workers = asyncdl.workers
    _interl = []
    while _workers > asyncdl.workers // 2:
        _interl = []
        for el in list(zip_longest(*list(_dict.values()))):
            _interl.extend([_el for _el in el if _el])

        for tunein in range(3):
            dif = get_dif_interl(_dict, _interl, _workers)

            if dif:
                if tunein < 2:
                    for i, host in enumerate(list(dif.keys())):
                        group = list(_dict[host])
                        for j, el in enumerate(group):
                            _interl.pop(_interl.index(el))
                            _interl.insert(_workers * (j + 1) + i, el)
                    continue
                else:
                    logger.info(
                        f"{_pre}[get_list_interl] tune in NOK, try with less num of workers")
                    _workers -= 1
                    break

            else:
                logger.info(
                    f"{_pre}[get_list_interl] tune in OK, no dif with workers[{_workers}]")
                asyncdl.workers = _workers
                asyncdl.WorkersRun.max_workers = _workers
                _http_list = sorted(_res, key=lambda x: _interl.index(x["id"]))
                return mix_lists(_http_list, _hls_list, asyncdl) + _temp_aldl

    asyncdl.workers = _workers
    asyncdl.WorkersRun.max_workers = _workers
    if _interl:
        _http_list = sorted(
            _res, key=lambda x: _interl.index(x["id"]))
    else:
        _http_list = _res
    return mix_lists(_http_list, _hls_list, asyncdl) + _temp_aldl


def empty_queue(q: Union[asyncio.Queue, Queue]):
    while True:
        try:
            q.get_nowait()
            q.task_done()
        except (asyncio.QueueEmpty, Empty):
            break


def load_module(name, path: str):
    _loader_details = [(SourceFileLoader, SOURCE_SUFFIXES)]
    finder = FileFinder(path, *_loader_details)
    spec = finder.find_spec(name)
    if not spec or not spec.loader:
        raise ImportError(f"no module named {name}")
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def upartial(f, *args, **kwargs):
    """
    An upgraded version of partial which accepts not named parameters
    """
    params = f.__code__.co_varnames[1:]
    kwargs = dict(zip(params, args)) | kwargs
    return functools.partial(f, **kwargs)


def get_host(url: str, shorten=None) -> str:
    _host = re.sub(r"^www\.", "", urlparse(url).netloc)
    if shorten == "vgembed":
        _nhost = _host.split(".")
        if _host.count(".") >= 3:
            _host = ".".join(_nhost[-3:])
    return _host


def nested_obj(d, *selectors, get_all=True, default=None, v=False):
    logger = logging.getLogger("nestedobj")
    NO_RES = object()

    def is_sequence(x):
        return isinstance(x, Iterable) and not isinstance(x, (str, bytes))

    def _nested_obj(obj, sel):
        try:
            if not is_sequence(sel):
                sel = [sel]
            _res = functools.reduce(getitem, sel, obj)
            if v:
                logger.info(f"selector[{sel}]: {_res}")
            return _res
        except (IndexError, KeyError) as e:
            if v:
                logger.info(f"selector[{sel}]: error {repr(e)}")
            return NO_RES

    if v:
        logger.info(f"selectors[{selectors}]")
    res = []
    for selector in selectors:
        result = _nested_obj(d, selector)
        if result is not NO_RES:
            if get_all:
                res.append(result)
            else:
                return result
    if not res:
        return default
    else:
        return res[0] if len(res) == 1 else res


def put_sequence(q: Union[queue.Queue, asyncio.Queue], seq: Iterable) -> Union[queue.Queue, asyncio.Queue]:
    if seq:
        if queue_ := getattr(q, "queue", getattr(q, "_queue", None)):
            queue_.extend(seq)
    return q


def subnright(pattern, repl, text, n):
    pattern = re.compile(rf"{pattern}(?!.*{pattern})", flags=re.DOTALL)
    _text = text
    for _ in range(n):
        _text = pattern.sub(repl, _text)
    return _text


class classproperty(property):

    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


_NOT_FOUND = object()


class cached_classproperty(functools.cached_property):
    __slots__ = ("func", "attrname", "__doc__", "lock")

    def __init__(self, func, attrname=None):
        self.func = func
        self.attrname = attrname
        self.__doc__ = func.__doc__
        self.lock = threading.RLock()

    def __set_name__(self, owner, name):
        if self.attrname is None:
            self.attrname = name
        elif name != self.attrname:
            raise TypeError(
                "Cannot assign the same cached_property to two different names "
                f"({self.attrname!r} and {name!r})."
            )

    def __get__(self, instance, owner=None):
        if owner is None:
            raise TypeError("Cannot use cached_classproperty without an owner class.")
        if self.attrname is None:
            raise TypeError("Cannot use cached_classproperty instance without calling __set_name__ on it.")
        try:
            cache = owner.__dict__
        except AttributeError:
            msg = f"No '__dict__' attribute on {owner.__name__!r} " f"to cache {self.attrname!r} property."
            raise TypeError(msg) from None
        val = cache.get(self.attrname, _NOT_FOUND)
        if val is _NOT_FOUND or val is self:
            with self.lock:  # type: ignore
                # check if another thread filled cache while we awaited lock
                val = cache.get(self.attrname, _NOT_FOUND)
                if val is _NOT_FOUND or val is self:
                    val = self.func(owner)
                    setattr(owner, self.attrname, val)
        return val


class Cache:
    def __init__(self, app="noname"):
        self.app = app
        self.logger = logging.getLogger("cache")
        self.root_dir = os.path.join(os.getenv("XDG_CACHE_HOME") or os.path.expanduser("~/.cache"), app)
        os.makedirs(self.root_dir, exist_ok=True)

    def _get_cache_fn(self, key):
        key = urllib.parse.quote(key, safe="").replace("%", ",")  # encode non-ascii characters
        return os.path.join(self.root_dir, f"{key}.json")

    def store(self, key, obj):
        def write_json_file(obj, fn):
            with open(fn, mode="w", encoding="utf-8") as f:
                json.dump({"date": datetime.now().strftime("%Y.%m.%d"), "data": obj}, f, ensure_ascii=False)

        fn = self._get_cache_fn(key)
        try:
            write_json_file(obj, fn)
        except Exception as e:
            self.logger.exception(f"Writing cache to {fn!r} failed: {e}")

    def load(self, key, default=None):
        cache_fn = self._get_cache_fn(key)
        with contextlib.suppress(OSError):
            try:
                with open(cache_fn, encoding="utf-8") as cachef:
                    self.logger.info(f"Loading {key} from cache")
                    return json.load(cachef).get("data")
            except (ValueError, KeyError):
                try:
                    file_size = os.path.getsize(cache_fn)
                except OSError as oe:
                    file_size = str(oe)
                self.logger.warning(f"Cache retrieval from {cache_fn} failed ({file_size})")

        return default


class MySyncAsyncEvent:
    def __init__(self, name: Optional[str] = None, initset: bool = False):
        if name:
            self.name = name
        self._cause = None
        self.event = threading.Event()
        self.aevent = asyncio.Event()
        self._flag = False
        if initset:
            self.set()

    def set(self, cause: Optional[str] = None):
        self.aevent.set()
        self.event.set()
        self._flag = True
        self._cause = cause or 'set_with_no_cause'

    def is_set(self) -> Optional[str]:
        """
        Return cause(true if cause is none) if
        and only if the internal flag is true.
        """

        if self._flag:
            return self._cause

    def clear(self):
        self.aevent.clear()
        self.event.clear()
        self._flag = False
        self._cause = None

    def wait(self, timeout: Optional[float] = None) -> bool:
        return True if self._flag else self.event.wait(timeout=timeout)

    async def async_wait(self, timeout: Optional[float] = None) -> dict:

        if self._flag:
            return {"cause": self._cause}
        try:
            await asyncio.wait_for(
                self.aevent.wait(), timeout=timeout)
            return {"event": self.name}
        except asyncio.TimeoutError:
            return {"timeout": timeout}

    def add_task(self, timeout: Optional[float] = None) -> asyncio.Task:
        return asyncio.create_task(
            self.async_wait(timeout=timeout), name=self.name)

    def __repr__(self):
        cls = self.__class__
        status = f"set, cause: {self._cause}" if self._flag else "unset"
        _res = f"<{cls.__module__}.{cls.__qualname__} at {id(self):#x}: {status}"
        _res += f"\n\tname: {self.name if hasattr(self, 'name') else 'noname'}"
        _res += f"\n\tsync event: {repr(self.event)}\n\tasync event: {repr(self.aevent)}\n>"
        return _res


class ProgressTimer:
    TIMER_FUNC = time.monotonic

    def __init__(self):
        self._last_ts = self.TIMER_FUNC()

    def __repr__(self):
        return f"{self.elapsed_seconds():.2f}"

    def reset(self):
        self._last_ts = self.TIMER_FUNC()

    def elapsed_seconds(self) -> float:
        return self.TIMER_FUNC() - self._last_ts

    def has_elapsed(self, seconds: float) -> bool:
        if seconds <= 0.0:
            return False
        elapsed_seconds = self.elapsed_seconds()
        if elapsed_seconds < seconds:
            return False

        self._last_ts += elapsed_seconds - elapsed_seconds % seconds
        return True

    def wait_haselapsed(self, seconds: float):
        while True:
            if self.has_elapsed(seconds):
                return True
            else:
                time.sleep(0.2)


class EMA:
    """
    Exponential moving average: smoothing to give progressively lower
    weights to older values.

    Parameters
    ----------
    smoothing  : float, optional
        Smoothing factor in range [0, 1], [default: 0.3].
        Increase to give more weight to recent values.
        Ranges from 0 (yields old value) to 1 (yields new value).
    """

    def __init__(self, smoothing=0.3):
        self.alpha = smoothing
        self.last = 0
        self.calls = 0

    def reset(self):
        self.last = 0
        self.calls = 0

    def __call__(self, x=None):
        """
        Parameters
        ----------
        x  : float
            New value to include in EMA.
        """
        beta = 1 - self.alpha
        if x is not None:
            self.last = self.alpha * x + beta * self.last
            self.calls += 1
        return self.last / (1 - beta**self.calls) if self.calls else self.last


class SpeedometerMA:
    TIMER_FUNC = time.monotonic

    def __init__(
        self, initial_bytes: int = 0,
        upt_time: Union[int, float] = 1.0, ave_time: Union[int, float] = 5.0,
        smoothing: float = 0.3
    ):
        self.initial_bytes = initial_bytes
        self.rec_bytes = 0
        self.ts_data = [(self.TIMER_FUNC(), initial_bytes)]
        self.timer = ProgressTimer()
        self.last_value = None
        self.UPDATE_TIMESPAN_S = float(upt_time)
        self.AVERAGE_TIMESPAN_S = float(ave_time)
        self.ema_value = (lambda x: x) if smoothing < 0 else EMA(smoothing=smoothing)

    def __call__(self, byte_counter: int):
        time_now = self.TIMER_FUNC()
        self.rec_bytes = byte_counter - self.initial_bytes
        # only append data older than 50ms
        if time_now - self.ts_data[-1][0] > 0.05:
            self.ts_data.append((time_now, byte_counter))

        # remove older entries
        idx = max(0, bisect(self.ts_data, (time_now - self.AVERAGE_TIMESPAN_S,)) - 1)
        self.ts_data[:idx] = ()

        diff_time = time_now - self.ts_data[0][0]
        speed = (byte_counter - self.ts_data[0][1]) / diff_time if diff_time else None
        if self.timer.has_elapsed(seconds=self.UPDATE_TIMESPAN_S):
            self.last_value = speed

        return self.ema_value(self.last_value or speed)

    def reset(self, initial_bytes: int = 0):
        self.ts_data = [(self.TIMER_FUNC(), initial_bytes)]
        self.timer = ProgressTimer()
        self.last_value = None
        self.ema_value = EMA(smoothing=0.3)


class SmoothETA:
    def __init__(self):
        self.last_value = None

    def __call__(self, value: float):
        if value <= 0:
            return 0

        time_now = time.monotonic()
        if self.last_value:
            predicted = cast(float, self.last_value - time_now)
            if predicted <= 0:
                deviation = float("inf")
            else:
                deviation = max(predicted, value) / min(predicted, value)

            if deviation < 1.25:
                return predicted

        self.last_value = time_now + value
        return value

    def reset(self):
        self.last_value = None


class SignalHandler:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print(signum)
        print("Exiting gracefully")
        self.KEEP_PROCESSING = False


class long_operation_in_thread:
    """
    decorator to run a sync function from sync context in
    a non blocking thread. The func with this decorator returns without blocking
    a mysynasyncevent to stop the execution of the func in the
    thread
    """

    def __init__(self, name: str) -> None:
        self.name = name  # name of thread for logging

    def __call__(self, func):
        name = self.name

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> MySyncAsyncEvent:
            stop_event = MySyncAsyncEvent(name)
            thread = threading.Thread(
                target=func, name=name, args=args,
                kwargs={"stop_event": stop_event, **kwargs},
                daemon=True)
            thread.start()
            return stop_event

        return wrapper


class run_operation_in_executor:
    """
    decorator to run a sync function from sync context
    The func with this decorator returns without blocking
    a mysynasyncevent to stop the execution of the func, and a future
    that wrappes the function submitted with a thread executor
    """

    def __init__(self, name: str) -> None:
        self.name = name  # for thread prefix loggin and stop event name

    def __call__(self, func):
        name = self.name

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> tuple[MySyncAsyncEvent, Future]:
            stop_event = MySyncAsyncEvent(name)
            exe = ThreadPoolExecutor(thread_name_prefix=name)
            _kwargs = {"stop_event": stop_event}
            _kwargs.update(kwargs)
            fut = exe.submit(lambda: func(*args, **_kwargs))
            return (stop_event, fut)

        return wrapper


class run_operation_in_executor_from_loop:
    """
    decorator to run a sync function from asyncio loop
    with the use loop.run_in_executor method.
    The func with this decorator returns without blocking
    a mysynasyncevent to stop the execution of the func, and a future/task
    that wrappes the function
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, func):
        name = self.name

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> tuple[MySyncAsyncEvent, asyncio.Task]:
            stop_event = MySyncAsyncEvent(name)
            kwargs["stop_event"] = stop_event
            _task = asyncio.create_task(
                sync_to_async(
                    func, thread_sensitive=False,
                    executor=ThreadPoolExecutor(thread_name_prefix=name)
                )(*args, **kwargs),
                name=name)
            return (stop_event, _task)

        return wrapper


############################################################
# """                     SYNC ASYNC                     """
############################################################


class async_suppress(contextlib.AbstractAsyncContextManager):
    def __init__(self, *exceptions):
        self._exceptions = exceptions

    async def __aenter__(self):
        pass

    async def __aexit__(self, exctype, excinst, exctb):
        return exctype is not None and issubclass(exctype, self._exceptions)


def add_task(coro, bktasks=None, name=None):
    if not isinstance(coro, asyncio.Task):
        _task = asyncio.create_task(coro, name=name)
    else:
        _task = coro
    if bktasks:
        bktasks.add(_task)
        _task.add_done_callback(bktasks.discard)
    return _task


def wait_for_either(ev, timeout=None):
    events = variadic(ev)
    if _res := [getattr(ev, "name", "noname") for ev in events if ev.is_set()]:
        return _res[0]

    def check_timeout(_st, _n):
        return False if _n is None else time.monotonic() - _st >= _n

    start = time.monotonic()
    while True:
        if _res := [getattr(ev, "name", "noname") for ev in events if ev.is_set()]:
            return _res[0]
        elif check_timeout(start, timeout):
            return "TIMEOUT"
        time.sleep(CONF_INTERVAL_GUI)


async def await_for_any(
        events: Union[MySyncAsyncEvent, Iterable[MySyncAsyncEvent]],
        timeout: Optional[int] = None) -> dict:

    _events = cast(Iterable[MySyncAsyncEvent], variadic(events))

    if _res := [getattr(_ev, "name", "noname")
                for _ev in _events if _ev and _ev.is_set()]:

        return {"event": _res}

    _tasks_events = {event.add_task(): f'{event.name}' for event in _events}

    done, pending = await asyncio.wait(
        _tasks_events, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)

    if pending:
        list(map(lambda x: x.cancel(), pending))
        await asyncio.wait(pending)
    if not done:
        return {"timeout": timeout}
    _done = done.pop()
    return _done.result()


async def async_wait_for_any(events, timeout: Optional[float] = None) -> dict[str, list[str]]:
    _events = variadic(events)

    if _res := [getattr(_ev, "name", "noname") for _ev in _events if _ev and _ev.is_set()]:
        return {"event": _res}

    def check_timeout(_st, _n):
        return False if _n is None else time.monotonic() - _st >= _n

    start = time.monotonic()
    while True:
        if _res := [cast(str, getattr(_ev, "name", "noname"))
                    for _ev in _events if _ev and _ev.is_set()]:
            return {"event": _res}
        elif check_timeout(start, timeout):
            return {"timeout": [str(timeout)]}
        await asyncio.sleep(CONF_INTERVAL_GUI / 2)


async def async_waitfortasks(
    fs: Optional[Iterable | Coroutine | asyncio.Task] = None,
    timeout: Optional[float] = None,
    events: Optional[Iterable | asyncio.Event | MySyncAsyncEvent] = None,
    wait_pending_tasks: bool = True,
    get_results: Union[str, bool] = "discard_if_condition",
    **kwargs,
) -> Optional[dict[str, dict | Iterable | bool]]:

    """
    si timeout y se llega al timeout en el asyncio.wait,
    se devuelven las tasks en pending cancelled

    """

    _final_wait = {}
    _tasks: dict[asyncio.Task, str] = {}
    _tasks_events = {}
    _one_task_to_wait_tasks = None

    _background_tasks = kwargs.get("background_tasks", set())

    if fs:
        listfs = cast(Iterable, variadic(fs))

        for _fs in listfs:
            if not isinstance(_fs, asyncio.Task):
                _tasks[
                    add_task(
                        _fs,
                        bktasks=_background_tasks,
                        name=f"_entry_fs_{_fs.__name__}",
                    )
                ] = "task"
            else:
                _tasks[_fs] = "task"

        _one_task_to_wait_tasks = add_task(
            asyncio.wait(_tasks, return_when=asyncio.ALL_COMPLETED),
            bktasks=_background_tasks, name="fs_list")

        _final_wait[_one_task_to_wait_tasks] = "tasks"

    if events:
        _events = cast(Iterable, variadic(events))

        def getter(ev):
            return getattr(ev, "name", "noname")

        for event in _events:
            if isinstance(event, asyncio.Event):
                _tasks_events[
                    add_task(
                        event.wait(),
                        bktasks=_background_tasks,
                        name=f"{getter(event)}",
                    )
                ] = f"{getter(event)}"
            elif isinstance(event, MySyncAsyncEvent):
                _tasks_events[
                    add_task(
                        event.async_wait(),
                        bktasks=_background_tasks,
                        name=f"{getter(event)}",
                    )
                ] = f"{getter(event)}"

        _final_wait |= _tasks_events

    if not _final_wait:
        if timeout:
            _task_sleep = add_task(asyncio.sleep(timeout * 2), bktasks=_background_tasks)
            _tasks[_task_sleep] = "task"
            _final_wait.update(_tasks)
        else:
            return {"timeout": "nothing to await"}

    _pending = []
    _done = []
    _done_before_condition = []
    _condition = {"timeout": False, "event": None}
    _results = []
    _cancelled = []

    done, pending = await asyncio.wait(
        _final_wait, timeout=timeout,
        return_when=asyncio.FIRST_COMPLETED)

    # aux task created and has to be cancelled
    to_cancel = []

    try:
        if not done:
            _condition["timeout"] = True
            if _tasks:
                _done_before_condition.extend(list(filter(
                    lambda x: x._state == "FINISHED", _tasks)))
                _pending.extend(list(filter(
                    lambda x: x._state == "PENDING", _tasks)))
        else:
            _task_done = done.pop()
            if "list_fs" in _task_done.get_name():
                if _tasks_events:
                    to_cancel.append(_tasks_events)
                _done.extend(_tasks)

            else:
                _condition["event"] = _task_done.get_name()
                if _one_task_to_wait_tasks:
                    to_cancel.append(_one_task_to_wait_tasks)
                to_cancel.extend(_tasks_events)
                if _tasks:
                    _done_before_condition.extend(list(filter(
                        lambda x: x._state == "FINISHED", _tasks)))
                    _pending.extend(list(filter(
                        lambda x: x._state == "PENDING", _tasks)))

        if to_cancel:
            list(map(lambda x: x.cancel(), to_cancel))
            await asyncio.wait(to_cancel)

        if _pending:
            list(map(lambda x: x.cancel(), _pending))
            if wait_pending_tasks:
                await asyncio.wait(_pending)

        if get_results:
            _get_from = _done
            if get_results != "discard_if_condition":
                _get_from += _done_before_condition

            if _get_from:
                _results.extend([_d.result() for _d in _get_from if not _d.exception()])

        return {'condition': _condition, 'results': _results, 'done': _done,
                'done_before_condition': _done_before_condition, 'pending': _pending,
                'cancelled': _cancelled}

    except Exception as e:
        logger = logging.getLogger('utils.asyncwaitfortasks')
        logger.exception(repr(e))


@contextlib.asynccontextmanager
async def async_lock(lock: Union[LockType, threading.Lock, contextlib.nullcontext, None] = None):
    if not lock or (isinstance(lock, contextlib.nullcontext)):
        try:
            yield
        finally:
            pass
    else:
        executor = ThreadPoolExecutor(thread_name_prefix="lock2async")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, lock.acquire)
        try:
            yield  # the lock is held
        finally:
            await loop.run_in_executor(executor, lock.release)


async def async_wait_time(n: Union[int, float]):
    return await async_waitfortasks(timeout=n)


def wait_time(n: Union[int, float], event: Optional[threading.Event | MySyncAsyncEvent] = None):
    _started = time.monotonic()
    if not event:
        time.sleep(n)  # dummy
        return time.monotonic() - _started
    else:
        if event.wait(timeout=n):
            return
        else:
            return time.monotonic() - _started


async def async_wait_until(timeout, cor, args, kwargs, interv=CONF_INTERVAL_GUI):
    _started = time.monotonic()
    if not cor:

        async def _cor(*args, **kwargs):
            return True

    else:
        _cor = cor

    while not (await _cor(*args, **kwargs)):
        if ((time.monotonic() - _started)) >= timeout:
            raise TimeoutError()
        else:
            await async_wait_time(interv)


def wait_until(timeout, statement, args, kwargs, interv=CONF_INTERVAL_GUI):
    _started = time.monotonic()
    if not statement:

        def func(*args, **kwargs):
            return True

    else:
        func = statement

    while not func(*args, **kwargs):
        if ((time.monotonic() - _started)) >= timeout:
            raise TimeoutError()
        else:
            time.sleep(interv)


############################################################
# """                     INIT                     """
############################################################


def init_logging(file_path=None, test=False):
    if not file_path:
        config_file = Path(Path.home(), "Projects/common/logging.json")
    else:
        config_file = Path(file_path)

    with open(config_file) as f:
        config = json.loads(f.read())

    config["handlers"]["info_file_handler"]["filename"] = config[
        "handlers"]["info_file_handler"]["filename"].format(path_logs=str(PATH_LOGS))

    logging.config.dictConfig(config)

    for log_name, _ in logging.Logger.manager.loggerDict.items():
        if log_name.startswith("proxy"):
            logger = logging.getLogger(log_name)
            logger.setLevel(logging.INFO)

    logger = logging.getLogger("proxy.http.proxy.server")
    logger.setLevel(logging.ERROR)
    logger = logging.getLogger("proxy.core.base.tcp_server")
    logger.setLevel(logging.ERROR)
    logger = logging.getLogger("proxy.http.handler")
    logger.setLevel(logging.ERROR)
    logger = logging.getLogger("plugins.proxy_pool_by_host")
    logger.setLevel(logging.ERROR)

    if test:
        return logging.getLogger("test")


class ActionNoYes(argparse.Action):
    def __init__(self, option_strings, dest, default=None, required=False, help=None):
        if len(option_strings) != 1:
            raise ValueError("Only single argument is allowed with YesNo action")
        opt = option_strings[0]
        if not opt.startswith("--"):
            raise ValueError("Yes/No arguments must be prefixed with --")
        opt = opt[2:]
        opts = [f"--{opt}", f"--no-{opt}"]
        super(ActionNoYes, self).__init__(
            opts, dest, nargs="?", const=None, default=default,
            required=required, help=help)

    def __call__(self, parser, namespace, values, option_strings=None):
        if option_strings:
            if option_strings.startswith("--no-"):
                setattr(namespace, self.dest, False)
            else:
                _val = values or True
                setattr(namespace, self.dest, _val)


def init_argparser():
    parser = argparse.ArgumentParser(
        description="Async downloader videos / playlist videos HLS / HTTP")
    parser.add_argument("-w", help="Number of DL workers", default="5", type=int)
    parser.add_argument(
        "--winit",
        help="Number of init workers, default is same number for DL workers",
        default="10",
        type=int,
    )
    parser.add_argument(
        "-p", "--parts", help="Number of workers for each DL", default="16", type=int)
    parser.add_argument(
        "--format", help="Format preferred of the video in youtube-dl format",
        default="bv*+ba/b", type=str
    )
    parser.add_argument("--sort", help="Formats sort preferred", default="ext:mp4:m4a", type=str)
    parser.add_argument("--index", help="index of a video in a playlist", default=None, type=int)
    parser.add_argument("--file", help="jsonfiles", action="append", dest="collection_files", default=[])
    parser.add_argument("--checkcert", help="checkcertificate", action="store_true", default=False)
    parser.add_argument("--ytdlopts", help="init dict de conf", default="", type=str)
    parser.add_argument("--proxy", action=ActionNoYes, default=None)
    parser.add_argument("--useragent", default=CONF_FIREFOX_UA, type=str)
    parser.add_argument("--first", default=None, type=int)
    parser.add_argument("--last", default=None, type=int)
    parser.add_argument("--nodl", help="not download", action="store_true", default=False)
    parser.add_argument("--headers", default="", type=str)
    parser.add_argument("-u", action="append", dest="collection", default=[])
    parser.add_argument(
        "--dlcaching",
        help="whether to force to check external storage or not",
        action=ActionNoYes,
        default=False,
    )
    parser.add_argument("--path", default=None, type=str)
    parser.add_argument("--caplinks", action="store_true", default=False)
    parser.add_argument("-v", "--verbose", help="verbose", action="store_true", default=False)
    parser.add_argument("--vv", help="verbose plus", action=ActionNoYes, default=False)
    parser.add_argument("-q", "--quiet", help="quiet", action="store_true", default=False)
    parser.add_argument(
        "--aria2c",
        action=ActionNoYes,
        default="6800",
        help="use of external aria2c running in port [PORT]. By default PORT=6800. Set to 'no' to disable",
    )
    parser.add_argument("--subt", action=ActionNoYes, default=True)
    parser.add_argument("--nosymlinks", action="store_true", default=False)
    parser.add_argument("--check-speed", action=ActionNoYes, default=True)
    parser.add_argument(
        "--deep-aldl",
        help="whether to enable greedy mode when checking if aldl by only taking into account 'ID'. Otherwise, will check 'ID_TITLE'",
        action=ActionNoYes,
        default=False)
    parser.add_argument("--http-downloader", choices=["native", "aria2c", "saldl"], default="aria2c")
    parser.add_argument("--use-path-pl", action="store_true", default=False)
    parser.add_argument("--use-cookies", action="store_true", default=True)
    parser.add_argument("--no-embed", action="store_true", default=False)
    parser.add_argument("--rep-pause", action="store_true", default=False)

    args = parser.parse_args()

    if args.winit == 0:
        args.winit = args.w

    if args.aria2c is False:
        args.rpcport = None

    elif args.aria2c is True:
        args.rpcport = 6800
    else:
        args.rpcport = int(args.aria2c)
        args.aria2c = True

    if args.path and len(args.path.split("/")) == 1:
        args.path = str(Path(Path.home(), "testing", args.path))

    if args.vv:
        args.verbose = True

    if args.quiet:
        args.verbose = False
        args.vv = False

    args.enproxy = True
    if args.proxy is False:
        args.enproxy = False
        args.proxy = None
    elif args.proxy is True:
        args.proxy = None

    args.nocheckcert = not args.checkcert
    return args


def get_listening_tcp() -> dict:
    """
    dict of result executing 'listening' in shell with keys:
        tcp port,
        command
    """
    printout = subprocess.run(["sudo", "_listening"], encoding="utf-8", capture_output=True).stdout
    final_list = defaultdict(list)
    for el in re.findall(r"^(\d+) (\d+) (.+)", printout, re.MULTILINE):
        final_list[el[2]].append({"port": int(el[1]), "pid": int(el[0])})
        final_list[int(el[1])].append({"pid": int(el[0]), "command": el[2]})
    return dict(final_list)


def find_in_ps(pattern, value=None):
    res = subprocess.run(
        ["ps", "-u", "501", "-x", "-o", "pid,tty,command"], encoding="utf-8", capture_output=True
    ).stdout
    mobj = re.findall(pattern, res)
    if not value or str(value) in mobj:
        return mobj


def init_aria2c(args):
    logger = logging.getLogger("asyncDL")

    _info = get_listening_tcp()
    _in_use_aria2c_ports = cast(list, traverse_obj(_info, ("aria2c", ..., "port")) or [None])
    if args.rpcport in _info:
        _port = _in_use_aria2c_ports[-1] or args.rpcport
        for n in range(10):
            args.rpcport = _port + (n + 1) * 100
            if args.rpcport not in _info:
                break
    _cmd = f"aria2c --rpc-listen-port {args.rpcport} --enable-rpc "
    _cmd += "--rpc-max-request-size=2M --rpc-listen-all --quiet=true"
    _proc = subprocess.Popen(
        _cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, shell=False
    )

    time.sleep(1)
    _proc.poll()
    time.sleep(1)
    _proc.poll()

    if _proc.returncode is not None or args.rpcport not in traverse_obj(
        get_listening_tcp(), ("aria2c", ..., "port")
    ):
        raise ValueError(f"[init_aria2c] couldnt run aria2c in port {args.rpcport} - {_proc}")

    logger.info(f"[init_aria2c] running on port: {args.rpcport}")

    return _proc


def get_httpx_client(config: Optional[dict] = None) -> httpx.Client:
    if not config:
        config = {}
    return httpx.Client(**(CLIENT_CONFIG | config))


def get_httpx_async_client(config: Optional[dict] = None) -> httpx.AsyncClient:
    if not config:
        config = {}
    return httpx.AsyncClient(**(CLIENT_CONFIG | config))


def get_driver(**kwargs) -> Optional[Firefox]:
    if kwargs.get("noheadless") is None:
        kwargs["noheadless"] = True
    if _driver := try_get(
            SeleniumInfoExtractor._get_driver(**kwargs),
            lambda x: x[0] if x else None):
        return _driver


############################################################
# """                     IP/TORGUARD                    """
############################################################


def is_ipaddr(res):
    try:
        ip_address(res)
        return True
    except Exception:
        return False


class myIP:
    URLS_API_GETMYIP = {
        "httpbin": {"url": "https://httpbin.org/get", "key": "origin"},
        "ipify": {"url": "https://api.ipify.org?format=json", "key": "ip"},
        "ipapi": {"url": "http://ip-api.com/json", "key": "query"},
    }
    CONFIG = {}
    CLIENT = None

    @classmethod
    def _set_config(cls, key, timeout=1):
        _proxies = {"all://": f"http://127.0.0.1:{key}"} if key else None
        _timeout = httpx.Timeout(timeout=timeout)
        cls.CONFIG.update({"proxies": _proxies, "timeout": _timeout})
        cls.CLIENT = get_httpx_client(config=cls.CONFIG)

    @staticmethod
    def _get_rtt(ip):
        res = subprocess.run(
            ["ping", "-c", "10", "-q", "-S", "192.168.1.128", ip],
            encoding="utf-8",
            capture_output=True,
        ).stdout
        _tavg = try_get(re.findall(r"= [^\/]+\/([^\/]+)\/", res), lambda x: float(x[0]))
        return {"ip": ip, "time": _tavg}

    @classmethod
    def get_ip(cls, api="ipify"):
        if api not in cls.URLS_API_GETMYIP:
            raise ValueError("[get_ip] api not supported")

        _urlapi = cls.URLS_API_GETMYIP[api]["url"]
        _keyapi = cls.URLS_API_GETMYIP[api]["key"]
        return try_get(cls.CLIENT.get(_urlapi), lambda x: x.json().get(_keyapi))

    @classmethod
    def get_myiptryall(cls):
        exe = ThreadPoolExecutor(thread_name_prefix="getmyip")
        try:
            futures = [exe.submit(cls.get_ip, api=api) for api in cls.URLS_API_GETMYIP]
            for el in as_completed(futures):
                if not el.exception() and is_ipaddr(_ip := el.result()):
                    return _ip
        finally:
            exe.shutdown(wait=False)

    @classmethod
    def get_myip(cls, key=None, timeout=1, tryall=True, api=None):
        """
        class method which is entry for the functionality.

        _myip = myIP.get_myip(key=12408, timeout=8)

        key is the port of the 127.0.0.1:{key} proxy. Dont set it to not use proxy
        """
        cls._set_config(key, timeout=timeout)
        try:
            if tryall:
                return cls.get_myiptryall()
            if not api:
                api = random.choice(list(cls.URLS_API_GETMYIP))
            return cls.get_ip(api=api)
        finally:
            if cls.CLIENT:
                cls.CLIENT.close()


def getmyip(key=None, timeout=1):
    return myIP.get_myip(key=key, timeout=timeout)


def sanitize_killproc(proc_gost):
    for proc in variadic(proc_gost):
        proc.terminate()
        try:
            if proc.stdout:
                proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()
            if proc.stdin:
                proc.stdin.close()
        except Exception:
            pass
        finally:
            proc.wait()


class TorGuardProxies:
    CONF_TORPROXIES_LIST_HTTPPORTS = [489, 23, 7070, 465, 993, 282, 778, 592]
    CONF_TORPROXIES_COUNTRIES = [
        "fn",
        "no",
        "bg",
        "pg",
        "it",
        "fr",
        "sp",
        "ire",
        "ice",
        "cz",
        "aus",
        "ger",
        "uk",
        "uk.man",
        "ro",
        "slk",
        "nl",
        "hg",
        "bul",
    ]
    CONF_TORPROXIES_DOMAINS = [f"{cc}.secureconnect.me" for cc in CONF_TORPROXIES_COUNTRIES]
    CONF_TORPROXIES_NOK = Path(PATH_LOGS, "bad_proxies.txt")

    EVENT = MySyncAsyncEvent("dummy")

    logger = logging.getLogger("torguardprx")

    @classmethod
    def mytest_proxies_rt(cls, routing_table, timeout=2):
        TorGuardProxies.logger.info("[init_proxies] starting test proxies")
        bad_pr = []
        exe = ThreadPoolExecutor(thread_name_prefix="testproxrt")
        try:
            futures = {
                exe.submit(getmyip, key=_key, timeout=timeout): _key for _key in list(routing_table.keys())}

            for fut in as_completed(list(futures.keys())):
                if TorGuardProxies.EVENT.is_set():
                    break
                if not fut.exception() and is_ipaddr(_ip := fut.result()):
                    if _ip != routing_table[futures[fut]]:
                        TorGuardProxies.logger.debug(
                            f"[{futures[fut]}] test: {_ip} expect res: {routing_table[futures[fut]]}")
                        bad_pr.append(routing_table[futures[fut]])
                else:
                    bad_pr.append(routing_table[futures[fut]])
            return bad_pr
        finally:
            exe.shutdown(wait=False, cancel_futures=True)

    @classmethod
    def _init_gost(cls, cmd_gost: list) -> list:
        proc_gost = []
        for cmd in cmd_gost:
            try:
                _proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
                _proc.poll()
                if _proc.returncode:
                    TorGuardProxies.logger.error(f"[initprox] rc[{_proc.returncode}] to cmd[{cmd}]")
                    raise ConnectError("init proxies error")
                else:
                    proc_gost.append(_proc)
                time.sleep(0.05)
            except ConnectError:
                sanitize_killproc(proc_gost)
                proc_gost = []
                break
        return proc_gost

    @classmethod
    def mytest_proxies_raw(cls, list_ips, port=CONF_TORPROXIES_HTTPPORT, timeout=2):
        cmd_gost = [
            f"gost -L=:{CONF_PROXIES_BASE_PORT + 2000 + i} " +
            f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip}:{port}"
            for i, ip in enumerate(list_ips)]
        routing_table = {CONF_PROXIES_BASE_PORT + 2000 + i: ip for i, ip in enumerate(list_ips)}

        if not (proc_gost := cls._init_gost(cmd_gost)):
            TorGuardProxies.logger.error("[init_proxies] error with gost commands")
            return False
        try:
            _res_bad = None
            if not TorGuardProxies.EVENT.is_set():
                _res_ps = subprocess.run(["ps"], encoding="utf-8", capture_output=True).stdout
                TorGuardProxies.logger.debug(f"[init_proxies] %no%\n\n{_res_ps}")

                _res_bad = cls.mytest_proxies_rt(routing_table, timeout=timeout)
                _line_ps_pr = []
                for _ip in _res_bad:
                    if _temp := try_get(re.search(rf".+{_ip}\:\d+", _res_ps), lambda x: x.group() if x else None):
                        _line_ps_pr.append(_temp)
                TorGuardProxies.logger.info(
                    f"[init_proxies] check in ps print equal number of bad ips: res_bad [{len(_res_bad)}] "
                    + f"ps_print [{len(_line_ps_pr)}]")

            if _res_bad:
                cached_res = cls.CONF_TORPROXIES_NOK
                with open(cached_res, "w") as f:
                    _test = "\n".join(_res_bad)
                    f.write(_test)
            return _res_bad or True

        finally:
            sanitize_killproc(proc_gost)

    @classmethod
    def get_ips(cls, name):
        res = subprocess.run(
            f"dscacheutil -q host -a name {name}".split(" "),
            encoding="utf-8",
            capture_output=True,
        ).stdout
        return re.findall(r"ip_address: (.+)", res)

    @classmethod
    def init_proxies(
        cls,
        num=CONF_PROXIES_MAX_N_GR_HOST,
        size=CONF_PROXIES_N_GR_VIDEO,
        port=CONF_TORPROXIES_HTTPPORT,
        timeout=5,
        event=None,
    ) -> Tuple[List, Dict]:
        TorGuardProxies.logger.info("[init_proxies] start")

        try:
            if event:
                TorGuardProxies.EVENT = event

            IPS_SSL = []

            if TorGuardProxies.EVENT.is_set():
                return [], {}

            for domain in cls.CONF_TORPROXIES_DOMAINS:
                IPS_SSL += cls.get_ips(domain)

            _bad_ips = None
            cached_res = cls.CONF_TORPROXIES_NOK
            if cached_res.exists() and (
                    (datetime.now() - datetime.fromtimestamp(cached_res.stat().st_mtime)).seconds < 7200):  # every 2h we check the proxies
                with open(cached_res, "r") as f:
                    if (_content := f.read()):
                        _bad_ips = [_ip for _ip in _content.split("\n") if _ip]
            else:
                if TorGuardProxies.EVENT.is_set():
                    return [], {}
                if _bad_ips := cls.mytest_proxies_raw(IPS_SSL, port=port, timeout=timeout):

                    if isinstance(_bad_ips, list):
                        for _ip in _bad_ips:
                            if _ip in IPS_SSL:
                                IPS_SSL.remove(_ip)
                else:
                    TorGuardProxies.logger.error("[init_proxies] test failed")
                    return [], {}

            _ip_main = random.choice(IPS_SSL)

            IPS_SSL.remove(_ip_main)

            if len(IPS_SSL) < num * (size + 1):
                TorGuardProxies.logger.warning("[init_proxies] not enough IPs to generate sample")
                return [], {}

            _ips = random.sample(IPS_SSL, num * (size + 1))

            def grouper(iterable, n, *, incomplete="fill", fillvalue=None):
                from itertools import zip_longest

                args = [iter(iterable)] * n
                if incomplete == "fill":
                    return zip_longest(*args, fillvalue=fillvalue)
                if incomplete == "strict":
                    return zip(*args, strict=True)  # type: ignore
                if incomplete == "ignore":
                    return zip(*args)
                else:
                    raise ValueError("Expected fill, strict, or ignore")

            FINAL_IPS = list(grouper(_ips, (size + 1)))
            cmd_gost_s = []
            routing_table = {}
            cmd_gost_s = [
                f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*i + j} " +
                f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[j]}:{port}"
                for j in range(size + 1) for i, ip in enumerate(FINAL_IPS)]
            routing_table = {(CONF_PROXIES_BASE_PORT + 100 * i + j): ip[j] for j in range(size + 1) for i, ip in enumerate(FINAL_IPS)}

            cmd_gost_main = [
                f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*num + 99} " +
                f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{_ip_main}:{port}"]
            routing_table[CONF_PROXIES_BASE_PORT + 100 * num + 99] = _ip_main

            cmd_gost = cmd_gost_s + cmd_gost_main

            if TorGuardProxies.EVENT.is_set():
                return [], {}

            TorGuardProxies.logger.debug(f"[init_proxies] {cmd_gost}")
            TorGuardProxies.logger.debug(f"[init_proxies] {routing_table}")

            if not (proc_gost := cls._init_gost(cmd_gost)):
                TorGuardProxies.logger.debug("[init_proxies] error with gost commands")
                return [], {}

            return proc_gost, routing_table

        finally:
            TorGuardProxies.logger.info("[init_proxies] done")

    def genwgconf(self, host, **kwargs):
        headers = {
            "User-Agent": CONF_FIREFOX_UA,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en,es-ES;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://torguard.net",
            "Alt-Used": "torguard.net",
            "Connection": "keep-alive",
            "Referer": "https://torguard.net/tgconf.php?action=vpn-openvpnconfig",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "TE": "trailers",
        }
        data = {
            "token": "7cb788d89a93f49b54a91b3517e75a9fda7b7d55",
            "tokk": "ad97fcbd2c6d",
            "device": "",
            "tunnel": "wireguard",
            "oserver[]": "",
            "server": "",
            "protocol": "udp",
            "cipher": "1912|SHA256",
            "Ecipher": "AES-128-CBC",
            "build": "2.6",
            "username": "atgarcia",
            "password": "",
            "privkey": "",
            "pubkey": "",
            "wgport": "1443",
            "mtu": "1390",
        }
        if is_ipaddr(host):
            data["server"] = host
        elif "torguard.com" in host:
            data["oserver[]"] = host
        else:
            TorGuardProxies.logger.error(f"[gen_wg_conf] {host} is not a torguard domain: xx.torguard.com")

        cl = get_httpx_client()

        if ckies := kwargs.get("cookies"):
            reqckies = ckies.get("Request Cookies", ckies)
            for name, value in reqckies.items():
                cl.cookies.set(name, value, "torguard.net")
        else:
            for cookie in extract_cookies_from_browser("firefox"):
                if "torguard.net" in cookie.domain:
                    cl.cookies.set(name=cookie.name, value=cookie.value, domain=cookie.domain)  # type: ignore
        if info := kwargs.get("info"):
            data |= info
        else:
            headersform = {
                "User-Agent": CONF_FIREFOX_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en,es-ES;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Origin": "https://torguard.net",
                "Alt-Used": "torguard.net",
                "Connection": "keep-alive",
                "Referer": "https://torguard.net/clientarea.php",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
                "TE": "trailers",
            }
            with limiter_0_1.ratelimit("torguardconf", delay=True):
                resform = cl.get(
                    "https://torguard.net/tgconf.php?action=vpn-openvpnconfig", headers=headersform
                )
            token, tokk = re.findall(r'"(?:token|tokk)" value="([^"]+)"', resform.text) or ["", ""]
            if token and tokk:
                data["token"] = token
                data["tokk"] = tokk
        urlpost = "https://torguard.net/generateconfig.php"
        with limiter_0_1.ratelimit("torguardconf", delay=True):
            respost = try_get(cl.post(urlpost, headers=headers, data=data), lambda x: x.json())
        if respost and respost.get("success") == "true" and (_config := respost.get("config")):
            allips = [
                "1.0.0.0/8",
                "2.0.0.0/8",
                "3.0.0.0/8",
                "4.0.0.0/6",
                "8.0.0.0/7",
                "11.0.0.0/8",
                "12.0.0.0/6",
                "16.0.0.0/4",
                "32.0.0.0/3",
                "64.0.0.0/2",
                "128.0.0.0/3",
                "160.0.0.0/5",
                "168.0.0.0/6",
                "172.0.0.0/12",
                "172.32.0.0/11",
                "172.64.0.0/10",
                "172.128.0.0/9",
                "173.0.0.0/8",
                "174.0.0.0/7",
                "176.0.0.0/4",
                "192.0.0.0/9",
                "192.128.0.0/11",
                "192.160.0.0/13",
                "192.169.0.0/16",
                "192.170.0.0/15",
                "192.172.0.0/14",
                "192.176.0.0/12",
                "192.192.0.0/10",
                "193.0.0.0/8",
                "194.0.0.0/7",
                "196.0.0.0/6",
                "200.0.0.0/5",
                "208.0.0.0/4",
                "10.26.0.1/32",
            ]
            allowedips = "AllowedIPs = " + ", ".join(allips)
            _config = _config.replace("DNS = 1.1.1.1", "DNS = 10.26.0.1").replace(
                "AllowedIPs = 0.0.0.0/0", allowedips
            )
            if not data["server"]:
                _ip = (
                    try_get(
                        re.search(r"Endpoint = (?P<ip>[^\:]+)\:", _config), lambda x: x.groupdict().get("ip")
                    )
                    or ""
                )
                _file = data["oserver[]"].split(".")[0].upper() + _ip.replace(".", "_") + ".conf"
            else:
                _file = (kwargs.get("pre", "") or "") + data["server"].replace(".", "_") + ".conf"
            with open(f"/Users/antoniotorres/testing/{_file}", "w") as f:
                f.write(_config)
        else:
            TorGuardProxies.logger.error(
                f"[gen_wg_conf] {respost.get('error') if respost else 'error with host[{host}]: check cookies and tokens'}"
            )


def get_all_wd_conf(name=None):
    if name and "torguard.com" in name:
        ips = TorGuardProxies.get_ips(name)
        if ips:
            init_logging()
            total = len(ips)
            _pre = name.split(".")[0].upper()
            proxies = TorGuardProxies()

            with ProgressBar(None, total) as pb:

                def getconf(_ip):
                    with contextlib.suppress(Exception):
                        proxies.genwgconf(_ip, pre=_pre)
                    with pb._lock:
                        pb.update()
                        pb.print("")

                with ThreadPoolExecutor(thread_name_prefix="tgconf", max_workers=5) as exe:
                    _ = [
                        exe.submit(
                            getconf,
                            ip,
                        )
                        for ip in ips
                    ]

                time.sleep(1)
                pb.print("")
        else:
            print(f"{name} doesnt get any ip when dns resolving")
    else:
        print("Missing domain torguard: xx.torguard.com")


def get_wd_conf(name=None, pre=None):
    if name:
        try:
            proxies = TorGuardProxies()
            proxies.genwgconf(name, pre=pre)
        except Exception as e:
            logger = logging.getLogger("wdconf")
            logger.exception(repr(e))
    else:
        print("Use ip or torguard domain xx.torguard.com")


############################################################
# """                     YTDLP                           """
############################################################

if yt_dlp:
    from pyrate_limiter import LimitContextDecorator
    from yt_dlp import YoutubeDL, parse_options
    from yt_dlp.cookies import extract_cookies_from_browser
    from yt_dlp.extractor.commonwebdriver import (
        By,
        ConnectError,
        HTTPStatusError,
        InfoExtractor,
        ProgressBar,
        ReExtractInfo,
        SeleniumInfoExtractor,
        StatusError503,
        StatusStop,
        dec_on_exception,
        dec_retry_error,
        ec,
        getter_basic_config_extr,
        limiter_0_1,
        limiter_1,
        limiter_5,
        limiter_15,
        limiter_non,
        load_config_extractors,
        my_dec_on_exception,
    )
    from yt_dlp.extractor.nakedsword import NakedSwordBaseIE
    from yt_dlp.utils import (
        ExtractorError,
        find_available_port,
        get_domain,
        js_to_json,
        prepend_extension,
        render_table,
        sanitize_filename,
        smuggle_url,
        traverse_obj,
        try_call,
        try_get,
        unsmuggle_url,
        variadic,
        write_string,
    )

    assert HTTPStatusError
    assert LimitContextDecorator
    assert find_available_port
    assert unsmuggle_url
    assert smuggle_url
    assert prepend_extension
    assert get_domain
    assert NakedSwordBaseIE
    assert load_config_extractors
    assert getter_basic_config_extr
    assert SeleniumInfoExtractor
    assert InfoExtractor
    assert StatusStop
    assert dec_on_exception
    assert dec_retry_error
    assert limiter_0_1
    assert limiter_1
    assert limiter_5
    assert limiter_15
    assert limiter_non
    assert ReExtractInfo
    assert ConnectError
    assert StatusError503
    assert my_dec_on_exception
    assert ec
    assert By
    assert try_call

    def get_values_regex(str_reg_list, str_content, *_groups, not_found=None):
        for str_reg in str_reg_list:
            if mobj := re.search(str_reg, str_content):
                return mobj.group(*_groups)
        return not_found

    class mylogger(logging.LoggerAdapter):
        def __init__(self, logger):
            super().__init__(logger, {})
            self.quiet = False

        def info(self, msg, *args, **kwargs):
            if self.quiet:
                self.log(logging.DEBUG, msg, *args, **kwargs)
            else:
                self.log(logging.INFO, msg, *args, **kwargs)

        def warning(self, msg, *args, **kwargs):
            if self.quiet:
                self.log(logging.DEBUG, msg, *args, **kwargs)
            else:
                self.log(logging.WARNING, msg, *args, **kwargs)

        def error(self, msg, *args, **kwargs):
            if self.quiet:
                self.log(logging.DEBUG, msg, *args, **kwargs)
            else:
                self.log(logging.ERROR, msg, *args, **kwargs)

        def pprint(self, msg):
            self.log(logging.DEBUG, msg)
            print(msg, end="\n", flush=True)

    class MyYTLogger(logging.LoggerAdapter):
        """
        para ser compatible con el logging de yt_dlp: yt_dlp usa debug para enviar los debug y
        los info. Los debug llevan '[debug] ' antes.
        se pasa un logger de logging al crear la instancia
        logger = MyYTLogger(logging.getLogger("name_ejemplo", {}))
        """

        _debug_phr = [
            "Falling back on generic information extractor",
            "Extracting URL:",
            "Media identified",
            "The information of all playlist entries will be held in memory",
            "Looking for video embeds",
            "Identified a HTML5 media",
            "Identified a KWS Player",
            " unable to extract",
            "Looking for embeds",
            "Looking for Brightcove embeds",
            "Identified a html5 embed",
            "from cache",
            "to cache",
            "Downloading MPD manifest" "Downloading m3u8 information",
            "Downloading media selection JSON",
            "Loaded ",
            "Sort order given by user:",
            "Formats sorted by:",
            "No video formats found!",
            "Requested format is not available",
            "You have asked for UNPLAYABLE formats to be listed/downloaded"
        ]

        _skip_phr = ["Downloading", "Extracting information", "Checking", "Logging"]

        def __init__(self, logger, quiet=False, verbose=False, superverbose=False):
            super().__init__(logger, {})
            self.quiet = quiet
            self.verbose = verbose
            self.superverbose = superverbose

        def error(self, msg, *args, **kwargs):
            self.log(logging.DEBUG, msg, *args, **kwargs)

        def warning(self, msg, *args, **kwargs):
            if any(_ in msg for _ in self._debug_phr):
                self.log(logging.DEBUG, msg, *args, **kwargs)
            else:
                self.log(logging.WARNING, msg, *args, **kwargs)

        def debug(self, msg, *args, **kwargs):
            mobj = get_values_regex([r"^(\[[^\]]+\])"], msg) or ""
            mobj2 = msg.split(": ")[-1]

            if self.quiet:
                self.log(logging.DEBUG, msg, *args, **kwargs)
            elif self.verbose and not self.superverbose:
                if any(
                    [
                        (mobj in ("[redirect]", "[download]", "[debug+]", "[info]")),
                        (mobj in ("[debug]") and any(_ in msg for _ in self._debug_phr)),
                        any(_ in mobj2 for _ in self._skip_phr),
                    ]
                ):
                    self.log(logging.DEBUG, msg[len(mobj):].strip(), *args, **kwargs)
                else:
                    self.log(logging.INFO, msg, *args, **kwargs)
            elif self.superverbose:
                self.log(logging.INFO, msg, *args, **kwargs)
            elif mobj in ("[redirect]", "[debug]", "[info]", "[download]", "[debug+]") or any(
                    _ in mobj2 for _ in self._skip_phr):
                self.log(logging.DEBUG, msg[len(mobj):].strip(), *args, **kwargs)
            else:
                self.log(logging.INFO, msg, *args, **kwargs)

    def change_status_nakedsword(status):
        NakedSwordBaseIE._STATUS = status

    def cli_to_api(*opts):
        default = yt_dlp.parse_options([]).ydl_opts
        diff = {k: v for k, v in parse_options(opts).ydl_opts.items() if default[k] != v}
        if "postprocessors" in diff:
            diff["postprocessors"] = [
                pp for pp in diff["postprocessors"] if pp not in default["postprocessors"]
            ]
        return diff

    class myYTDL(YoutubeDL):
        def __init__(self, params: Optional[dict] = None, auto_init: Union[bool, str] = True, **kwargs):
            self._close = kwargs.get("close", None)
            if self._close is None:
                self._close = True
            self.executor = kwargs.get("executor", None)
            if self.executor is None:
                self.executor = ThreadPoolExecutor(thread_name_prefix=self.__class__.__name__.lower())
            _silent = kwargs.get("silent")
            if _silent is None:
                _silent = False
            _proxy = kwargs.pop("proxy", None)
            opts = {}
            if _proxy:
                opts["proxy"] = _proxy
            if _silent:
                opts["quiet"] = True
                opts["verbose"] = False
                opts["verboseplus"] = False
                opts["logger"] = MyYTLogger(
                    logging.getLogger("yt_dlp_s"), quiet=True, verbose=False, superverbose=False
                )

            super().__init__(params=(params or {}) | opts, auto_init=auto_init)  # type: ignore

        def __exit__(self, *args):
            super().__exit__(*args)
            if self._close:
                self.close()

        async def __aenter__(self):
            return await sync_to_async(super().__enter__, thread_sensitive=False, executor=self.executor)()

        async def __aexit__(self, *args):
            return await sync_to_async(self.__exit__, thread_sensitive=False, executor=self.executor)(*args)

        def get_extractor(self, el: str) -> Union[tuple, InfoExtractor]:
            if el.startswith('http'):
                return self._extracted_from_get_extractor_3(el)
            _sel_ie = self.get_info_extractor(el)
            _sel_ie._real_initialize()
            return _sel_ie

        # TODO Rename this here and in `get_extractor`
        def _extracted_from_get_extractor_3(self, el):
            url = el
            ies = self._ies
            _sel_ie_key = "Generic"
            for ie_key, ie in ies.items():
                try:
                    if ie.suitable(url) and (ie_key != "Generic"):
                        # return (ie_key, self.get_info_extractor(ie_key))
                        _sel_ie_key = ie_key
                        break
                except Exception as e:
                    logger = logging.getLogger("asyncdl")
                    logger.exception(f"[get_extractor] fail with {ie_key} - {repr(e)}")
            _sel_ie = self.get_info_extractor(_sel_ie_key)
            _sel_ie._real_initialize()
            # return ("Generic", self.get_info_extractor("Generic"))
            return (_sel_ie_key, _sel_ie)

        def is_playlist(self, url: str) -> tuple:
            ie_key, ie = cast(tuple, self.get_extractor(url))
            if ie_key == "Generic":
                return (True, ie_key)
            else:
                return (ie._RETURN_TYPE == "playlist", ie_key)

        # def close(self):
        #     super().close()
        #     ies_close(self._ies_instances)

        async def stop(self):
            if (_stop := self.params.get("stop")):
                _stop.set()
                await asyncio.sleep(0)
            if (_stop_dl := self.params.get("stop_dl")):
                for _, _ev_stop_dl in _stop_dl.items():
                    _ev_stop_dl.set()
                    await asyncio.sleep(0)

        def sanitize_info(self, *args, **kwargs) -> dict:
            return cast(dict, super().sanitize_info(*args, **kwargs))

        async def async_extract_info(self, *args, **kwargs) -> dict:
            return cast(
                dict,
                await sync_to_async(self.extract_info, thread_sensitive=False, executor=self.executor)(
                    *args, **kwargs
                ),
            )

        async def async_process_ie_result(self, *args, **kwargs) -> dict:
            return await sync_to_async(
                self.process_ie_result, thread_sensitive=False, executor=self.executor
            )(*args, **kwargs)

    # class ProxyYTDL(myYTDL):
    #     def __init__(self, **kwargs):
    #         _kwargs = kwargs.copy()
    #         opts = _kwargs.pop("opts", {}).copy()
    #         proxy = _kwargs.pop("proxy", None)
    #         quiet = _kwargs.pop("quiet", True)
    #         verbose = _kwargs.pop("verbose", False)
    #         verboseplus = _kwargs.pop("verboseplus", False)
    #         _kwargs.pop("auto_init", None)

    #         opts["quiet"] = quiet
    #         opts["verbose"] = verbose
    #         opts["verboseplus"] = verboseplus
    #         opts["logger"] = MyYTLogger(
    #             logging.getLogger(self.__class__.__name__.lower()),
    #             quiet=opts["quiet"],
    #             verbose=opts["verbose"],
    #             superverbose=opts["verboseplus"],
    #         )
    #         opts["proxy"] = proxy

    #         super().__init__(params=opts, auto_init="no_verbose_header", **_kwargs)  # type: ignore

    def get_extractor_ytdl(url: str, ytdl: Union[YoutubeDL, myYTDL]) -> tuple:
        logger = logging.getLogger("asyncdl")
        ies = ytdl._ies
        for ie_key, ie in ies.items():
            try:
                if ie.suitable(url) and (ie_key != "Generic"):
                    return (ie_key, ytdl.get_info_extractor(ie_key))
            except Exception as e:
                logger.exception(f"[get_extractor] fail with {ie_key} - {repr(e)}")
        return ("Generic", ytdl.get_info_extractor("Generic"))

    def is_playlist_ytdl(url: str, ytdl: Union[YoutubeDL, myYTDL]) -> tuple:
        ie_key, ie = get_extractor_ytdl(url, ytdl)
        if ie_key == "Generic":
            return (True, ie_key)
        else:
            return (ie._RETURN_TYPE == "playlist", ie_key)

    def init_ytdl(args):
        """
        {
            "usenetrc": true,
            "netrc_location": null,
            "username": null,
            "password": null,
            "twofactor": null,
            "videopassword": null,
            "ap_mso": null,
            "ap_username": null,
            "ap_password": null,
            "client_certificate": null,
            "client_certificate_key": null,
            "client_certificate_password": null,
            "quiet": false,
            "no_warnings": false,
            "forceurl": false,
            "forcetitle": false,
            "forceid": false,
            "forcethumbnail": false,
            "forcedescription": false,
            "forceduration": false,
            "forcefilename": false,
            "forceformat": false,
            "forceprint": {},
            "print_to_file": {},
            "forcejson": false,
            "dump_single_json": false,
            "force_write_download_archive": false,
            "simulate": null,
            "skip_download": true,
            "format": "bv*+ba/b",
            "allow_unplayable_formats": false,
            "ignore_no_formats_error": true,
            "format_sort": [
                "ext:mp4:m4a"
            ],
            "format_sort_force": false,
            "allow_multiple_video_streams": false,
            "allow_multiple_audio_streams": false,
            "check_formats": null,
            "listformats": null,
            "listformats_table": true,
            "outtmpl": {
                "default": "%(id)s/%(id)s_%(title)s.%(ext)s",
                "chapter": "%(title)s-%(section_number)03d-%(section_title)s-[%(id)s].%(ext)s"
            },
            "outtmpl_na_placeholder": "NA",
            "paths": {
                "home": "~/testing/20230408"
            },
            "autonumber_size": null,
            "autonumber_start": 1,
            "restrictfilenames": true,
            "windowsfilenames": false,
            "ignoreerrors": true,
            "force_generic_extractor": false,
            "allowed_extractors": [
                "default"
            ],
            "ratelimit": null,
            "throttledratelimit": null,
            "retries": 2,
            "file_access_retries": 3,
            "fragment_retries": 10,
            "extractor_retries": 1,
            "retry_sleep_functions": {},
            "skip_unavailable_fragments": true,
            "keep_fragments": false,
            "concurrent_fragment_downloads": 1,
            "buffersize": 1024,
            "noresizebuffer": false,
            "http_chunk_size": null,
            "continuedl": true,
            "noprogress": false,
            "progress_with_newline": false,
            "progress_template": {},
            "playliststart": 1,
            "playlistend": null,
            "playlistreverse": null,
            "playlistrandom": null,
            "lazy_playlist": null,
            "noplaylist": false,
            "logtostderr": false,
            "consoletitle": false,
            "nopart": false,
            "updatetime": false,
            "writedescription": false,
            "writeannotations": false,
            "writeinfojson": null,
            "allow_playlist_files": true,
            "clean_infojson": true,
            "getcomments": false,
            "writethumbnail": false,
            "write_all_thumbnails": false,
            "writelink": false,
            "writeurllink": false,
            "writewebloclink": false,
            "writedesktoplink": false,
            "writesubtitles": true,
            "writeautomaticsub": false,
            "allsubtitles": false,
            "listsubtitles": false,
            "subtitlesformat": "best",
            "subtitleslangs": [
                "all"
            ],
            "matchtitle": null,
            "rejecttitle": null,
            "max_downloads": null,
            "prefer_free_formats": false,
            "trim_file_name": 0,
            "verbose": true,
            "dump_intermediate_pages": false,
            "write_pages": false,
            "load_pages": false,
            "test": false,
            "keepvideo": true,
            "min_filesize": null,
            "max_filesize": null,
            "min_views": null,
            "max_views": null,
            "daterange": "<yt_dlp.utils.DateRange object at 0x1026c3310>",
            "cachedir": null,
            "youtube_print_sig_code": false,
            "age_limit": null,
            "download_archive": null,
            "break_on_existing": false,
            "break_on_reject": false,
            "break_per_url": false,
            "skip_playlist_after_errors": null,
            "cookiefile": null,
            "cookiesfrombrowser": null,
            "legacyserverconnect": false,
            "nocheckcertificate": false,
            "prefer_insecure": null,
            "enable_file_urls": false,
            "http_headers": {
                "User-Agent": ,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Sec-Fetch-Mode": "navigate",
                "Accept-Encoding": "gzip,deflate"
            },
            "proxy": null,
            "socket_timeout": null,
            "bidi_workaround": null,
            "debug_printtraffic": false,
            "prefer_ffmpeg": true,
            "include_ads": null,
            "default_search": null,
            "dynamic_mpd": true,
            "extractor_args": {},
            "youtube_include_dash_manifest": true,
            "youtube_include_hls_manifest": true,
            "encoding": null,
            "extract_flat": "discard_in_playlist",
            "live_from_start": null,
            "wait_for_video": null,
            "mark_watched": false,
            "merge_output_format": null,
            "final_ext": null,
            "postprocessors": [
                {
                "key": "FFmpegSubtitlesConvertor",
                "format": "srt",
                "when": "before_dl"
                },
                {
                "key": "FFmpegConcat",
                "only_multi_video": true,
                "when": "playlist"
                }
            ],
            "fixup": null,
            "source_address": null,
            "call_home": false,
            "sleep_interval_requests": null,
            "sleep_interval": null,
            "max_sleep_interval": null,
            "sleep_interval_subtitles": 0,
            "external_downloader": {
                "default": "aria2c"
            },
            "download_ranges": "yt_dlp.utils.download_range_func([], [])",
            "force_keyframes_at_cuts": false,
            "list_thumbnails": false,
            "playlist_items": null,
            "xattr_set_filesize": null,
            "match_filter": null,
            "no_color": false,
            "ffmpeg_location": null,
            "hls_prefer_native": null,
            "hls_use_mpegts": null,
            "hls_split_discontinuity": false,
            "external_downloader_args": {},
            "postprocessor_args": {},
            "cn_verification_proxy": null,
            "geo_verification_proxy": null,
            "geo_bypass": true,
            "geo_bypass_country": null,
            "geo_bypass_ip_block": null,
            "_warnings": [],
            "_deprecation_warnings": [],
            "compat_opts": set()
        }
        """

        logger = logging.getLogger("yt_dlp")

        headers = {
            "User-Agent": args.useragent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

        ytdl_opts = {
            "allow_unplayable_formats": True,
            "retries": 1,
            "extractor_retries": 1,
            "force_generic_extractor": False,
            "allowed_extractors": ["default"],
            "http_headers": headers,
            "proxy": args.proxy,
            "logger": MyYTLogger(logger, quiet=args.quiet, verbose=args.verbose, superverbose=args.vv),
            "verbose": args.verbose,
            "quiet": args.quiet,
            "format": args.format,
            "format_sort": [args.sort],
            "nocheckcertificate": True,
            "subtitleslangs": ["en", "es", "ca"],
            "keepvideo": True,
            "convertsubtitles": "srt",
            "continuedl": True,
            "updatetime": False,
            "ignore_no_formats_error": True,
            "ignoreerrors": False,
            "no_abort_on_errors": False,
            "extract_flat": "in_playlist",
            "color": {"stderr": "no_color", "stdout": "no_color"},
            "usenetrc": True,
            "skip_download": True,
            "writesubtitles": True,
            "postprocessors": [{"key": "FFmpegSubtitlesConvertor", "format": "srt", "when": "before_dl"}],
            "concurrent_fragment_downloads": 64,
            "restrictfilenames": True,
            "user_agent": args.useragent,
            "verboseplus": args.vv,
            "sem": {},
            "stop_dl": {},
            "stop": threading.Event(),
            "lock": threading.Lock(),
            "embed": not args.no_embed,
            "_util_classes": {"SimpleCountDown": SimpleCountDown},
            "outtmpl": {"default": "%(id)s_%(title)s.%(ext)s"}
        }

        if args.use_cookies:
            ytdl_opts["cookiesfrombrowser"] = ("firefox", CONF_FIREFOX_PROFILE, None)

        if args.ytdlopts:
            ytdl_opts |= json.loads(js_to_json(args.ytdlopts))

        ytdl = myYTDL(params=ytdl_opts, auto_init="no_verbose_header")

        logger.debug(f"ytdl opts:\n{ytdl.params}")

        return ytdl

    def get_format_id(info_dict, _formatid) -> dict:
        if not info_dict:
            return {}

        if _req_fts := info_dict.get("requested_formats"):
            for _ft in _req_fts:
                if _ft["format_id"] == _formatid:
                    return _ft
        elif _req_ft := info_dict.get("format_id"):
            if _req_ft == _formatid:
                return info_dict
        return {}

    def _for_print_entry(entry):
        if not entry:
            return ""
        _entry = copy.deepcopy(entry)

        if _formats := _entry.get("formats"):
            _new_formats = []
            for _format in _formats:
                if len(_formats) > 5:
                    _id, _prot = _format["format_id"], _format["protocol"]
                    _format = {"format_id": _id, ...: ..., "protocol": _prot}

                elif _frag := _format.get("fragments"):
                    _format["fragments"] = [_frag[0], ..., _frag[-1]]
                _new_formats.append(_format)

            _entry["formats"] = _new_formats

        if _formats := _entry.get("requested_formats"):
            _new_formats = []
            for _format in _formats:
                if _frag := _format.get("fragments"):
                    _format["fragments"] = [_frag[0], ..., _frag[-1]]
                _new_formats.append(_format)

            _entry["requested_formats"] = _new_formats

        if _frag := _entry.get("fragments"):
            _entry["fragments"] = [_frag[0], ..., _frag[-1]]

        return _entry

    def _for_print(info):
        if not info:
            return ""
        _info = copy.deepcopy(info)
        if not (_entries := _info.get("entries")):
            return _for_print_entry(_info)
        _info["entries"] = [_for_print_entry(_el) for _el in _entries]
        return _info

    def _for_print_videos(videos):
        if not videos:
            return ""
        _videos = copy.deepcopy(videos)

        if isinstance(_videos, dict):
            for _urlkey, _value in _videos.items():
                if _info := traverse_obj(_value, "video_info"):
                    _value["video_info"] = _for_print(_info)

            return "{" + ",\n".join([f"'{key}': {value}" for key, value in _videos.items()]) + "}"

        elif isinstance(_videos, list):
            _videos = [str(_for_print(_vid)) for _vid in _videos]
            return "[" + ",\n".join(_videos) + "]"

    def render_res_table(data, headers=(), maxcolwidths=None, showindex=True, tablefmt="simple"):
        if tabulate:
            return tabulate(
                data, headers=headers, maxcolwidths=maxcolwidths, showindex=showindex, tablefmt=tablefmt
            )
        logger = logging.getLogger("asyncdl")
        logger.warning("Tabulate is not installed, tables will not be presented optimized")
        return render_table(headers, data, delim=True)

    def send_http_request(url, **kwargs) -> Optional[httpx.Response | dict]:
        """
        raises ReExtractInfo(403), HTTPStatusError, StatusError503, TimeoutError, ConnectError
        """
        _kwargs = kwargs.copy()
        new_e = _kwargs.pop("new_e", Exception)
        if 'client' not in _kwargs:
            _kwargs['client'] = httpx.Client(**CLIENT_CONFIG)
        try:
            return SeleniumInfoExtractor._send_http_request(url, **_kwargs)
        except ExtractorError as e:
            raise new_e(str(e)) from e
        except (ConnectError, httpx.HTTPStatusError) as e:
            return {"error": repr(e)}

    def get_xml(mpd_url, **kwargs):
        import defusedxml.ElementTree as etree

        with httpx.Client(**CLIENT_CONFIG) as client:
            if (_doc := try_get(client.get(mpd_url, **kwargs), lambda x: x.content.decode('utf-8', 'replace') if x else None)):
                return etree.XML(_doc)

    def validate_drm_lic(lic_url, challenge):
        with httpx.Client(**CLIENT_CONFIG) as client:
            return client.post(lic_url, content=challenge).content

    def get_drm_keys(lic_url, mpd_url):
        from videodownloader import VideoDownloader as vd
        return vd._get_key_drm(lic_url, mpd_url=mpd_url)

    def get_files_same_id():
        config_folders = {
            "local": Path(Path.home(), "testing"),
            "pandaext4": Path("/Volumes/Pandaext4/videos"),
            "pandaext1": Path("/Volumes/Pandaext1/videos"),
            "datostoni": Path("/Volumes/DatosToni/videos"),
            "wd1b": Path("/Volumes/WD1B/videos"),
            "wd5": Path("/Volumes/WD5/videos"),
            "wd8_1": Path("/Volumes/WD8_1/videos"),
            "wd8_2": Path("/Volumes/WD8_2/videos"),
            "t7": Path("/Volumes/T7/videos")
        }

        logger = logging.getLogger("get_files")

        list_folders = []

        for _vol, _folder in config_folders.items():
            if not _folder.exists():
                logger.error(f"failed {_vol}:{_folder}, let get previous info saved in previous files")

            else:
                list_folders.append(_folder)

        files_cached = []
        for folder in list_folders:
            logger.info(f">>>>>>>>>>>STARTS {str(folder)}")

            files = []
            try:
                files = [
                    file
                    for file in folder.rglob("*")
                    if file.is_file()
                    and not file.is_symlink()
                    and "videos/_videos/" not in str(file)
                    and not file.stem.startswith(".")
                    and (file.suffix.lower() in (".mp4", ".mkv", ".ts", ".zip"))
                ]

            except Exception as e:
                logger.error(f"[get_files_cached][{folder}] {repr(e)}")

            for file in files:
                _res = file.stem.split("_", 1)
                if len(_res) == 2:
                    _id = _res[0]

                else:
                    _id = sanitize_filename(file.stem, restricted=True).upper()

                files_cached.append((_id, str(file)))

        _res_dict = {}
        for el in files_cached:
            for item in files_cached:
                if (el != item) and (item[0] == el[0]):
                    if not _res_dict.get(el[0]):
                        _res_dict[el[0]] = {el[1], item[1]}
                    else:
                        _res_dict[el[0]].update([el[1], item[1]])
        return sorted(_res_dict.items(), key=lambda x: len(x[1]))

    def check_if_dl(info_dict, videos=None):
        if not videos:
            ls = LocalStorage()
            ls.load_info()
            videos = ls._data_for_scan
        res = {}
        if isinstance(info_dict, dict):
            info = [info_dict]
            if info_dict.get("entries"):
                info = info_dict["entries"]

            for vid in info:
                if not (_id := vid.get("id")) or not (_title := vid.get("title")):
                    continue

                _title = sanitize_filename(_title, restricted=True).upper()
                vid_name = f"{_id}_{_title}"
                res[vid_name] = videos.get(vid_name)
        else:
            info = [info_dict] if isinstance(info_dict, str) else info_dict
            for vid in info:
                vidname = sanitize_filename(vid, restricted=True).upper()
                res[vidname] = {}
                for key in videos.keys():
                    if vidname in key:
                        res[vidname].update({key: videos[key]})
        return res

    def change_title(info_dict, videos=None):
        if not videos:
            ls = LocalStorage()
            ls.load_info()
            videos = ls._data_for_scan
        if isinstance(info_dict, dict):
            info = [info_dict]
            if info_dict.get("entries"):
                info = info_dict["entries"]

            res = {}
            for vid in info:
                if not (_id := vid.get("id")) or not (_title := vid.get("title")):
                    continue

                _title = sanitize_filename(_title, restricted=True)
                vid_name = _id
                res[vid_name] = {"title": _title}
                for key in videos.keys():
                    if key.startswith(f"{_id}_"):
                        file = Path(videos[key])
                        res[vid_name]["file"] = file
                        res[vid_name]["file_name"] = file.stem
                        res[vid_name]["file_name_def"] = f"{vid_name}_{_title}"
                        if res[vid_name]["file_name"] != res[vid_name]["file_name_def"]:
                            res[vid_name]["file_change"] = Path(
                                file.parent, res[vid_name]["file_name_def"] + file.suffix
                            )
                        break
            return res

    # tools for gvd files, xattr, move files etc
    def dl_gvd_best_videos(date, ytdl=None, quiet=False):
        if not ytdl:
            kwargs = {el[0]: el[1] for el in args._get_kwargs()}
            _args = argparse.Namespace(**kwargs)
            if quiet:
                _args.quiet = True
                _args.verbose = False
                _args.vv = False
            yt = init_ytdl(_args)
        else:
            yt = ytdl

        logger = mylogger(logging.getLogger("dl_gvd"))
        logger.quiet = quiet
        url = f"https://www.gvdblog.com/search?date={date}"
        resleg = yt.extract_info(url, download=False)
        if resleg:
            write_string(f"entriesleg: {len(resleg['entries'])}")
            print("", file=sys.stderr, flush=True)
        resalt = yt.extract_info(f"{url}&alt=yes", download=False)
        if resalt:
            write_string(f"entriesalt: {len(resalt['entries'])}")
            print("", file=sys.stderr, flush=True)
        urls_final = []
        entriesleg = []
        entriesalt = []
        entries_final = []
        if (
            resleg
            and (entriesleg := resleg.get("entries", []))
            and resalt
            and (entriesalt := resalt.get("entries", []))
            and len(entriesleg) == len(entriesalt)
        ):
            urls_alt_dl = []
            urls_leg_dl = []
            for entleg, entalt in zip(entriesleg, entriesalt):
                if entleg["format_id"].startswith("hls") or not entalt["format_id"].startswith("hls"):
                    logger.info(f"cause 1 {entleg['original_url']}")
                    urls_leg_dl.append(entleg["original_url"])
                    urls_final.append(entleg["original_url"])
                    entries_final.append(entleg)
                elif not entleg["format_id"].startswith("hls") and entalt["format_id"].startswith("hls"):
                    entaltfilesize = entalt.get("filesize_approx") or (
                        entalt.get("tbr", 0) * entalt.get("duration", 0) * 1024 / 8
                    )
                    entlegfilesize = entleg.get("filesize")
                    if all(
                        [
                            entlegfilesize,
                            entaltfilesize,
                            entaltfilesize >= 2 * entlegfilesize,
                            entaltfilesize > 786432000 or entlegfilesize < 157286400,
                        ]
                    ):
                        logger.info(
                            f"cause 2.A {entalt['original_url']} - {naturalsize(entaltfilesize)} >= 1.5 * {naturalsize(entlegfilesize)}"
                        )
                        urls_alt_dl.append(entalt["original_url"])
                        urls_final.append(entalt["original_url"])
                        entries_final.append(entalt)
                    else:
                        logger.info(f"cause 2.B {entleg['original_url']}")
                        urls_leg_dl.append(entleg["original_url"])
                        urls_final.append(entleg["original_url"])
                        entries_final.append(entleg)
                else:
                    logger.info(f"cause 3 {entleg['original_url']}")
                    urls_leg_dl.append(entleg["original_url"])
                    urls_final.append(entleg["original_url"])
                    entries_final.append(entleg)

        if not urls_final:
            raise ValueError(
                f"ERROR couldnt create command: entriesleg[{len(entriesleg)}] entriesalt[{len(entriesalt)}]"
            )
        cmd = f"--path SearchGVDBlogPlaylistdate={date} -u " + " -u ".join(urls_final)
        logger.pprint(cmd)
        write_string(str(len(urls_final)))
        print("", file=sys.stderr, flush=True)
        return entries_final


class SentenceTranslator(object):
    def __init__(self, src, dst, patience=-1, timeout=30, error_messages_callback=None):
        self.src = src
        self.dst = dst
        self.patience = patience
        self.timeout = timeout
        self.error_messages_callback = error_messages_callback

    def __call__(self, sentence):
        try:
            patience = self.patience
            translated_sentence = []
            # handle the special case: empty string.
            if not sentence:
                return None
            translated_sentence = self.GoogleTranslate(sentence, src=self.src, dst=self.dst, timeout=self.timeout)
            fail_to_translate = translated_sentence[-1] == '\n'  # type: ignore
            while fail_to_translate and patience:
                translated_sentence = self.GoogleTranslate(translated_sentence, src=self.src, dst=self.dst, timeout=self.timeout)
                if translated_sentence[-1] == '\n':  # type: ignore
                    if patience == -1:
                        continue
                    patience -= 1
                else:
                    fail_to_translate = False

            return translated_sentence

        except KeyboardInterrupt:
            if self.error_messages_callback:
                self.error_messages_callback("Cancelling all tasks")
            else:
                print("Cancelling all tasks")
            return

        except Exception as e:
            if self.error_messages_callback:
                self.error_messages_callback(e)
            else:
                print(e)
            return

    def GoogleTranslate(self, text, src, dst, timeout=30):
        url = 'https://translate.googleapis.com/translate_a/'
        params = f'single?client=gtx&sl={src}&tl={dst}&dt=t&q={text}'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Referer': 'https://translate.google.com'}

        try:
            with httpx.Client() as client:
                response = client.get(url + params, headers=headers, timeout=timeout)
                if response.status_code == 200:
                    response_json = response.json()[0]
                    length = len(response_json)
                    translation = ""
                    for i in range(length):
                        translation = translation + response_json[i][0]
                    return translation
                return

        except KeyboardInterrupt:
            if self.error_messages_callback:
                self.error_messages_callback("Cancelling all tasks")
            else:
                print("Cancelling all tasks")
            return

        except Exception as e:
            if self.error_messages_callback:
                self.error_messages_callback(e)
            else:
                print(e)
            return


def translate_srt(filesrt, srclang, dstlang):

    import srt

    lock = threading.Lock()

    def worker(subt, i):
        if subt.content:
            _temp = subt.content.replace('\n', ' ')
            start = _temp.startswith('# ')
            end = _temp.endswith(' #')
            _res = trans(_temp.replace('# ', '').replace(' #', ''))
            if not _res:
                with lock:
                    print(f"ERROR: {i} - {_temp}")
            if start:
                _res = f'# {_res}'
            if end:
                _res += ' #'
            return _res
        else:
            with lock:
                print(f"ERROR: {i} - {subt.content}")

    with open(filesrt, 'r') as f:
        _srt_text = f.read()

    _list_srt = list(srt.parse(_srt_text))

    trans = SentenceTranslator(src=srclang, dst=dstlang, patience=0)
    with ThreadPoolExecutor(max_workers=16) as exe:
        futures = [exe.submit(worker, subt, i) for i, subt in enumerate(_list_srt)]

    for fut, sub in zip(futures, _list_srt):
        sub.content = fut.result()

    return srt.compose(_list_srt)

############################################################
# """                     various                           """
############################################################


def print_delta_seconds(seconds):
    return ":".join(
        [
            _item.split(".")[0]
            for _item in f"{timedelta(seconds=seconds)}".split(":")[1:]
        ])


def print_tasks(tasks):
    return "\n".join([f"{task.get_name()} : {repr(task.get_coro()).split(' ')[2]}" for task in tasks])


def print_threads(threads):
    return "\n".join([f"{thread.getName()} : {repr(thread._target)}" for thread in threads])


def none_to_zero(item):
    return item if item is not None else 0


def get_chain_links(f):
    _links = [f]
    _f = f
    while _f.is_symlink():
        _link = _f.readlink()
        _links.append(_link)
        _f = _link
    return _links


def kill_processes(logger=None, rpcport=None):
    def _log(msg):
        logger.info(msg) if logger else print(msg)

    try:
        term = (
            (subprocess.run(["tty"], encoding="utf-8", capture_output=True).stdout)
            .splitlines()[0]
            .replace("/dev/", "")
        )
        res = subprocess.run(
            ["ps", "-u", "501", "-x", "-o", "pid,tty,command"],
            encoding="utf-8",
            capture_output=True,
        ).stdout
        if rpcport:
            _aria2cstr = f"aria2c.+--rpc-listen-port {rpcport}.+"
        else:
            _aria2cstr = "aria2cDUMMY"
        mobj = re.findall(
            rf"(\d+)\s+(?:\?\?|{term})\s+((?:.+browsermob-proxy --port.+|"
            + rf"{_aria2cstr}|geckodriver.+|.+mitmdump.+|java -Dapp.name=browsermob-proxy.+|/Applications/"
            + r"Firefox.app/Contents/MacOS/firefox-bin.+))",
            res,
        )
        mobj2 = re.findall(
            rf"\d+\s+(?:\?\?|{term})\s+/Applications/Firefox.app/Contents/"
            + r"MacOS/firefox-bin.+--profile (/var/folders/[^\ ]+) ",
            res,
        )
        mobj3 = re.findall(rf"(\d+)\s+(?:\?\?|{term})\s+((?:.+async_all\.py))", res)
        if mobj:
            proc_to_kill = list(set(mobj))
            results = [
                subprocess.run(
                    ["kill", "-9", f"{process[0]}"],
                    encoding="utf-8",
                    capture_output=True,
                )
                for process in proc_to_kill
            ]
            _debugstr = [
                f"pid: {proc[0]}\n\tcommand: {proc[1]}\n\tres: {res}"
                for proc, res in zip(proc_to_kill, results)
            ]
            _log("[kill_processes]\n" + "\n".join(_debugstr))
        else:
            _log("[kill_processes] No processes found to kill")

        if len(mobj3) > 1:
            proc_to_kill = mobj3[1:]
            results = [
                subprocess.run(
                    ["kill", "-9", f"{process[0]}"],
                    encoding="utf-8",
                    capture_output=True,
                )
                for process in proc_to_kill
            ]
            _debugstr = [
                f"pid: {proc[0]}\n\tcommand: {proc[1]}\n\tres: {res}"
                for proc, res in zip(proc_to_kill, results)
            ]
            _log("[kill_processes_proxy]\n" + "\n".join(_debugstr))

        if mobj2:
            for el in mobj2:
                try:
                    shutil.rmtree(el, ignore_errors=True)
                except Exception as e:
                    _log(f"[kill_processes] error: {repr(e)}")
    except Exception as e:
        _log(f"[kill_processes_proxy]: {repr(e)}")
        raise


def int_or_none(res):
    return int(res) if res else None


def str_or_none(res):
    return str(res) if res else None


def naturalsize(value, binary=False, format_="6.2f"):
    SUFFIXES = {
        "decimal": ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"),
        "binary": ("KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"),
    }

    suffix = SUFFIXES["binary"] if binary else SUFFIXES["decimal"]
    base = 1024 if binary else 1000
    _bytes = float(value)
    abs_bytes = abs(_bytes)

    if abs_bytes == 1:
        return f"{abs_bytes:{format_}} KB"
    elif abs_bytes < base:
        return f"{abs_bytes:{format_}} B"

    for i, s in enumerate(suffix):
        unit = base ** (i + 2)
        if abs_bytes < unit:
            return f"{(base*abs_bytes/unit):{format_}} {s}"
    return f"{(base*abs_bytes/unit):{format_}} {s}"  # type: ignore


def print_norm_time(time):
    """Time in secs"""

    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time

    return f"{hour:.0f}h:{minutes:.0f}min:{seconds:.0f}secs"


def patch_http_connection_pool(**constructor_kwargs):
    """
    This allows to override the default parameters of the
    HTTPConnectionPool constructor.
    For example, to increase the poolsize to fix problems
    with "HttpConnectionPool is full, discarding connection"
    call this function with maxsize=16 (or whatever size
    you want to give to the connection pool)
    """
    import socket

    from urllib3 import connectionpool, poolmanager

    specificoptions = [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.SOL_TCP, socket.TCP_KEEPALIVE, 45),
        (socket.SOL_TCP, socket.TCP_KEEPINTVL, 10),
        (socket.SOL_TCP, socket.TCP_KEEPCNT, 6),
    ]

    class MyHTTPConnectionPool(connectionpool.HTTPConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs |= constructor_kwargs
            super(MyHTTPConnectionPool, self).__init__(*args, **kwargs)

            self.ConnectionCls.default_socket_options += specificoptions  # type: ignore

    poolmanager.pool_classes_by_scheme["http"] = MyHTTPConnectionPool  # type: ignore


def patch_https_connection_pool(**constructor_kwargs):
    """
    This allows to override the default parameters of the
    HTTPConnectionPool constructor.
    For example, to increase the poolsize to fix problems
    with "HttpSConnectionPool is full, discarding connection"
    call this function with maxsize=16 (or whatever size
    you want to give to the connection pool)
    """
    import socket

    from urllib3 import connectionpool, poolmanager

    specificoptions = [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.SOL_TCP, socket.TCP_KEEPALIVE, 45),
        (socket.SOL_TCP, socket.TCP_KEEPINTVL, 10),
        (socket.SOL_TCP, socket.TCP_KEEPCNT, 6),
    ]

    class MyHTTPSConnectionPool(connectionpool.HTTPSConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs |= constructor_kwargs
            super(MyHTTPSConnectionPool, self).__init__(*args, **kwargs)

            self.ConnectionCls.default_socket_options += specificoptions  # type: ignore

    poolmanager.pool_classes_by_scheme["https"] = MyHTTPSConnectionPool  # type: ignore


def init_config(quiet=False, test=False):

    patch_http_connection_pool(maxsize=1000)
    patch_https_connection_pool(maxsize=1000)
    os.environ["MOZ_HEADLESS_WIDTH"] = "1920"
    os.environ["MOZ_HEADLESS_HEIGHT"] = "1080"
    if not quiet:
        return init_logging(test=test)


class CountDowns:
    INPUT_TIMEOUT = 2
    DEFAULT_TIMEOUT = 30
    INTERV_TIME = 0.25
    N_PER_SECOND = 1 if INTERV_TIME >= 1 else int(1 / INTERV_TIME)
    PRINT_DIF_IN_SECS = 20
    _INPUT = Queue()

    def __init__(self, klass, events=None, logger=None):
        self._pre = "[countdown][WAIT403]"
        self.klass = klass
        self.outer_events = list(variadic(events)) if events else []
        self.kill_input = MySyncAsyncEvent("killinput")
        self.logger = logger or logging.getLogger("asyncdl")
        self.index_main = None
        self.countdowns = {}
        self.exe = ThreadPoolExecutor(thread_name_prefix="countdown")
        self.futures = {}
        self.lock = threading.Lock()
        self.start_input()

    def start_input(self):
        if "input" not in self.futures:
            self.futures["input"] = self.exe.submit(self.inputimeout)

    def clean(self):
        self.kill_input.set()
        _futures = []
        if _input := self.futures.get("input"):
            _futures.append(_input)
        time.sleep(self.INTERV_TIME)
        if self.countdowns:
            for _index, _count in self.countdowns.items():
                if _count["status"] == "running":
                    _count["stop"].set()
                    _futures.append(self.futures[_index])
                    time.sleep(self.INTERV_TIME)
        if _futures:
            wait_thr(_futures)
        self.futures = {}
        if self.countdowns:
            self.logger.debug(f"{self._pre} COUNTDOWNS:\n{self.countdowns}")
            self.countdowns = {}

    def setup(self, interval=None, print_secs=None):
        if interval and interval <= 1:
            CountDowns.INTERV_TIME = interval
            CountDowns.N_PER_SECOND = 1 if CountDowns.INTERV_TIME >= 1 else int(1 / CountDowns.INTERV_TIME)
        if print_secs:
            CountDowns.PRINT_DIF_IN_SECS = print_secs

    def inputimeout(self):
        self.logger.debug(f"{self._pre} start input")

        _events = self.outer_events + [self.kill_input]

        try:
            while True:
                try:
                    if [getattr(ev, "name", "noname") for ev in _events if ev.is_set()]:
                        break

                    _input = CountDowns._INPUT.get(block=True, timeout=CountDowns.INPUT_TIMEOUT)
                    if _input == "":
                        _input = self.index_main
                    elif _input in self.countdowns:
                        self.logger.debug(f"{self._pre} input[{_input}] is index video")
                        self.countdowns[_input]["stop"].set()
                    else:
                        self.logger.debug(f"{self._pre} input[{_input}] not index video")

                except Empty:
                    pass
                except Exception as e:
                    self.logger.exception(f"{self._pre} {repr(e)}")
        finally:
            self.logger.debug(f"{self._pre} return Input")

    def start_countdown(self, n, index, event=None):
        def send_queue(x):
            if (x == self.N_PER_SECOND * n) or (x % self.N_PER_SECOND) == 0:
                _msg = f"{self.countdowns[index]['premsg']} {x//self.N_PER_SECOND}"
                self.klass._QUEUE[index].put_nowait(_msg)

        _res = None
        _events = self.outer_events + [self.countdowns[index]["stop"]]
        if event:
            _events += list(variadic(event))

        for i in range(self.N_PER_SECOND * n, 0, -1):
            send_queue(i)
            _res = [getattr(ev, "name", "noname") for ev in _events if ev.is_set()]
            if _res:
                break
            time.sleep(self.INTERV_TIME)

        self.klass._QUEUE[index].put_nowait("")

        if not _res:
            _res = ["TIMEOUT_COUNT"]

        self.logger.debug(f"{self.countdowns[index]['premsg']} return Count: {_res}")
        return _res

    def add(self, n=None, index=None, event=None, msg=None):
        _premsg = f"{self._pre}"
        if msg:
            _premsg += msg

        if index in self.countdowns:
            self.logger.error(f"{_premsg} error: already in countdown")
            return ["ERROR"]

        if n is not None and isinstance(n, int) and n > 3:
            timeout = n - 3
        else:
            timeout = self.DEFAULT_TIMEOUT - 3

        time.sleep(3)

        with self.lock:
            if not self.index_main:
                self.index_main = index

        self.logger.info(f"{_premsg} index_main[{self.index_main}]")

        self.countdowns[index] = {
            "index": index,
            "premsg": _premsg,
            "timeout": timeout,
            "status": "running",
            "stop": MySyncAsyncEvent(f"killcounter[{index}]"),
        }

        _fut = self.exe.submit(self.start_countdown, timeout, index, event=event)
        self.futures[index] = _fut
        self.countdowns[index]["fut"] = _fut

        self.logger.debug(f"{_premsg} added counter \n{self.countdowns}")

        done, _ = wait_thr([_fut])

        # self.countdowns[index]["status"] = "done"

        self.countdowns.pop(index, None)

        with self.lock:
            if self.index_main == index:
                self.index_main = None

        _res = ["ERROR"]
        if done:
            for d in done:
                try:
                    _res = d.result()
                    if "stop" in _res:
                        _res = None
                except Exception as e:
                    self.logger.exception(f"{_premsg} error {repr(e)}")

        self.logger.debug(f"{_premsg} finish wait for counter: {_res}")
        return _res


class Token:
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return f"token:{self.msg}"


class SimpleCountDown:
    restimeout = Token("restimeout")
    resexit = Token("resexit")

    def __init__(
        self, pb, inputqueue, check: Optional[Callable] = None, logger=None, indexdl=None, timeout=60
    ):
        self._pre = "[countdown][WAIT403]"
        self.check = check or (lambda: None)
        self.pb = pb

        self.timeout = timeout
        self.indexdl = indexdl

        self.inputq = inputqueue if queue else None
        self.logger = logger or logging.getLogger("asyncdl")

    def enable_echo(self, enable):
        fd = sys.stdin.fileno()
        new = termios.tcgetattr(fd)
        if enable:
            new[3] |= termios.ECHO
        else:
            new[3] &= ~termios.ECHO

        termios.tcsetattr(fd, termios.TCSANOW, new)

    def _wait_for_enter(self, sel: selectors.DefaultSelector, interval: Optional[int] = None):
        if not (events := sel.select(interval)):
            return self.restimeout
        for key, _ in events:
            return key.fileobj.readline().rstrip()

    def _wait_for_queue(self, interval: Optional[int] = None):
        try:
            if self.inputq:
                return self.inputq.get(block=True, timeout=interval)
        except queue.Empty:
            return self.restimeout
        except Exception as e:
            self.logger.exception(repr(e))

    @run_operation_in_executor(name="inptmout")
    def countdown(self, *args, **kwargs):
        exit_event = cast(MySyncAsyncEvent, kwargs["stop_event"])

        if not self.inputq:
            termios.tcflush(sys.stdin, termios.TCIFLUSH)
            self.enable_echo(False)
            sel = selectors.DefaultSelector()
            sel.register(sys.stdin, selectors.EVENT_READ, data=sys.stdin)
            self.wait_for = functools.partial(self._wait_for_enter, sel)
        else:
            self.wait_for = self._wait_for_queue

        _input = "error"
        try:
            start = time.monotonic()
            while time.monotonic() - start < self.timeout:
                try:
                    _input = self.wait_for(1)

                    if _input in ["exit", "", str(self.indexdl)]:
                        break
                    self.check()
                    self.pb.update()
                    self.pb.print("Waiting")

                except Exception as e:
                    self.logger.exception(repr(e))
                    _input = "error"
                    break
        finally:
            if not self.inputq:
                self.enable_echo(True)
            _input = cast(str, _input)
            exit_event.set(_input)

    def __call__(self):
        self.exit_event, _ = self.countdown()
        self.exit_event.wait()
        return self.exit_event.is_set()


if PySimpleGUI:
    sg = PySimpleGUI

    class FrontEndGUI:
        _PASRES_REPEAT = False
        _PASRES_EXIT = MySyncAsyncEvent("pasresexit")
        _LOCK = threading.Lock()
        _MAP = {
            "manip": ("init_manipulating", "manipulating"),
            "finish": ("error", "done", "stop"),
            "init": "init",
            "downloading": "downloading"}

        def __init__(self, asyncdl):
            self.asyncdl = asyncdl
            self.logger = logging.getLogger("FEgui")
            self.list_finish = {}
            self.console_dl_status = False
            if self.asyncdl.args.rep_pause:
                FrontEndGUI._PASRES_REPEAT = True

            self.pasres_time_from_resume_to_pause = 35
            self.pasres_time_in_pause = 8
            self.reset_repeat = False
            self.list_all_old = {"init": {}, "downloading": {}, "manip": {}, "finish": {}}

            self.stop = MySyncAsyncEvent("stopfegui")
            self.exit_gui = MySyncAsyncEvent("exitgui")

            self.list_upt = {}
            self.list_res = {}
            self.stop_upt_window, self.fut_upt_window = self.upt_window_periodic()
            self.asyncdl.add_task(self.fut_upt_window)
            self.exit_upt = MySyncAsyncEvent("exitupt")
            self.stop_pasres, self.fut_pasres = self.pasres_periodic()
            self.asyncdl.add_task(self.fut_pasres)
            self.exit_pasres = MySyncAsyncEvent("exitpasres")

            self.task_gui = self.asyncdl.add_task(self.gui())

        @classmethod
        def pasres_break(cls):
            with FrontEndGUI._LOCK:
                if not FrontEndGUI._PASRES_REPEAT:
                    return False
                FrontEndGUI._PASRES_REPEAT = False
                FrontEndGUI._PASRES_EXIT.set()
                time.sleep(1)
                return True

        @classmethod
        def pasres_continue(cls):
            with FrontEndGUI._LOCK:
                if not FrontEndGUI._PASRES_REPEAT:
                    FrontEndGUI._PASRES_EXIT.clear()
                    FrontEndGUI._PASRES_REPEAT = True

        async def gui_root(self, event, values):

            def _handle_all(values):
                self.window_root["ST"].update(values["all"]["nwmon"])
                if "init" in values["all"]:
                    if list_init := values["all"]["init"]:
                        upt = "\n\n" + "".join(
                            [el[1] for el in sorted(list(list_init.values()), key=lambda x: x[0])])
                    else:
                        upt = ""
                    self.window_root["-ML0-"].update(value=upt)
                if "downloading" in values["all"]:
                    if list_downloading := values["all"]["downloading"]:
                        upt = "\n\n" + "".join(list((list_downloading.values())))
                    else:
                        upt = ""
                    self.window_root["-ML1-"].update(value=upt)
                    if self.console_dl_status:
                        upt = "\n".join(list_downloading.values())
                        sg.cprint(
                            f"\n\n-------STATUS DL----------------\n\n{upt}"
                            + "\n\n-------END STATUS DL------------\n\n")
                        self.console_dl_status = False
                if "manipulating" in values["all"]:
                    _text = []
                    if list_manipulating := values["all"]["manipulating"]:
                        _text.extend(["\n\n-------CREATING FILE------------\n\n"])
                        _text.extend(list(list_manipulating.values()))
                    upt = "".join(_text) if _text else ""
                    self.window_root["-ML3-"].update(value=upt)
                if "finish" in values["all"]:
                    self.list_finish = values["all"]["finish"]
                    if self.list_finish:
                        upt = "\n\n" + "".join(list(self.list_finish.values()))
                    else:
                        upt = ""
                    self.window_root["-ML2-"].update(value=upt)

            try:
                if "kill" in event or event == sg.WIN_CLOSED:
                    return "break"
                elif event == "nwmon":
                    self.window_root["ST"].update(values["nwmon"])
                elif event == "all":
                    _handle_all(values)
                elif event in ("error", "done", "stop"):
                    self.list_finish.update(values[event])
                    if self.list_finish:
                        upt = "\n\n" + "".join(list(self.list_finish.values()))
                    else:
                        upt = ""
                    self.window_root["-ML2-"].update(value=upt)
            except Exception as e:
                self.logger.exception(f"[gui_root] {repr(e)}")

        async def gui_console(self, event, values):
            async def _handle_timepasres(values):
                if not values["-IN-"]:
                    sg.cprint(
                        "[pause-resume autom] Please enter timers [time to resume:"
                        + f"{self.pasres_time_from_resume_to_pause}],"
                        + f"[time in pause:{self.pasres_time_in_pause}]"
                        + f"\nDL in pasres: {list(self.asyncdl.list_pasres)}"
                    )
                else:
                    timers = [timer.strip() for timer in values["-IN-"].split(",")]
                    if len(timers) > 2:
                        sg.cprint("[pause-resume autom] max 2 timers")
                    elif any(
                        (not timer.isdecimal() or int(timer) < 0) for timer in timers
                    ):
                        sg.cprint("[pause-resume autom] not an integer, or negative")
                    else:
                        self.pasres_time_from_resume_to_pause = int(timers[0])
                        self.pasres_time_in_pause = (
                            int(timers[1]) if len(timers) == 2 else int(timers[0])
                        )
                        sg.cprint(
                            f"[pause-resume autom] [time to resume] {self.pasres_time_from_resume_to_pause} "
                            + f"[time in pause] {self.pasres_time_in_pause}")

                    self.window_console["-IN-"].update(value="")

            async def _handle_numvideoworkers(values):
                if not values["-IN-"]:
                    sg.cprint("Please enter number")
                else:
                    if not values["-IN-"].split(",")[0].isdecimal():
                        sg.cprint("#vidworkers not an integer")
                    else:
                        _nvidworkers = int(values["-IN-"].split(",")[0])
                        if _nvidworkers <= 0:
                            sg.cprint("#vidworkers must be > 0")
                        elif self.asyncdl.list_dl:
                            _copy_list_dl = self.asyncdl.list_dl.copy()
                            if "," in values["-IN-"]:
                                _ind = int(values["-IN-"].split(",")[1])
                                if _ind in _copy_list_dl:
                                    await _copy_list_dl[_ind].change_numvidworkers(_nvidworkers)
                                else:
                                    sg.cprint("DL index doesnt exist")
                            else:
                                self.asyncdl.args.parts = _nvidworkers
                                for _, dl in _copy_list_dl.items():
                                    await dl.change_numvidworkers(_nvidworkers)
                        else:
                            sg.cprint("DL list empty")
                    self.window_console["-IN-"].update(value="")

            async def _handle_event(values):
                if not self.asyncdl.list_dl:
                    sg.cprint("DL list empty")
                else:
                    _copy_list_dl = self.asyncdl.list_dl.copy()
                    _index_list = []
                    if _values := values.get(event):  # from thread pasres
                        _index_list = [int(el) for el in _values.split(",")]
                    elif not (_values := values["-IN-"]) or _values.lower() == "all":
                        _index_list = [int(dl.index) for _, dl in _copy_list_dl.items()]
                        self.window_console["-IN-"].update(value="")
                    else:
                        if any(
                            any(
                                [
                                    not el.isdecimal(),
                                    int(el) == 0,
                                    int(el) > len(_copy_list_dl),
                                ]
                            )
                            for el in values["-IN-"].replace(" ", "").split(",")
                        ):
                            sg.cprint("incorrect numbers of dl")
                        else:
                            _index_list = [int(el) for el in values["-IN-"].replace(" ", "").split(",")]

                        self.window_console["-IN-"].update(value="")

                    if _index_list:
                        if event in ["+PasRes", "-PasRes"]:
                            sg.cprint(f"[pause-resume autom] before: {list(self.asyncdl.list_pasres)}")

                        info = []
                        for _index in _index_list:
                            if event == "MoveTopWaitingDL":
                                if not self.asyncdl.WorkersInit.exit.is_set():
                                    sg.cprint(
                                        "[move to top waiting list] cant process until every video " +
                                        "has been checked by init")
                                else:
                                    await self.asyncdl.WorkersRun.move_to_waiting_top(_index)
                            if event == "StopCount":
                                CountDowns._INPUT.put_nowait(str(_index))
                            elif event == "+PasRes":
                                if self.asyncdl.list_dl[_index].info_dl["status"] in ("init", "downloading"):
                                    self.asyncdl.list_pasres.add(_index)
                            elif event == "-PasRes":
                                self.asyncdl.list_pasres.discard(_index)
                            elif event == "Pause":
                                await self.asyncdl.list_dl[_index].pause()
                            elif event == "Resume":
                                await self.asyncdl.list_dl[_index].resume()
                            elif event == "Reset":
                                await self.asyncdl.list_dl[_index].reset_from_console()
                            elif event == "Stop":
                                await self.asyncdl.list_dl[_index].stop("exit")
                            elif event in ["Info", "ToFile"]:
                                _info = json.dumps(self.asyncdl.list_dl[_index].info_dict)
                                sg.cprint(f"[{_index}] info\n{_info}")
                                sg.cprint(
                                    f'[{_index}] filesize[{self.asyncdl.list_dl[_index].info_dl["downloaders"][0].filesize}]'
                                    + f'downsize[{self.asyncdl.list_dl[_index].info_dl["downloaders"][0].down_size}]'
                                    + f"pause[{self.asyncdl.list_dl[_index].pause_event.is_set()}]"
                                    + f"resume[{self.asyncdl.list_dl[_index].resume_event.is_set()}]"
                                    + f"stop[{self.asyncdl.list_dl[_index].stop_event.is_set()}]"
                                    + f"reset[{self.asyncdl.list_dl[_index].reset_event.is_set()}]")

                                info.append(_info)

                        if event in ["+PasRes", "-PasRes"]:
                            sg.cprint(f"[pause-resume autom] after: {list(self.asyncdl.list_pasres)}")

                        if event == "ToFile":
                            _launch_time = self.asyncdl.launch_time.strftime("%Y%m%d_%H%M")
                            _file = Path(Path.home(), "testing", f"{_launch_time}.json")
                            _data = {"entries": info}
                            with open(_file, "w") as f:
                                f.write(json.dumps(_data))
                            sg.cprint(f"saved to file: {_file}")

            sg.cprint(event, values)
            if event == sg.WIN_CLOSED:
                return "break"
            elif event in ["Exit"]:
                self.logger.debug("[gui_console] event Exit")
                await self.asyncdl.cancel_all_dl()
            elif event in ["-PASRES-"]:
                FrontEndGUI._PASRES_REPEAT = bool(values["-PASRES-"])
            elif event in ["-RESETREP-"]:
                self.reset_repeat = bool(values["-RESETREP-"])
            elif event in ["-DL-STATUS"]:
                self.asyncdl.print_pending_tasks()
                if not self.console_dl_status:
                    self.console_dl_status = True
            elif event in ["IncWorkerRun"]:
                await self.asyncdl.WorkersRun.add_worker()
                sg.cprint(f"Workers: {self.asyncdl.WorkersRun.max_workers}")
            elif event in ["DecWorkerRun"]:
                await self.asyncdl.WorkersRun.del_worker()
                sg.cprint(f"Workers: {self.asyncdl.WorkersRun.max_workers}")
            elif event in ["TimePasRes"]:
                await _handle_timepasres(values)
            elif event in ["NumVideoWorkers"]:
                await _handle_numvideoworkers(values)
            elif event in [
                    "ToFile", "Info", "Pause", "Resume", "Reset",
                    "Stop", "+PasRes", "-PasRes", "StopCount",
                    "MoveTopWaitingDL"]:
                await _handle_event(values)

        async def gui(self):
            try:
                self.window_console = self.init_gui_console()
                self.window_root = self.init_gui_root()
                await asyncio.sleep(0)
                while not self.stop.is_set():
                    try:
                        window, event, values = sg.read_all_windows(timeout=0)
                        if not window or not event or event == sg.TIMEOUT_KEY:
                            await asyncio.sleep(0)
                            continue
                        _res = []
                        if window == self.window_console:
                            _res.append(await self.gui_console(event, values))
                        elif window == self.window_root:
                            _res.append(await self.gui_root(event, values))
                        if "break" in _res:
                            break
                        await asyncio.sleep(0)
                    except asyncio.CancelledError as e:
                        self.logger.warning(f"[gui] inner exception {repr(e)}")
                        raise
                    except Exception as e:
                        self.logger.exception(f"[gui] inner exception {repr(e)}")

            except Exception as e:
                self.logger.warning(f"[gui] outer exception {repr(e)}")
            finally:
                self.exit_gui.set()
                self.logger.debug("[gui] BYE")

        def get_window(self, name, layout, location=None, ml_keys=None, to_front=False):
            if not location:
                location = (0, 0)

            window = sg.Window(
                name,
                layout,
                alpha_channel=0.99,
                location=location,
                finalize=True,
                resizable=True)

            window.set_min_size(window.size)
            if ml_keys:
                for key in ml_keys:
                    window[key].expand(True, True, True)
            if to_front:
                window.bring_to_front()
            return window

        def init_gui_root(self):
            sg.theme("SystemDefaultForReal")

            col_0 = sg.Column(
                [
                    [sg.Text("WAITING TO DL", font="Any 14")],
                    [
                        sg.Multiline(
                            default_text="Waiting for info",
                            size=(70, 40),
                            font=("Courier New Bold", 10),
                            write_only=True,
                            key="-ML0-",
                            autoscroll=True,
                            auto_refresh=True,
                        )
                    ],
                ],
                element_justification="l",
                expand_x=True,
                expand_y=True,
            )

            col_00 = sg.Column(
                [
                    [
                        sg.Text(
                            "Waiting for info",
                            size=(80, 2),
                            font=("Courier New Bold", 12),
                            key="ST",
                        )
                    ]
                ]
            )

            col_1 = sg.Column(
                [
                    [sg.Text("NOW DOWNLOADING/CREATING FILE", font="Any 14")],
                    [
                        sg.Multiline(
                            default_text="Waiting for info",
                            size=(90, 35),
                            font=("Courier New Bold", 11),
                            write_only=True,
                            key="-ML1-",
                            autoscroll=True,
                            auto_refresh=True,
                        )
                    ],
                    [
                        sg.Multiline(
                            default_text="Waiting for info",
                            size=(90, 5),
                            font=("Courier New Bold", 10),
                            write_only=True,
                            key="-ML3-",
                            autoscroll=True,
                            auto_refresh=True,
                        )
                    ],
                ],
                element_justification="c",
                expand_x=True,
                expand_y=True,
            )

            col_2 = sg.Column(
                [
                    [sg.Text("DOWNLOADED/STOPPED/ERRORS", font="Any 14")],
                    [
                        sg.Multiline(
                            default_text="Waiting for info",
                            size=(70, 40),
                            font=("Courier New Bold", 10),
                            write_only=True,
                            key="-ML2-",
                            autoscroll=True,
                            auto_refresh=True,
                        )
                    ],
                ],
                element_justification="r",
                expand_x=True,
                expand_y=True,
            )

            layout_root = [[col_00], [col_0, col_1, col_2]]

            return self.get_window(
                "async_downloader", layout_root, ml_keys=[f"-ML{i}-" for i in range(4)])
            # sg.Window(
            #     "async_downloader",
            #     layout_root,
            #     alpha_channel=0.99,
            #     location=(0, 0),
            #     finalize=True,
            #     resizable=True)

            # window_root.set_min_size(window_root.size)

            # window_root["-ML0-"].expand(True, True, True)
            # window_root["-ML1-"].expand(True, True, True)
            # window_root["-ML2-"].expand(True, True, True)
            # window_root["-ML3-"].expand(True, True, True)

            # return window_root

        def init_gui_console(self):
            sg.theme("SystemDefaultForReal")

            col_pygui = sg.Column(
                [
                    [sg.Text("Select DL", font="Any 14")],
                    [sg.Input(key="-IN-", font="Any 10", focus=True)],
                    [
                        sg.Multiline(
                            size=(50, 12),
                            font="Any 10",
                            write_only=True,
                            key="-ML-",
                            reroute_cprint=True,
                            auto_refresh=True,
                            autoscroll=True)
                    ],
                    [
                        sg.Checkbox(
                            "PauseRep",
                            key="-PASRES-",
                            default=FrontEndGUI._PASRES_REPEAT,
                            enable_events=True),
                        sg.Checkbox(
                            "ResRep",
                            key="-RESETREP-",
                            default=False,
                            enable_events=True),
                        sg.Button("+PasRes"),
                        sg.Button("-PasRes"),
                        sg.Button("DLStatus", key="-DL-STATUS"),
                        sg.Button("Info"),
                        sg.Button("ToFile"),
                        sg.Button("+runwk", key="IncWorkerRun"),
                        sg.Button("-runwk", key="DecWorkerRun"),
                        sg.Button("#vidwk", key="NumVideoWorkers"),
                        sg.Button("MoveTop", key="MoveTopWaitingDL"),
                        sg.Button("StopCount"),
                        sg.Button("TimePasRes"),
                        sg.Button("Pause"),
                        sg.Button("Resume"),
                        sg.Button("Reset"),
                        sg.Button("Stop"),
                        sg.Button("Exit"),
                    ],
                ],
                element_justification="c",
                expand_x=True,
                expand_y=True)

            layout_pygui = [[col_pygui]]

            return self.get_window(
                "Console", layout_pygui, location=(0, 500), ml_keys=["-ML-"], to_front=True)
            # window_console = sg.Window(
            #     "Console",
            #     layout_pygui,
            #     alpha_channel=0.99,
            #     location=(0, 500),
            #     finalize=True,
            #     resizable=True)

            # window_console.set_min_size(window_console.size)
            # window_console["-ML-"].expand(True, True, True)
            # window_console.bring_to_front()

            # return window_console

        def _upt_status(self, st, _copy_list_dl, _waiting, _running):
            self.list_upt[st] = {}
            self.list_res[st] = {}
            if st == "init":
                _list_items = _waiting
                for i, index in enumerate(_list_items):
                    if self.asyncdl.list_dl[index].info_dl["status"] in self._MAP[st]:
                        self.list_res[st].update({index: (i, self.asyncdl.list_dl[index].print_hookup())})
            else:
                _list_items = _copy_list_dl if st != "downloading" else _running
                for index in _list_items:
                    if self.asyncdl.list_dl[index].info_dl["status"] in self._MAP[st]:
                        self.list_res[st].update({index: self.asyncdl.list_dl[index].print_hookup()})

            if self.list_res[st] == self.list_all_old[st]:
                del self.list_upt[st]
            else:
                self.list_upt[st] = self.list_res[st]

        def update_window(self, status=None, nwmon=None):

            if status in ["all", None]:
                _status = ("init", "downloading", "manip", "finish")
            else:
                _status = variadic(status)

            self.list_upt = {}
            self.list_res = {}
            if nwmon:
                self.list_upt["nwmon"] = nwmon

            if self.asyncdl.list_dl:
                _copy_list_dl = self.asyncdl.list_dl.copy()
                _waiting = list(self.asyncdl.WorkersRun.waiting).copy()
                _running = list(self.asyncdl.WorkersRun.running).copy()
                for st in _status:
                    self._upt_status(st, _copy_list_dl, _waiting, _running)

            if self.list_upt and getattr(self, "window_root", None):
                self.window_root.write_event_value("all", self.list_upt)

            for st, val in self.list_all_old.items():
                if st not in self.list_res:
                    self.list_res[st] = val

            self.list_all_old = self.list_res.copy()

        @run_operation_in_executor_from_loop(name="uptwinthr")
        def upt_window_periodic(self, *args, **kwargs):
            self.logger.debug("[upt_window_periodic] start")
            stop_upt = kwargs["stop_event"]
            try:
                progress_timer = ProgressTimer()
                short_progress_timer = ProgressTimer()
                self.list_nwmon = []
                io_init = psutil.net_io_counters()
                speedometer = SpeedometerMA(initial_bytes=io_init.bytes_recv, ave_time=0.5, smoothing=0.9)
                ds = None
                while not stop_upt.is_set():
                    if progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI):
                        io_upt = psutil.net_io_counters()
                        ds = speedometer(io_upt.bytes_recv)
                        msg = f"RECV: {naturalsize(speedometer.rec_bytes, binary=True)}  "
                        msg += f'DL: {naturalsize(ds, binary=True, format_="7.3f") + "ps" if ds > 1024 else "--"}'
                        self.update_window("all", nwmon=msg)
                        if short_progress_timer.has_elapsed(seconds=10 * CONF_INTERVAL_GUI):
                            self.list_nwmon.append((datetime.now(), ds))
                    else:
                        time.sleep(CONF_INTERVAL_GUI / 4)

            except Exception as e:
                self.logger.exception(f"[upt_window_periodic]: error: {repr(e)}")
            finally:
                # if self.list_nwmon:
                #     try:
                #         # def _strdate(el):
                #         #     _secs = el[0].second + (el[0].microsecond / 1000000)
                #         #     return f'{el[0].strftime("%H:%M:")}{_secs:06.3f}'

                #         # _str_nwmon = ', '.join([f'{_strdate(el)}' for el in self.list_nwmon])
                #         # self.logger.debug(
                #         #     f'[upt_window_periodic] nwmon {len(self.list_nwmon)}]\n{_str_nwmon}')
                #     except Exception as e:
                #         self.logger.exception(f'[upt_window_periodic] {repr(e)}')

                self.exit_upt.set()
                self.logger.debug("[upt_window_periodic] BYE")

        def get_dl_media(self):
            if self.list_nwmon:
                _speed_data = [el[1] for el in self.list_nwmon]
                _media = naturalsize(median(_speed_data), binary=True)
                return f"DL MEDIA: {_media}ps"

        @run_operation_in_executor_from_loop(name="pasresthr")
        def pasres_periodic(self, *args, **kwargs):
            self.logger.debug("[pasres_periodic] START")
            stop_event = kwargs["stop_event"]
            _start_no_pause = None
            try:
                while not stop_event.is_set():
                    if self.asyncdl.list_pasres and FrontEndGUI._PASRES_REPEAT:
                        _waitres_nopause = wait_for_either(
                            [stop_event, FrontEndGUI._PASRES_EXIT],
                            timeout=self.pasres_time_from_resume_to_pause)
                        FrontEndGUI._PASRES_EXIT.clear()
                        if not FrontEndGUI._PASRES_REPEAT or not self.asyncdl.list_pasres:
                            continue
                        if _waitres_nopause == "TIMEOUT" and (_list := list(self.asyncdl.list_pasres)):
                            if not self.reset_repeat:
                                if _start_no_pause:
                                    sg.cprint(f"[time resume -> pause] {time.monotonic()-_start_no_pause}")

                                self.window_console.write_event_value(
                                    "Pause", ",".join(list(map(str, _list))))
                                time.sleep(1)
                                self.logger.debug("[pasres_periodic]: pauses sent")
                                _start_pause = time.monotonic()
                                _waitres = wait_for_either(
                                    [stop_event, FrontEndGUI._PASRES_EXIT], timeout=self.pasres_time_in_pause)
                                FrontEndGUI._PASRES_EXIT.clear()
                                self.logger.debug("[pasres_periodic]: start sending resumes")
                                if _waitres == "TIMEOUT":
                                    _time = self.pasres_time_in_pause / len(_list)
                                    for i, _el in enumerate(_list):
                                        self.window_console.write_event_value("Resume", str(_el))

                                        if i + 1 < len(_list):
                                            _waitres = wait_for_either(
                                                [stop_event, FrontEndGUI._PASRES_EXIT],
                                                timeout=random.uniform(0.75 * _time, 1.25 * _time))
                                            FrontEndGUI._PASRES_EXIT.clear()
                                            if _waitres != "TIMEOUT":
                                                self.window_console.write_event_value(
                                                    "Resume", ",".join(list(map(str, _list[i + 1:]))))

                                                break

                                else:
                                    self.window_console.write_event_value(
                                        "Resume", ",".join(list(map(str, _list))))

                                self.logger.debug(
                                    "[pasres_periodic]: resumes sent, start timer to next pause")

                                sg.cprint(f"[time in pause] {time.monotonic()-_start_pause}")
                                _start_no_pause = time.monotonic()

                            else:
                                self.window_console.write_event_value(
                                    "Reset", ",".join(list(map(str, _list))))
                    else:
                        _start_no_pause = None
                        time.sleep(CONF_INTERVAL_GUI)

            except Exception as e:
                self.logger.exception(f"[pasres_periodic]: error: {repr(e)}")
            finally:
                self.exit_pasres.set()
                self.logger.debug("[pasres_periodic] BYE")

        async def close(self):
            self.stop_pasres.set()
            await asyncio.sleep(0)
            self.stop_upt_window.set()
            await asyncio.sleep(0)
            self.logger.debug("[close] start to wait for uptwindows and pasres")

            await asyncio.wait([self.fut_upt_window, self.fut_pasres])

            self.logger.debug("[close] end to wait for uptwindows and pasres")

            self.stop.set()
            await asyncio.sleep(0)
            self.logger.debug("[close] start to wait for exit_gui")
            await self.exit_gui.async_wait()
            self.logger.debug("[close] end to wait for exit_gui")
            if hasattr(self, "window_console") and self.window_console:
                self.window_console.close()
                del self.window_console
            if hasattr(self, "window_root") and self.window_root:
                self.window_root.close()
                del self.window_root


class NWSetUp:

    if proxy:
        Proxy = proxy.Proxy

    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger("setupnw")
        self.shutdown_proxy = MySyncAsyncEvent("shutdownproxy")
        self.init_ready = MySyncAsyncEvent("initready")
        self.routing_table = {}
        self.proc_gost = []
        self.proc_aria2c = None
        self.exe = ThreadPoolExecutor(thread_name_prefix="setupnw")

        self._tasks_init = {}
        if not self.asyncdl.args.nodl:
            if self.asyncdl.args.aria2c:
                ainit_aria2c = sync_to_async(
                    init_aria2c, thread_sensitive=False, executor=self.exe)
                _task_aria2c = self.asyncdl.add_task(
                    ainit_aria2c(self.asyncdl.args))
                _tasks_init_aria2c = {_task_aria2c: "aria2"}
                self._tasks_init |= _tasks_init_aria2c
            if self.asyncdl.args.enproxy:
                self.stop_proxy, self.fut_proxy = self.run_proxy_http()
                self.asyncdl.add_task(self.fut_proxy)
                ainit_proxies = sync_to_async(
                    TorGuardProxies.init_proxies, thread_sensitive=False, executor=self.exe)
                _task_proxies = self.asyncdl.add_task(
                    ainit_proxies(event=self.asyncdl.end_dl))
                _task_init_proxies = {_task_proxies: "proxies"}
                self._tasks_init |= _task_init_proxies
        if self._tasks_init:
            self.task_init = self.asyncdl.add_task(self.init())
        else:
            self.init_ready.set()

    async def init(self):
        if not self._tasks_init:
            return
        done, _ = await asyncio.wait(self._tasks_init)
        for task in done:
            try:
                if self._tasks_init[task] == "aria2":
                    self.proc_aria2c = task.result()
                else:
                    self.proc_gost, self.routing_table = task.result()
                    self.asyncdl.ytdl.params["routing_table"] = self.routing_table
            except Exception as e:
                self.logger.exception(f"[init] {repr(e)}")
        self.init_ready.set()

    @run_operation_in_executor_from_loop(name="proxythr")
    def run_proxy_http(self, *args, **kwargs):
        stop_event: MySyncAsyncEvent = kwargs["stop_event"]
        log_level = kwargs.get("log_level", "INFO")
        try:
            with NWSetUp.Proxy(
                [
                    "--log-level",
                    log_level,
                    "--plugins",
                    "plugins.ProxyPoolByHostPlugin",
                ]
            ) as p:
                try:
                    self.logger.debug(p.flags)
                    stop_event.wait()
                except BaseException:
                    self.logger.error("context manager proxy")
        finally:
            self.shutdown_proxy.set()

    async def close(self):
        if self.asyncdl.args.enproxy:
            self.logger.debug("[close] proxy")
            self.stop_proxy.set()
            await asyncio.sleep(0)
            self.shutdown_proxy.wait()
            self.logger.debug("[close] OK shutdown")

            await asyncio.gather()

            if self.proc_gost:
                self.logger.debug("[close] gost")
                for proc in self.proc_gost:
                    proc.terminate()
                    try:
                        if proc.stdout:
                            proc.stdout.close()
                        if proc.stderr:
                            proc.stderr.close()
                        if proc.stdin:
                            proc.stdin.close()
                    except Exception:
                        pass
                    finally:
                        await sync_to_async(
                            proc.wait, thread_sensitive=False, executor=self.exe)()
                        await asyncio.sleep(0)

        if self.proc_aria2c:
            self.logger.debug("[close] aria2c")
            self.proc_aria2c.terminate()
            try:
                if self.proc_aria2c.stdout:
                    self.proc_aria2c.stdout.close()
                if self.proc_aria2c.stderr:
                    self.proc_aria2c.stderr.close()
                if self.proc_aria2c.stdin:
                    self.proc_aria2c.stdin.close()
            except Exception:
                pass
            finally:
                await sync_to_async(self.proc_aria2c.wait, thread_sensitive=False, executor=self.exe)()

    async def reset_aria2c(self):
        if not self.proc_aria2c:
            return
        self.logger.debug("[close] aria2c")
        self.proc_aria2c.terminate()
        try:
            if self.proc_aria2c.stdout:
                self.proc_aria2c.stdout.close()
            if self.proc_aria2c.stderr:
                self.proc_aria2c.stderr.close()
            if self.proc_aria2c.stdin:
                self.proc_aria2c.stdin.close()
        except Exception:
            pass
        finally:
            await sync_to_async(
                self.proc_aria2c.wait, thread_sensitive=False, executor=self.exe)()

        ainit_aria2c = sync_to_async(
            init_aria2c, thread_sensitive=False, executor=self.exe)
        _task_aria2c = [self.asyncdl.add_task(ainit_aria2c(self.asyncdl.args))]
        done, _ = await asyncio.wait(_task_aria2c)
        for task in done:
            self.proc_aria2c = task.result()
            return (self.proc_aria2c, self.asyncdl.args.rpcport)


@dataclass
class InfoDL:
    pause_event: MySyncAsyncEvent
    resume_event: MySyncAsyncEvent
    stop_event: MySyncAsyncEvent
    end_tasks: MySyncAsyncEvent
    reset_event: MySyncAsyncEvent
    total_sizes: dict
    nwsetup: NWSetUp

    def clear(self):
        self.pause_event.clear()
        self.resume_event.clear()
        self.stop_event.clear()
        self.end_tasks.clear()
        self.reset_event.clear()


if FileLock and xattr:

    class LocalStorage:

        lock = FileLock(Path(PATH_LOGS, "files_cached.json.lock"))
        local_storage = Path(PATH_LOGS, "files_cached.json")
        prev_local_storage = Path(PATH_LOGS, "prev_files_cached.json")

        config_folders = {
            "local": Path(Path.home(), "testing"),
            "pandaext4": Path("/Volumes/Pandaext4/videos"),
            "pandaext1": Path("/Volumes/Pandaext1/videos"),
            "datostoni": Path("/Volumes/DatosToni/videos"),
            "wd1b": Path("/Volumes/WD1B/videos"),
            "wd5": Path("/Volumes/WD5/videos"),
            "wd8_1": Path("/Volumes/WD8_1/videos"),
            "wd8_2": Path("/Volumes/WD8_2/videos"),
            "t7": Path("/Volumes/T7/videos")
        }

        def __init__(self):
            self._data_from_file = {}  # data struct per vol
            self._data_for_scan = {}  # data ready for scan
            self._last_time_sync = {}
            self.logger = logging.getLogger("LocalStorage")

        def load_info(self):
            """
            Load from file
            """

            with open(LocalStorage.local_storage, "r") as f:
                self._data_from_file = json.load(f)

            for _key, _data in self._data_from_file.items():
                if _key in list(LocalStorage.config_folders.keys()):
                    self._data_for_scan.update(_data)
                elif "last_time_sync" in _key:
                    self._last_time_sync.update(_data)
                else:
                    self.logger.error(f"found key not registered volumen - {_key}")

        def dump_info(self, videos_cached, last_time_sync, local=False):
            """ "
            Dump videos_cached info to FileExistsError
            """

            def getter(x):
                if "Pandaext4/videos" in x:
                    return "pandaext4"
                elif "Pandaext1/videos" in x:
                    return "pandaext1"
                elif "WD5/videos" in x:
                    return "wd5"
                elif "WD1B/videos" in x:
                    return "wd1b"
                elif "antoniotorres/testing" in x:
                    return "local"
                elif "DatosToni/videos" in x:
                    return "datostoni"
                elif "WD8_1/videos" in x:
                    return "wd8_1"
                elif "WD8_2/videos" in x:
                    return "wd8_2"
                elif "T7/videos" in x:
                    return "t7"

            if videos_cached:
                self._data_for_scan = videos_cached.copy()
            if last_time_sync:
                self._last_time_sync = last_time_sync.copy()

            _upt_temp = {
                "last_time_sync": {},
                "local": {},
                "wd5": {},
                "wd1b": {},
                "pandaext4": {},
                "pandaext1": {},
                "datostoni": {},
                "wd8_1": {},
                "wd8_2": {},
                "t7": {}
            }

            _upt_temp["last_time_sync"] = last_time_sync

            for key, val in videos_cached.items():
                _vol = getter(val)
                if not _vol:
                    self.logger.error(f"found file with not registered volumen - {val} - {key}")
                else:
                    _upt_temp[_vol].update({key: val})

            shutil.copy(str(LocalStorage.local_storage), str(LocalStorage.prev_local_storage))

            if not local:
                with open(LocalStorage.local_storage, "w") as f:
                    json.dump(_upt_temp, f)

            else:
                with open(LocalStorage.local_storage, "r") as f:
                    _temp = json.load(f)

                _temp["local"] = _upt_temp["local"]
                _temp["last_time_sync"]["local"] = _upt_temp["last_time_sync"]["local"]

                with open(LocalStorage.local_storage, "w") as f:
                    json.dump(_temp, f)

            self._data_from_file = {}  # data struct per vol
            self._data_for_scan = {}  # data ready for scan
            self._last_time_sync = {}

    def getxattr(x):
        return upartial(xattr.getxattr, attr="user.dublincore.description")(x).decode()

    def _getxattr(f):
        with contextlib.suppress(OSError):
            return re.sub(r"(\?alt=yes$)", "", getxattr(f))

    class LocalVideos:
        def __init__(self, asyncdl, deep=False):
            self.asyncdl = asyncdl
            self.logger = logging.getLogger("videoscached")
            self.deep = deep
            self._videoscached = {}
            self._repeated = []
            self._dont_exist = []
            self._repeated_by_xattr = []
            self._localstorage = LocalStorage()
            self.ready_videos_cached, self.fut_videos_cached = self.get_videos_cached()

        async def aready(self):
            while not self.ready_videos_cached.is_set():
                await asyncio.sleep(0)

        def ready(self):
            self.ready_videos_cached.wait()

        def upt_local(self):
            self.ready_videos_cached.clear()
            self._videoscached = {}
            self._repeated = []
            self._dont_exist = []
            self._repeated_by_xattr = []
            self.ready_videos_cached, self.fut_videos_cached = self.get_videos_cached(local=True)
            self.ready()

        @run_operation_in_executor(name="vidcach")
        def get_videos_cached(self, *args, **kwargs):
            """
            In local storage, files are saved wihtin the file files.cached.json
            in 5 groups each in different volumnes.
            If any of the volumes can't be accesed in real time, the
            local storage info of that volume will be used.
            """

            def insert_videoscached(_text, _file):
                if not (_video_path_str := self._videoscached.get(_text)):
                    self._videoscached.update({_text: str(_file)})

                else:
                    _video_path = Path(_video_path_str)
                    if _video_path != _file:
                        if not _file.is_symlink() and not _video_path.is_symlink():
                            # only if both are hard files we have
                            # to do something, so lets report it
                            # in repeated files
                            self._repeated.append(
                                {
                                    "text": _text,
                                    "indict": _video_path_str,
                                    "file": str(_file),
                                }
                            )

                        if self.deep:
                            self.deep_check(_text, _file, _video_path)

            _finished: MySyncAsyncEvent = kwargs["stop_event"]

            force_local = kwargs.get("local", False)

            self.logger.debug(
                f"[videos_cached] start scanning - dlcaching[{self.asyncdl.args.dlcaching}] - local[{force_local}]"
            )

            last_time_sync = {}

            try:
                with self._localstorage.lock:
                    self._localstorage.load_info()

                    list_folders_to_scan = {}

                    last_time_sync = self._localstorage._last_time_sync

                    if not self.asyncdl.args.dlcaching or force_local:
                        for _vol, _folder in self._localstorage.config_folders.items():
                            if _vol != "local":
                                if not force_local:
                                    self._videoscached.update(self._localstorage._data_from_file[_vol])
                            else:
                                list_folders_to_scan[_folder] = _vol

                    else:
                        for _vol, _folder in self._localstorage.config_folders.items():
                            if not _folder.exists():  # comm failure
                                self.logger.error(f"Fail connect to [{_vol}], will use last info")
                                self._videoscached.update(self._localstorage._data_from_file[_vol])
                            else:
                                list_folders_to_scan[_folder] = _vol

                    for folder in list_folders_to_scan:
                        try:
                            files = [
                                file
                                for file in folder.rglob("*")
                                if file.is_file()
                                and not file.stem.startswith(".")
                                and (file.suffix.lower() in (".mp4", ".mkv", ".zip"))
                                and len(file.suffixes) == 1
                            ]

                            for file in files:
                                if not force_local and not file.is_symlink():
                                    with contextlib.suppress(Exception):
                                        _xattr_desc = _getxattr(file)
                                        if _xattr_desc:
                                            if not self._videoscached.get(_xattr_desc):
                                                self._videoscached.update({_xattr_desc: str(file)})
                                            else:
                                                self._repeated_by_xattr.append(
                                                    {
                                                        _xattr_desc: [
                                                            self._videoscached[_xattr_desc],
                                                            str(file),
                                                        ]
                                                    }
                                                )
                                _res = file.stem.split("_", 1)
                                if len(_res) == 2:
                                    _id = _res[0]
                                    _title = sanitize_filename(_res[1], restricted=True).upper()
                                    _name = f"{_id}_{_title}"
                                else:
                                    _id = None
                                    _title = None
                                    _name = sanitize_filename(file.stem, restricted=True).upper()

                                if _id and self.asyncdl.args.deep_aldl:
                                    insert_videoscached(_id, file)
                                else:
                                    insert_videoscached(_name, file)

                        except Exception as e:
                            self.logger.error(f"[videos_cached][{list_folders_to_scan[folder]}]{repr(e)}")

                        else:
                            last_time_sync.update(
                                {
                                    list_folders_to_scan[folder]: str(datetime.now()) if force_local else str(self.asyncdl.launch_time)
                                }
                            )

                    self._localstorage.dump_info(self._videoscached, last_time_sync, local=force_local)

                    self.logger.info(f"[videos_cached] Total videos cached: [{len(self._videoscached)}]")

                    if not force_local:
                        self.asyncdl.videos_cached = self._videoscached.copy()

                    _finished.set()

                    if not force_local:
                        try:
                            if self._repeated:
                                self.logger.warning("[videos_cached] Please check vid rep in logs")
                                self.logger.debug(f"[videos_cached] videos repeated: \n {self._repeated}")

                            if self._dont_exist:
                                self.logger.warning("[videos_cached] Pls check vid dont exist in logs")
                                self.logger.debug(f"[videos_cached] videos dont exist: \n{self._dont_exist}")

                            if self._repeated_by_xattr:
                                self.logger.warning("[videos_cached] Pls check vid repeated by xattr)")
                                self.logger.debug(
                                    f"[videos_cached] videos repeated by xattr: \n{self._repeated_by_xattr}"
                                )

                        except Exception as e:
                            self.logger.exception(f"[videos_cached] {repr(e)}")

            except Exception as e:
                self.logger.exception(f"[videos_cached] {repr(e)}")

        def deep_check(self, _name, file, _video_path):
            if not file.is_symlink() and _video_path.is_symlink():
                _links = get_chain_links(_video_path)
                if _links[-1] == file:
                    if len(_links) > 2:  # chain of at least 2 symlinks
                        self.logger.debug(
                            "[videos_cached_deep]\nfile not symlink: "
                            + f"{str(file)}\nvideopath symlink: "
                            + f"{str(_video_path)}\n\t\t"
                            + f'{" -> ".join([str(_l) for _l in _links])}'
                        )

                        for _link in _links[:-1]:
                            _link.unlink()
                            _link.symlink_to(file)
                            _link._accessor.utime(
                                _link,
                                (int(self.asyncdl.launch_time.timestamp()), file.stat().st_mtime),
                                follow_symlinks=False,
                            )

                        self._videoscached.update({_name: str(file)})

                    else:
                        self.logger.debug(
                            "[videos_cached_deep] \n**file not symlink: "
                            + f"{str(file)}\nvideopath symlink: "
                            + f"{str(_video_path)}\n\t\t"
                            + f'{" -> ".join([str(_l) for _l in _links])}'
                        )

            elif file.is_symlink() and not _video_path.is_symlink():
                _links = get_chain_links(file)
                if _links[-1] == _video_path:
                    if len(_links) > 2:
                        self.logger.debug(
                            "[videos_cached]\nfile symlink: "
                            + f"{str(file)}\n\t\t"
                            + f'{" -> ".join([str(_l) for _l in _links])}\n'
                            + f"videopath not symlink: {str(_video_path)}"
                        )

                        for _link in _links[:-1]:
                            _link.unlink()
                            _link.symlink_to(_video_path)
                            _link._accessor.utime(
                                _link,
                                (int(self.asyncdl.launch_time.timestamp()), _video_path.stat().st_mtime),
                                follow_symlinks=False,
                            )

                    self._videoscached.update({_name: str(_video_path)})
                    if not _video_path.exists():
                        self._dont_exist.append(
                            {
                                "title": _name,
                                "file_not_exist": str(_video_path),
                                "links": [str(_l) for _l in _links[:-1]],
                            }
                        )
                else:
                    self.logger.debug(
                        f"[videos_cached_deep]\n**file symlink: {str(file)}\n"
                        + f'\t\t{" -> ".join([str(_l) for _l in _links])}\n'
                        + f"videopath not symlink: {str(_video_path)}"
                    )

            else:
                _links_file = get_chain_links(file)
                _links_video_path = get_chain_links(_video_path)
                if (_file := _links_file[-1]) == _links_video_path[-1]:
                    if len(_links_file) > 2:
                        self.logger.debug(
                            f"[videos_cached_deep]\nfile symlink: {str(file)}\n"
                            + f'\t\t{" -> ".join([str(_l) for _l in _links_file])}')

                        for _link in _links_file[:-1]:
                            _link.unlink()
                            _link.symlink_to(_file)
                            _link._accessor.utime(
                                _link,
                                (int(self.asyncdl.launch_time.timestamp()), _file.stat().st_mtime),
                                follow_symlinks=False)

                    if len(_links_video_path) > 2:
                        self.logger.debug(
                            "[videos_cached_deep]\nvideopath symlink: "
                            + f"{str(_video_path)}\n\t\t"
                            + f'{" -> ".join([str(_l) for _l in _links_video_path])}')

                        for _link in _links_video_path[:-1]:
                            _link.unlink()
                            _link.symlink_to(_file)
                            _link._accessor.utime(
                                _link,
                                (
                                    int(self.asyncdl.launch_time.timestamp()),
                                    _file.stat().t_mtime,
                                ),
                                follow_symlinks=False,
                            )

                    self._videoscached.update({_name: str(_file)})

                    if not _file.exists():
                        self._dont_exist.append(
                            {
                                "title": _name,
                                "file_not_exist": str(_file),
                                "links": [
                                    str(_l)
                                    for _l in (
                                        _links_file[:-1] + _links_video_path[:-1]
                                    )
                                ],
                            }
                        )

                else:
                    self.logger.debug(
                        "[videos_cached_deep]\n**file symlink: "
                        + f"{str(file)}\n\t\t"
                        + f'{" -> ".join([str(_l) for _l in _links_file])}\n'
                        + f"videopath symlink: {str(_video_path)}\n\t\t"
                        + f'{" -> ".join([str(_l) for _l in _links_video_path])}')


def get_files_same_meta(folder1, folder2):
    files = defaultdict(lambda: [])
    for folder in (folder1, folder2):
        for file in Path(folder).rglob("*"):
            if (
                file.is_file()
                and not file.is_symlink()
                and not file.stem.startswith(".")
                and file.suffix.lower() in (".mp4", ".mkv", ".zip")
            ):
                files[_getxattr(str(file)) or "nometa"].append(str(file))

    return files


def move_gvd_files_same_meta(date):
    info = get_files_same_meta(
        f"/Users/antoniotorres/testing/SearchGVDBlogPlaylistdate={date}",
        f"/Users/antoniotorres/testing/SearchGVDBlogPlaylistdate={date}_alt=yes",
    )
    _share = f"/Users/antoniotorres/testing/SearchGVDBlogPlaylistdate={date}/share"
    os.mkdir(_share)

    for key, val in info.items():
        if len(val) > 1:
            print(key, "\n\t", val[0], "\n\t", val[1], "\n")
            shutil.move(val[0], _share)
            shutil.move(val[1], _share)


args = argparse.Namespace(
    w=8,
    winit=10,
    parts=16,
    format="bv*+ba/b",
    sort="ext:mp4:m4a",
    index=None,
    collection_files=[],
    checkcert=False,
    ytdlopts="",
    proxy=None,
    useragent=CONF_FIREFOX_UA,
    first=None,
    last=None,
    nodl=False,
    headers="",
    collection=[],
    dlcaching=False,
    path=None,
    caplinks=False,
    verbose=True,
    vv=False,
    quiet=False,
    aria2c=True,
    subt=True,
    nosymlinks=False,
    http_downloader="aria2c",
    use_path_pl=False,
    use_cookies=True,
    no_embed=False,
    rep_pause=False,
    rpcport=6800,
    enproxy=False,
    nocheckcert=True,
)


def get_ytdl(_args):
    return init_ytdl(_args)
