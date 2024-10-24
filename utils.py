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
from importlib.util import module_from_spec, spec_from_file_location
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

try:
    from supportlogging import LogContext, init_logging
except Exception as e:
    print(str(e), file=sys.stderr)
    init_logging = None
    LogContext = None

try:
    from asgiref.sync import sync_to_async
except Exception:
    sync_to_async = None

try:
    from selenium.webdriver import Firefox
except Exception:
    Firefox = None

try:
    from tabulate import tabulate
except Exception:
    tabulate = None


try:
    import psutil
except Exception as e:
    print(str(e), file=sys.stderr)
    psutil = None

try:
    import PySimpleGUI as sg
except Exception as e:
    print(str(e), file=sys.stderr)
    sg = None

try:
    import yt_dlp
except Exception as e:
    print(str(e), file=sys.stderr)
    yt_dlp = None

FileLock = None
try:
    from filelock import FileLock
except Exception:
    if yt_dlp:
        print("PLEASE INSTALL filelock")

try:
    import proxy
    import proxy_plugins
except Exception:
    if yt_dlp:
        print("PLEASE INSTALL proxy")
    proxy = None
    proxy_plugins = None

try:
    import xattr
except Exception:
    if yt_dlp:
        print("PLEASE INSTALL xattr")
    xattr = None

# ***********************************+
# ************************************

MAXLEN_TITLE = 150

PATH_LOGS = Path(Path.home(), "Projects/common/logs")

drm_base_path = Path(
    Path.home(),
    "Projects/dumper/key_dumps/Android Emulator 5554/private_keys/7283/2049378471",
)

CONF_DRM = {
    "private_key": Path(drm_base_path, "private_key.pem"),
    "client_id": Path(drm_base_path, "client_id.bin"),
}

CONF_DASH_SPEED_PER_WORKER = 102400

CONF_FIREFOX_PROFILE = "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/b33yk6rw.selenium"
CONF_FIREFOX_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0"
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

CONF_ARIA2C_EXTR_GROUP = ["doodstream", "odnoklassniki", "heretv"]
CONF_AUTO_PASRES = ["doodstream"]

CLIENT_CONFIG = {
    "timeout": httpx.Timeout(timeout=20),
    "limits": httpx.Limits(),
    "headers": {
        "User-Agent": CONF_FIREFOX_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Sec-Fetch-Mode": "navigate",
    },
    "follow_redirects": True,
    "verify": False,
}


logger = logging.getLogger('asyncdl')

class LoggerWriter:
    def __init__(self, logger, level):
        if isinstance(logger, str):
            self.logger = logging.getLogger(logger)
        else:
            self.logger = logger
        self.level = level if isinstance(level, int) else logging.getLevelName(level)


    def write(self, msg):
        if msg and msg not in (' ', '\n'):
            self.logger.log(self.level, msg)

    def flush(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        sys.stdout = self
        sys.stderr = self
        return self

    def __exit__(self, *args, **kwargs):
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__


class BufferingLoggerWriter(LoggerWriter):
    def __init__(self, logger, level):
        super().__init__(logger, level)
        self.buffer = ''

    def write(self, message):
        if '\n' not in message:
            self.buffer += message
        else:
            parts = message.split('\n')
            if self.buffer:
                msg = self.buffer + parts.pop(0)
                if msg and msg not in (' ', '\n'):
                    self.logger.log(self.level, msg)
            self.buffer = parts.pop()
            for msg in parts:
                if msg and msg not in (' ', '\n'):
                    self.logger.log(self.level, msg)


async def aenumerate(_aiter, start: int = 0):
    index = start
    async for item in _aiter:
        yield index, item
        index += 1

def deep_update(mapping, *updating_mappings):
    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if (
                k in updated_mapping
                and isinstance(updated_mapping[k], dict)
                and isinstance(v, dict)
            ):
                updated_mapping[k] = deep_update(updated_mapping[k], v)
            else:
                updated_mapping[k] = v
    return updated_mapping


def get_dependencies(your_package):
    import requests

    pypi_url = "https://pypi.python.org/pypi/" + your_package + "/json"
    data = requests.get(pypi_url).json()
    if _pkgs := data.get("info", {}).get("requires_dist"):
        return [
            (re.findall(r"^(\w+)", el)[0], el)
            for el in _pkgs
            if (
                "extra ==" not in el
                and 'sys_platform == "win32"' not in el
                and 'implementation_name != "cpython"' not in el
                and 'os_name == "nt"' not in el
                and 'implementation_name == "pypy"' not in el
                and "python_version < " not in el
                and 'platform_python_implementation == "PyPy"' not in el
            )
            or ("extra ==" in el and "extras" in el)
            or ("extra ==" in el and "'socks" in el)
        ]
    else:
        return []


def pip_show(_pckg):
    pckg = subprocess.run(
        ["pip", "show", _pckg], capture_output=True, encoding="utf-8"
    ).stdout
    if len(_temp := pckg.split("----")) > 1:
        pckg = _temp[0] + "\n".join(
            [_eltemp.replace("License:", "License_:") for _eltemp in _temp[1:]]
        )
    if _lic := (mytry_call(lambda: pckg.split("License: ")[1].split("\nLocation:")[0])):
        pckg = pckg.replace(
            _lic, _lic.replace("\n", "$$$___###rc###").replace(": ", "$$$___###dp###")
        )
    data = {el.split(": ")[0]: el.split(": ")[1] for el in pckg.splitlines()}
    data["Requires"] = data["Requires"].split(", ") if data["Requires"] else []
    data["Required-by"] = data["Required-by"].split(", ") if data["Required-by"] else []
    data["License"] = (
        data["License"]
        .replace("$$$___###dp###", ": ")
        .replace("License_:", "License:")
        .replace("$$$___###rc###", "\n")
        if data.get("License")
        else ""
    )
    return data


def pip_list():
    piplist = subprocess.run(
        "pip list | awk '{print $1}' | egrep -v 'Package|---'",
        shell=True,
        capture_output=True,
        encoding="utf-8",
    ).stdout.splitlines()

    with ThreadPoolExecutor() as exe:
        futures = [exe.submit(pip_show, pk) for pk in piplist]

    return {_res["Name"]: _res for fut in futures if (_res := fut.result())}


class MyRetryManager:
    def __init__(self, retries, limiter=None):
        self.limiter = limiter or contextlib.nullcontext()
        self.retries = retries
        self.error = None
        self.attempt = 0

    def __aiter__(self):
        # with self.limiter:
        #     return self
        return self

    def __iter__(self):
        # with self.limiter:
        #     return self
        return self

    async def __anext__(self):
        if not self.error and self.attempt < self.retries:
            self.attempt += 1
            async with self.limiter:
                return self
        else:
            raise StopAsyncIteration

    def __next__(self):
        if not self.error and self.attempt < self.retries:
            self.attempt += 1
            with self.limiter:
                return self
        else:
            raise StopIteration


class AsyncDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)

        self.exc_info = exc_info


class AsyncDLError(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


def mytry_call(*funcs, expected_type=None, default=None, args=[], kwargs={}):
    for f in funcs:
        try:
            val = f(*args, **kwargs)
        except (
            OSError,
            AttributeError,
            KeyError,
            TypeError,
            IndexError,
            ValueError,
            ZeroDivisionError,
        ):
            pass
        else:
            if expected_type is None or isinstance(val, expected_type):
                return val


def empty_queue(q: Union[asyncio.Queue, Queue]):
    while True:
        try:
            q.get_nowait()
        except (asyncio.QueueEmpty, Empty):
            break


def load_module(name: str, path: str):
    if path.endswith(".py"):
        spec = spec_from_file_location(name, path)
    else:
        _loader_details = [(SourceFileLoader, SOURCE_SUFFIXES)]
        finder = FileFinder(path, *_loader_details)
        spec = finder.find_spec(name)
    if not spec or not spec.loader:
        raise ImportError(f"no module named {name}")
    mod = module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def get_host(url: str, shorten=None) -> str:
    _host = re.sub(r"^www\.", "", urlparse(url).netloc)
    if shorten == "vgembed":
        _nhost = _host.split(".")
        if _host.count(".") >= 3:
            _host = ".".join(_nhost[-3:])
    return _host


def nested_obj(d, *selectors, get_all=True, default=None, v=False):
    logger = logging.getLogger("asyncdl")
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


def put_sequence(queue: Union[queue.Queue, asyncio.Queue], seq: Iterable)-> Union[queue.Queue, asyncio.Queue]:
    if seq:
        for el in seq:
            queue.put_nowait(el)
    return queue


def matchpatternnostringbefore(pattern, nostring, text):
    return re.search(rf"^(?:(?!{nostring}).)*{pattern}", text)


def subnright(pattern, repl, text, n):
    pattern = re.compile(rf"{pattern}(?!.*{pattern})", flags=re.DOTALL)
    _text = text
    for _ in range(n):
        _text = pattern.sub(repl, _text)
    return _text


def upt_dict(info_dict: Union[dict, list], **kwargs) -> Union[dict, list]:
    info_dict_list = [info_dict] if isinstance(info_dict, dict) else info_dict
    for _el in info_dict_list:
        _el.update(**kwargs)


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
            raise TypeError(
                "Cannot use cached_classproperty instance without calling __set_name__ on it."
            )
        try:
            cache = owner.__dict__
        except AttributeError:
            msg = (
                f"No '__dict__' attribute on {owner.__name__!r} "
                f"to cache {self.attrname!r} property."
            )
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
        self.root_dir = os.path.join(
            os.getenv("XDG_CACHE_HOME") or os.path.expanduser("~/.cache"), app
        )
        os.makedirs(self.root_dir, exist_ok=True)

    def _get_cache_fn(self, key):
        key = urllib.parse.quote(key, safe="").replace(
            "%", ","
        )  # encode non-ascii characters
        return os.path.join(self.root_dir, f"{key}.json")

    def store(self, key, obj):
        def write_json_file(obj, fn):
            with open(fn, mode="w", encoding="utf-8") as f:
                json.dump(
                    {"date": datetime.now().strftime("%Y.%m.%d"), "data": obj},
                    f,
                    ensure_ascii=False,
                )

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
                self.logger.warning(
                    f"Cache retrieval from {cache_fn} failed ({file_size})"
                )

        return default


class MySyncAsyncEvent:
    def __init__(self, name: Optional[str] = None, initset: bool = False):
        if name:
            self.name = name
        self._cause = None
        self.event = threading.Event()
        self.aevent = asyncio.Event()
        self._flag = False
        self._tasks = set()
        if initset:
            self.set()

    def set(self, cause: Optional[str] = None):
        self.aevent.set()
        self.event.set()
        self._flag = True
        self._cause = cause or "set_with_no_cause"

    def is_set(self) -> Optional[str]:
        if self._flag:
            return self._cause

    def clear(self):
        self.aevent.clear()
        self.event.clear()
        self._flag = False
        self._cause = None

    def wait(self, timeout: Optional[float] = None) -> dict:
        if self._flag:
            return {"event": self.name, "cause": self._cause}
        if self.event.wait(timeout=timeout):
            return {"event": self.name, "cause": self._cause}
        else:
            return {"timeout": timeout}

    async def async_wait(self, timeout: Optional[float] = None) -> dict:
        if self._flag:
            return {"event": self.name, "cause": self._cause}
        try:
            await asyncio.wait_for(self.aevent.wait(), timeout=timeout)
            return {"event": self.name, "cause": self._cause}
        except asyncio.TimeoutError:
            return {"timeout": timeout}

    def add_task(self, timeout: Optional[float] = None) -> asyncio.Task:
        _task = asyncio.create_task(self.async_wait(timeout=timeout), name=self.name)
        self._tasks.add(_task)
        _task.add_done_callback(self._tasks.discard)
        return _task

    def __repr__(self):
        status = f"set, cause: {self._cause}" if self._flag else "unset"
        _res = f"<{self.__class__.__module__}.{self.__class__.__qualname__} at {id(self):#x}: {status}"
        _res += f"\n\tname: {self.name if hasattr(self, 'name') else 'noname'}"
        _res += (
            f"\n\tsync event: {repr(self.event)}\n\tasync event: {repr(self.aevent)}\n>"
        )
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
        if seconds <= 0:
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
                time.sleep(CONF_INTERVAL_GUI / 2)

    async def async_wait_haselapsed(self, seconds: float):
        while True:
            if self.has_elapsed(seconds):
                return True
            else:
                await asyncio.sleep(CONF_INTERVAL_GUI / 2)


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
        self,
        initial_bytes: int = 0,
        upt_time: Union[int, float] = 1.0,
        ave_time: Union[int, float] = 5.0,
        smoothing: float = 0.3,
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
        self.KEEP_PROCESSING = True
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            signal.signal(s, self.exit_gracefully)

    def exit_gracefully(self, sig, frame):
        print(sig, frame)
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
                target=func,
                name=name,
                args=args,
                kwargs={"stop_event": stop_event, **kwargs},
                daemon=True,
            )
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

    def __init__(self, name: str, stop_event=None) -> None:
        self.name = name  # for thread prefix loggin and stop event name
        self.stop_event = stop_event or MySyncAsyncEvent(name)

    def __call__(self, func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> tuple[MySyncAsyncEvent, Future]:
            exe = ThreadPoolExecutor(thread_name_prefix=self.name)
            _kwargs = {"stop_event": self.stop_event}
            _kwargs.update(kwargs)
            fut = exe.submit(lambda: func(*args, **_kwargs))
            return (self.stop_event, fut)

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
                    func,
                    thread_sensitive=False,
                    executor=ThreadPoolExecutor(max_workers=1, thread_name_prefix=name),
                )(*args, **kwargs),
                name=name,
            )
            return (stop_event, _task)

        return wrapper


class async_suppress(contextlib.AbstractAsyncContextManager):
    def __init__(self, *exceptions, level=logging.DEBUG, logger=None, msg=None):
        self._exceptions = exceptions
        self.logger = logger or logging.getLogger("asyncdl")
        self.level = level if isinstance(level, int) else logging.getLevelName(level)
        self.msg = f"{msg} " if msg else ""

    async def __aenter__(self):
        pass

    async def __aexit__(self, exctype, excinst, exctb):
        if exctype is not None and issubclass(exctype, self._exceptions):
            self.logger.log(
                self.level, f"{self.msg}Exception supressed: {exctype}, {excinst}"
            )
            return True


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
    timeout: Optional[int] = None,
) -> dict:
    _events = cast(Iterable[MySyncAsyncEvent], variadic(events))

    if _res := [
        getattr(_ev, "name", "noname") for _ev in _events if _ev and _ev.is_set()
    ]:
        return {"event": _res}

    _tasks_events = {event.add_task(): f"{event.name}" for event in _events}

    done, pending = await asyncio.wait(
        _tasks_events, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
    )

    if pending:
        list(map(lambda x: x.cancel(), pending))
        await asyncio.wait(pending)
    if not done:
        return {"timeout": timeout}
    _done = done.pop()
    return _done.result()


async def async_wait_for_any(
    events, timeout: Optional[float] = None
) -> dict[str, list[str]]:
    _events = variadic(events)

    if _res := [
        getattr(_ev, "name", "noname") for _ev in _events if _ev and _ev.is_set()
    ]:
        return {"event": _res}

    def check_timeout(_st, _n):
        return False if _n is None else time.monotonic() - _st >= _n

    start = time.monotonic()
    try:
        while True:
            if _res := [
                cast(str, getattr(_ev, "name", "noname"))
                for _ev in _events
                if _ev and _ev.is_set()
            ]:
                return {"event": _res}
            elif check_timeout(start, timeout):
                return {"timeout": [str(timeout)]}
            await asyncio.sleep(CONF_INTERVAL_GUI / 2)
    except Exception as e:
        logger = logging.getLogger("asyncdl")
        logger.error(repr(e))


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
        listfs = variadic(fs)

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
            bktasks=_background_tasks,
            name="fs_list",
        )

        _final_wait[_one_task_to_wait_tasks] = "tasks"

    if events:
        _events = variadic(events)

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
            _task_sleep = add_task(
                asyncio.sleep(timeout * 2), bktasks=_background_tasks
            )
            _tasks[_task_sleep] = "task"
            _final_wait.update(_tasks)
        else:
            return {"timeout": "nothing to await"}

    _pending = []
    _done = []
    _done_before_condition = []
    _condition = {"timeout": False, "event": None}
    _results = []
    _errors = []
    _cancelled = []

    done, pending = await asyncio.wait(
        list(_final_wait.keys()), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
    )

    # aux task created and has to be cancelled
    to_cancel = []

    try:
        if not done:
            _condition["timeout"] = True
            if _tasks:
                _done_before_condition.extend(
                    list(filter(lambda x: x._state == "FINISHED", list(_tasks.keys())))
                )
                _pending.extend(
                    list(filter(lambda x: x._state == "PENDING", list(_tasks.keys())))
                )
        else:
            _task_done = done.pop()
            if "fs_list" in _task_done.get_name():
                if _tasks_events:
                    to_cancel.extend(list(_tasks_events.keys()))
                _done.extend(list(_tasks.keys()))

            else:
                _condition["event"] = _task_done.get_name()
                if _one_task_to_wait_tasks:
                    to_cancel.append(_one_task_to_wait_tasks)
                to_cancel.extend(list(_tasks_events.keys()))
                if _tasks:
                    _done_before_condition.extend(
                        list(
                            filter(
                                lambda x: x._state == "FINISHED", list(_tasks.keys())
                            )
                        )
                    )
                    _pending.extend(
                        list(
                            filter(lambda x: x._state == "PENDING", list(_tasks.keys()))
                        )
                    )

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
                _errors.extend([_e for _d in _get_from if (_e := _d.exception())])

        return {
            "condition": _condition,
            "results": _results,
            "errors": _errors,
            "done": _done,
            "done_before_condition": _done_before_condition,
            "pending": _pending,
            "cancelled": _cancelled,
        }

    except Exception as e:
        logger = logging.getLogger("asyncdl")
        logger.exception(repr(e))


@contextlib.asynccontextmanager
async def async_lock(
    lock: Union[LockType, threading.Lock, contextlib.nullcontext, None] = None,
):
    if not lock or (isinstance(lock, contextlib.nullcontext)):
        yield

    else:
        await asyncio.to_thread(lock.acquire)
        yield  # the lock is held
        await asyncio.to_thread(lock.release)


async def async_wait_time(n: Union[int, float]):
    return await async_waitfortasks(timeout=n)


def wait_time(
    n: Union[int, float], event: Optional[threading.Event | MySyncAsyncEvent] = None
):
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
        if (time.monotonic() - _started) >= timeout:
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
        if (time.monotonic() - _started) >= timeout:
            raise TimeoutError()
        else:
            time.sleep(interv)


############################################################
# """                     YTDLP                           """
############################################################


if yt_dlp:
    from pyrate_limiter import LimitContextDecorator
    from yt_dlp import YoutubeDL, parse_options
    from yt_dlp.cookies import YoutubeDLCookieJar, extract_cookies_from_browser
    from yt_dlp.extractor.common import InfoExtractor
    from yt_dlp.networking import HEADRequest
    from yt_dlp.utils import (
        ExtractorError,
        determine_protocol,
        find_available_port,
        get_domain,
        js_to_json,
        prepend_extension,
        render_table,
        sanitize_filename,
        smuggle_url,
        traverse_obj,
        try_get,
        unsmuggle_url,
        update_url,
        variadic,
        write_string,
    )

    try:
        from yt_dlp_plugins.extractor.commonwebdriver import (
            By,
            ConnectError,
            GroupProgressBar,
            ReExtractInfo,
            SeleniumInfoExtractor,
            StatusError503,
            StatusStop,
            dec_on_exception,
            dec_retry_error,
            ec,
            getter_basic_config_extr,
            limiter_0_01,
            limiter_0_1,
            limiter_1,
            limiter_5,
            limiter_15,
            limiter_non,
            load_config_extractors,
            my_dec_on_exception,
        )
    except Exception:
        pass
    assert LimitContextDecorator
    assert find_available_port
    assert unsmuggle_url
    assert update_url
    assert smuggle_url
    assert prepend_extension
    assert get_domain
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
        _debug_phr = [
            "Falling back on generic information extractor",
            "Extracting URL",
            "Extracting cookies from:",
            "Media identified",
            "The information of all playlist entries will be held in memory",
            "Looking for video embeds",
            "Identified a HTML5 media",
            "Identified a KWS Player",
            "Identified a KVS Player",
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
            "You have asked for UNPLAYABLE formats to be listed/downloaded",
            "in player engine - download may fail",
            "cookies from firefox",
            "Ignoring subtitle tracks found in the HLS manifest",
            "Using netrc for",
            "You have requested merging of multiple formats",
        ]

        _skip_phr = ["Downloading", "Extracting information", "Checking", "Logging"]

        def __init__(self, logger, quiet=False, verbose=False, superverbose=False):
            super().__init__(logger, {})
            self.quiet = quiet
            self.verbose = verbose
            self.superverbose = superverbose

        def error(self, msg, *args, **kwargs):
            self.log(logging.DEBUG, msg, *args, **kwargs)

        def info(self, msg, *args, **kwargs):
            if self.quiet:
                return
            if any(_ in msg for _ in self._debug_phr):
                self.log(logging.DEBUG, msg, *args, **kwargs)
            else:
                self.log(logging.INFO, msg, *args, **kwargs)

        def warning(self, msg, *args, **kwargs):
            if any(_ in msg for _ in self._debug_phr):
                self.log(logging.DEBUG, msg, *args, **kwargs)
            else:
                self.log(logging.WARNING, msg, *args, **kwargs)

        def debug(self, msg, *args, **kwargs):
            if self.quiet:
                # self.log(logging.DEBUG, msg, *args, **kwargs)
                return
            mobj = get_values_regex([r"^(\[[^\]]+\])"], msg) or ""
            mobj2 = msg.split(": ")[-1]
            if self.verbose and not self.superverbose:
                if any(
                    [
                        (mobj in ("[redirect]", "[download]", "[debug+]", "[info]")),
                        (
                            mobj in ("[debug]")
                            and any(_ in msg for _ in self._debug_phr)
                        ),
                        "Extracting URL:" in msg,
                        any(_ in mobj2 for _ in self._skip_phr),
                    ]
                ):
                    self.log(logging.DEBUG, msg[len(mobj) :].strip(), *args, **kwargs)
                else:
                    self.log(logging.INFO, msg, *args, **kwargs)
            elif self.superverbose:
                self.log(logging.INFO, msg, *args, **kwargs)
            elif mobj in (
                "[redirect]",
                "[debug]",
                "[info]",
                "[download]",
                "[debug+]",
            ) or any(_ in mobj2 for _ in self._skip_phr):
                self.log(logging.DEBUG, msg[len(mobj) :].strip(), *args, **kwargs)
            else:
                self.log(logging.INFO, msg, *args, **kwargs)

    def cli_to_api(*opts):
        default = yt_dlp.parse_options([]).ydl_opts
        diff = {
            k: v for k, v in parse_options(opts).ydl_opts.items() if default[k] != v
        }
        if "postprocessors" in diff:
            diff["postprocessors"] = [
                pp
                for pp in diff["postprocessors"]
                if pp not in default["postprocessors"]
            ]
        return diff

    class myYTDL(YoutubeDL):
        def __init__(
            self,
            params: Optional[dict] = None,
            auto_init: Union[bool, str] = True,
            **kwargs,
        ):
            self._close = kwargs.get("close") or True
            if not (executor := kwargs.get("executor")):
                executor = ThreadPoolExecutor(
                    thread_name_prefix=self.__class__.__name__.lower()
                )
            self.sync_to_async = functools.partial(
                sync_to_async, thread_sensitive=False, executor=executor
            )
            opts = {}
            if _proxy := kwargs.get("proxy"):
                opts["proxy"] = _proxy
            if kwargs.get("silent"):
                opts["quiet"] = True
                opts["verbose"] = False
                opts["verboseplus"] = False
                opts["logger"] = MyYTLogger(
                    logging.getLogger("yt_dlp"),
                    quiet=True,
                    verbose=False,
                    superverbose=False,
                )

            super().__init__(params=(params or {}) | opts, auto_init=auto_init)  # type: ignore

        def __exit__(self, *args):
            super().__exit__(*args)
            if self._close:
                self.close()

        async def __aenter__(self):
            return await self.sync_to_async(super().__enter__)()

        async def __aexit__(self, *args):
            return await self.sync_to_async(self.__exit__)(*args)

        def get_extractor(self, el: str) -> InfoExtractor:
            if el.startswith("http"):
                _, _sel_ie = self._get_info_extractor_from_url(el)
            else:
                _sel_ie = self.get_info_extractor(el)
            _sel_ie.initialize()
            return _sel_ie

        def _get_info_extractor_from_url(self, url: str) -> InfoExtractor:
            _sel_ie_key = "Generic"
            for ie_key, ie in self._ies.items():
                try:
                    if ie.suitable(url) and (ie_key != "Generic"):
                        _sel_ie_key = ie_key
                        break
                except Exception as e:
                    logger = logging.getLogger("asyncdl")
                    logger.exception(f"[get_extractor] fail with {ie_key} - {repr(e)}")
            _sel_ie = self.get_info_extractor(_sel_ie_key)
            return _sel_ie_key, _sel_ie

        def is_playlist(self, url: str) -> tuple:
            ie_key, ie = cast(tuple, self._get_info_extractor_from_url(url))
            if ie_key == "Generic":
                return (True, ie_key)
            else:
                return (ie._RETURN_TYPE == "playlist", ie_key)

        async def stop(self):
            if _stop := self.params.get("stop"):
                _stop.set()
                await asyncio.sleep(0)


        def _get_filesize(self, info) -> dict:
            try:
                if _res := self.urlopen(
                    HEADRequest(info["url"], headers=info.get("http_headers", {}))
                ):
                    _filesize_str = _res.get_header("Content-Length")
                    _accept_ranges = any(
                        [
                            _res.get_header("Accept-Ranges"),
                            _res.get_header("Content-Range"),
                        ]
                    )
                    return {
                        "filesize": int_or_none(_filesize_str),
                        "accept_ranges": _accept_ranges,
                    }
            except Exception as e:
                logger = logging.getLogger("asyncdl")
                logger.exception(f"[myytdl_getfilesize] fail {repr(e)}\n{info}")
                return {}

        def sanitize_info(self, info: dict, **kwargs) -> dict:
            if (
                info["extractor"] == "generic"
                and not info.get("filesize")
                and get_protocol(info) in ("http", "https")
            ):
                info |= self._get_filesize(info)

            return cast(dict, super().sanitize_info(info, **kwargs))

        async def async_extract_info(self, *args, **kwargs) -> dict:
            return await self.sync_to_async(self.extract_info)(*args, **kwargs)

        async def async_process_ie_result(self, *args, **kwargs) -> dict:
            return await self.sync_to_async(self.process_ie_result)(*args, **kwargs)

    def get_cookies_netscape_file(cookies_str: str, cookies_file_path: str) -> bool:
        with myYTDL() as ytdl:
            ytdl._load_cookies(cookies_str, autoscope=False)
            ytdl.cookiejar.save(filename=cookies_file_path)
        return os.path.exists(cookies_file_path)

    def get_cookies_jar(cookies_str: str) -> YoutubeDLCookieJar | None:
        with myYTDL() as ytdl:
            ytdl._load_cookies(cookies_str, autoscope=False)
            return ytdl.cookiejar

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

        headers = {
            "User-Agent": args.useragent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        if args.headers:
            headers |= {
                el[0]: el[1]
                for item in args.headers
                if item and (el := item.split(":", 1))
            }

        ytdl_opts = {
            "cachedir": "/Users/antoniotorres/.config/yt-dlp",
            "allow_unplayable_formats": True,
            "retries": 0,
            "extractor_retries": 0,
            "force_generic_extractor": False,
            "allowed_extractors": ["default"],
            "http_headers": headers,
            "proxy": args.proxy,
            "logger": MyYTLogger(
                logging.getLogger("yt_dlp"),
                quiet=args.quiet,
                verbose=args.verbose,
                superverbose=args.vv,
            ),
            "verbose": args.verbose,
            "quiet": args.quiet,
            "format": args.format,
            "format_sort": [args.sort],
            "nocheckcertificate": True,
            "subtitlesformat": "srt/vtt",
            "subtitleslangs": ["es", "en"],
            "keepvideo": True,
            "convertsubtitles": "srt",
            "embedsubtitles": True,
            "continuedl": True,
            "updatetime": False,
            "ignore_no_formats_error": True,
            "ignoreerrors": 'only_download',
            "extract_flat": "in_playlist",
            "color": {"stderr": "no_color", "stdout": "no_color"},
            "usenetrc": True,
            "skip_download": True,
            "writesubtitles": True,
            "writeautomaticsub": True,
            "postprocessors": [
                {
                    "key": "FFmpegSubtitlesConvertor",
                    "format": "srt",
                    "when": "before_dl",
                },
                {"key": "FFmpegEmbedSubtitle", "already_have_subtitle": True},
                {
                    "key": "FFmpegMetadata",
                    "add_chapters": None,
                    "add_metadata": True,
                    "add_infojson": None,
                },
                {"key": "XAttrMetadata"},
                {"key": "FFmpegConcat", "only_multi_video": True, "when": "playlist"},
            ],
            "external_downloader": {"default": "native"},
            "skip_unavailable_fragments": False,
            "concurrent_fragment_downloads": 128,
            "restrictfilenames": True,
            "user_agent": args.useragent,
            "verboseplus": args.vv,
            "sem": {},
            "stop": threading.Event(),
            "lock": threading.Lock(),
            "embed": not args.no_embed,
            "_util_classes": {"SimpleCountDown": SimpleCountDown, "myYTDL": myYTDL},
            "outtmpl": {"default": args.outtmpl},
        }

        if args.use_cookies:
            ytdl_opts["cookiesfrombrowser"] = ("firefox", CONF_FIREFOX_PROFILE, None)

        if args.ytdlopts:
            ytdl_opts |= json.loads(js_to_json(args.ytdlopts))

        ytdl = myYTDL(params=ytdl_opts, auto_init="no_verbose_header")

        logging.getLogger("asyncdl").debug(f"ytdl opts:\n{ytdl.params}")

        return ytdl

    def get_protocol(info):
        protocol = determine_protocol(info)
        if "dash" in protocol or "dash" in info.get("container", ""):
            return "dash"
        if req_fmts := info.get("requested_formats"):
            for fmt in req_fmts:
                if "dash" in fmt.get("container", ""):
                    return "dash"
        return protocol

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

            return (
                "{"
                + ",\n".join([f"'{key}': {value}" for key, value in _videos.items()])
                + "}"
            )

        elif isinstance(_videos, list):
            _videos = [str(_for_print(_vid)) for _vid in _videos]
            return "[" + ",\n".join(_videos) + "]"

    def render_res_table(data, headers=(), maxcolwidths=None, showindex=True, tablefmt="simple"):
        if tabulate:
            return tabulate(
                data,
                headers=headers,
                maxcolwidths=maxcolwidths,
                showindex=showindex,
                tablefmt=tablefmt,
            )
        logger = logging.getLogger("asyncdl")
        logger.warning(
            "Tabulate is not installed, tables will not be presented optimized"
        )
        return render_table(headers, data, delim=True)

    def raise_extractor_error(msg, expected=True, _from=None):
        raise ExtractorError(msg, expected=expected) from _from

    def raise_reextract_info(msg, expected=True, _from=None):
        raise ReExtractInfo(msg, expected=expected) from _from

    async def _async_send_http_request(url: str, **kwargs) -> Optional[httpx.Response]:
        _type = kwargs.pop("_type", "GET")
        fatal = kwargs.pop("fatal", True)
        _logger = kwargs.pop("logger", print)
        _client_cl = False
        if not (client := kwargs.pop("client", None)):
            client = httpx.AsyncClient(**CLIENT_CONFIG)
            _client_cl = True
        res = None
        req = None
        _msg_err = ""

        try:
            req = client.build_request(_type, url, **kwargs)
            if not (res := await client.send(req)):
                return None
            if fatal:
                res.raise_for_status()
            return res
        except httpx.HTTPStatusError as e:
            e.args = (e.args[0].split(" for url")[0],)
            _msg_err = f"{repr(e)}"
            if e.response.status_code == 403:
                raise_reextract_info(_msg_err)
            elif e.response.status_code in (502, 503, 520, 521):
                raise StatusError503(_msg_err) from None
            else:
                raise
        except Exception as e:
            _msg_err = f"{repr(e)}"
            raise
        finally:
            _logger(f"[async_send_http_req] {_msg_err} {req}:{req.headers}:{res}")
            if _client_cl:
                client.close()

    def _send_http_request(url: str, **kwargs) -> Optional[httpx.Response]:
        _type = kwargs.pop("_type", "GET")
        fatal = kwargs.pop("fatal", True)
        _logger = kwargs.pop("logger", print)
        _client_cl = False
        if not (client := kwargs.pop("client", None)):
            client = httpx.Client(**CLIENT_CONFIG)
            _client_cl = True
        res = None
        req = None
        _msg_err = ""

        try:
            req = client.build_request(_type, url, **kwargs)
            if not (res := client.send(req)):
                return None
            if fatal:
                res.raise_for_status()
            return res
        except httpx.HTTPStatusError as e:
            e.args = (e.args[0].split(" for url")[0],)
            _msg_err = f"{repr(e)}"
            if e.response.status_code == 403:
                raise_reextract_info(_msg_err)
            elif e.response.status_code in (502, 503, 520, 521):
                raise StatusError503(_msg_err) from None
            else:
                raise
        except Exception as e:
            _msg_err = f"{repr(e)}"
            raise
        finally:
            _logger(f"[send_http_req] {_msg_err} {req}:{req.headers}:{res}")
            if _client_cl:
                client.close()


    def send_http_request(url, **kwargs) -> Optional[httpx.Response | dict]:
        """
        raises ReExtractInfo, StatusError503, Exception
        """
        new_e = kwargs.pop("new_e", None)
        try:
            return _send_http_request(url, **kwargs)
        except (StatusError503, ReExtractInfo):
            raise
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            return {"error": repr(e)}
        except Exception as e:
            if not new_e:
                raise
            else:
                raise new_e(repr(e)) from e

    async def async_send_http_request(url, **kwargs) -> Optional[httpx.Response | dict]:
        """
        raises ReExtractInfo, StatusError503, Exception
        """
        new_e = kwargs.pop("new_e", None)
        try:
            return await _async_send_http_request(url, **kwargs)
        except (StatusError503, ReExtractInfo):
            raise
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            return {"error": repr(e)}
        except Exception as e:
            if not new_e:
                raise
            else:
                raise new_e(repr(e)) from e

    def get_pssh_from_manifest(
        manifest_url: Optional[str] = None, manifest_doc: Optional[str] = None, **kwargs
    ) -> list:
        if not manifest_doc and manifest_url:
            if not (manifest_doc := get_manifest(manifest_url, **kwargs)):
                raise Exception("error with manifest doc")
        if "<?xml" in manifest_doc:
            import xmltodict

            return get_pssh_from_mpd(mpd_dict=xmltodict.parse(manifest_doc))
        elif "#EXTM3U" in manifest_doc:
            return get_pssh_from_m3u8(m3u8_doc=manifest_doc)

    def get_pssh_from_m3u8(
        m3u8_url: Optional[str] = None, m3u8_doc: Optional[str] = None, **kwargs
    ) -> list:
        uuid = "urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
        pssh = set()
        try:
            if not m3u8_doc:
                with httpx.Client(**CLIENT_CONFIG) as client:
                    m3u8_doc = try_get(
                        client.get(m3u8_url, **kwargs),
                        lambda x: x.content.decode("utf-8", "replace"),
                    )
        except Exception:
            return list(pssh)

        import m3u8

        m3u8obj = m3u8.loads(m3u8_doc, uri=m3u8_url)
        if keys := m3u8obj.session_keys:
            for key in keys:
                if key.method == "SAMPLE-AES-CTR":
                    if getattr(key, "keyformat", "").lower() == uuid:
                        if uri := getattr(key, "uri", None):
                            if _pssh := mytry_call(
                                lambda: uri.split("data:text/plain;base64,")[1]
                            ):
                                pssh.add(_pssh)
        return list(pssh)

    def get_pssh_from_mpd(
        mpd_url: Optional[str] = None, mpd_dict: Optional[dict] = None, **kwargs
    ) -> list:
        uuid = "urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
        pssh = set()
        if not mpd_dict:
            mpd_dict = get_xml(mpd_url, **kwargs)

        if mpd_dict:
            mpd = json.loads(json.dumps(mpd_dict))
            periods = mpd["MPD"]["Period"]

            for period in variadic(periods):
                for ad_set in variadic(period["AdaptationSet"]):
                    if any(
                        [
                            ad_set.get("@contentType") == "video",
                            ad_set.get("@mimeType") == "video/mp4",
                        ]
                    ):
                        for t in ad_set["ContentProtection"]:
                            if t["@schemeIdUri"].lower() == uuid:
                                pssh.add(t["cenc:pssh"])
            return list(pssh)

    def get_manifest(manifest_url: str, **kwargs) -> dict:
        _upt_config = {}
        if _cookies_arg := kwargs.pop("cookies", None):
            if isinstance(_cookies_arg, str):
                _cookies_type = get_cookies_jar(_cookies_arg)
            else:
                _cookies_type = _cookies_arg.copy()
            _upt_config["cookies"] = _cookies_type

        with httpx.Client(**(CLIENT_CONFIG | _upt_config)) as client:
            return try_get(
                client.get(manifest_url, **kwargs),
                lambda x: x.content.decode("utf-8", "replace"),
            )

    def get_xml(mpd_url: str, **kwargs) -> dict:
        import xmltodict

        if not (_doc := kwargs.pop("doc", None)):
            _doc = get_manifest(mpd_url, **kwargs)
        if _doc:
            return xmltodict.parse(_doc)

    def get_drm_keys(
        lic_url: str,
        pssh: Optional[str] = None,
        func_validate: Optional[Callable] = None,
        manifest_url: Optional[str] = None,
        manifest_doc: Optional[str] = None,
        **kwargs,
    ) -> list | str:
        from mydrm import myDRM

        return myDRM.get_drm_keys(
            lic_url,
            pssh=pssh,
            func_validate=func_validate,
            manifest_url=manifest_url,
            **kwargs,
        )

    def get_drm_xml(
        lic_url: str,
        file_dest: str | Path,
        pssh: Optional[str] = None,
        func_validate: Optional[Callable] = None,
        manifest_url: Optional[str] = None,
        manifest_doc: Optional[str] = None,
        **kwargs,
    ):
        from mydrm import myDRM

        return myDRM.get_drm_xml(
            lic_url,
            file_dest,
            pssh=pssh,
            func_validate=func_validate,
            manifest_url=manifest_url,
            manifest_doc=manifest_doc,
            **kwargs,
        )

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
            "t7": Path("/Volumes/T7/videos"),
        }

        logger = logging.getLogger("asyncdl")

        list_folders = []

        for _vol, _folder in config_folders.items():
            if not _folder.exists():
                logger.error(
                    f"failed {_vol}:{_folder}, let get previous info saved in previous files"
                )

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
                                file.parent,
                                res[vid_name]["file_name_def"] + file.suffix,
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
                if entleg["format_id"].startswith("hls") or not entalt[
                    "format_id"
                ].startswith("hls"):
                    logger.info(f"cause 1 {entleg['original_url']}")
                    urls_leg_dl.append(entleg["original_url"])
                    urls_final.append(entleg["original_url"])
                    entries_final.append(entleg)
                elif not entleg["format_id"].startswith("hls") and entalt[
                    "format_id"
                ].startswith("hls"):
                    entaltfilesize = entalt.get("filesize_approx") or (
                        entalt.get("tbr", 0) * entalt.get("duration", 0) * 1000 / 8
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

############################################################
# """                     INIT                     """
############################################################


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
            opts,
            dest,
            nargs="?",
            const=None,
            default=default,
            required=required,
            help=help,
        )

    def __call__(self, parser, namespace, values, option_strings=None):
        if option_strings:
            if option_strings.startswith("--no-"):
                setattr(namespace, self.dest, False)
            else:
                _val = values or True
                setattr(namespace, self.dest, _val)


def init_argparser():
    parser = argparse.ArgumentParser(
        description="Async downloader videos / playlist videos HLS / HTTP"
    )
    parser.add_argument("-w", help="Number of DL workers", default="5", type=int)
    parser.add_argument(
        "--winit",
        help="Number of init workers, default is same number for DL workers",
        default="10",
        type=int,
    )
    parser.add_argument(
        "-p", "--parts", help="Number of workers for each DL", default="16", type=int
    )
    parser.add_argument(
        "--format",
        help="Format preferred of the video in youtube-dl format",
        default="bv*+ba/b",
        type=str,
    )
    parser.add_argument(
        "--outtmpl",
        help="Format preferred for filrname",
        default="%(id)s_%(title)s.%(ext)s",
        type=str,
    )
    parser.add_argument(
        "--sort", help="Formats sort preferred", default="ext:mp4:m4a", type=str
    )
    parser.add_argument(
        "--index", help="index of a video in a playlist", default=None, type=int
    )
    parser.add_argument(
        "--file", help="jsonfiles", action="append", dest="collection_files", default=[]
    )
    parser.add_argument(
        "--checkcert", help="checkcertificate", action="store_true", default=False
    )
    parser.add_argument("--ytdlopts", help="init dict de conf", default="", type=str)
    parser.add_argument("--proxy", action=ActionNoYes, default=None)
    parser.add_argument("--useragent", default=CONF_FIREFOX_UA, type=str)
    parser.add_argument("--first", default=None, type=int)
    parser.add_argument("--last", default=None, type=int)
    parser.add_argument(
        "--nodl", help="not download", action="store_true", default=False
    )
    parser.add_argument("--add-header", action="append", dest="headers", default=[])
    parser.add_argument("-u", action="append", dest="collection", default=[])
    parser.add_argument(
        "--dlcaching",
        help="whether to force to check external storage or not",
        action=ActionNoYes,
        default=False,
    )
    parser.add_argument("--path", default=None, type=str)
    parser.add_argument("--caplinks", action="store_true", default=False)
    parser.add_argument(
        "-v", "--verbose", help="verbose", action="store_true", default=False
    )
    parser.add_argument("--vv", help="verbose plus", action=ActionNoYes, default=False)
    parser.add_argument(
        "-q", "--quiet", help="quiet", action="store_true", default=False
    )
    parser.add_argument(
        "--aria2c",
        action=ActionNoYes,
        default="6800",
        help="use of external aria2c running in port [PORT]. By default PORT=6800. Set to 'no' to disable",
    )
    parser.add_argument("--subt", action=ActionNoYes, default=True)
    parser.add_argument("--xattr", action=ActionNoYes, default=True)
    parser.add_argument("--drm", action=ActionNoYes, default=True)
    parser.add_argument("--nosymlinks", action="store_true", default=False)
    parser.add_argument("--check-speed", action=ActionNoYes, default=True)
    parser.add_argument(
        "--deep-aldl",
        help="whether to enable greedy mode when checking if aldl by only taking into account 'ID'. Otherwise, will check 'ID_TITLE'",
        action=ActionNoYes,
        default=False,
    )
    parser.add_argument("--downloader-ytdl", action="store_true", default=False)
    parser.add_argument("--use-path-pl", action="store_true", default=False)
    parser.add_argument("--use-cookies", action="store_true", default=True)
    parser.add_argument("--no-embed", action="store_true", default=False)
    parser.add_argument("--rep-pause", action="store_true", default=False)
    parser.add_argument("--keep-videos", action="store_true", default=False)

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

    def jsonKeys2int(x):
        trans = lambda x: int(x) if x.isdigit() else x  # noqa: E731
        if isinstance(x, dict):
            return {trans(k): v for k, v in x.items()}

    if printout := subprocess.run(
        ["sudo", "_listening", "-o", "json"], encoding="utf-8", capture_output=True
    ).stdout:
        return json.loads(printout, object_hook=jsonKeys2int)
    else:
        raise ValueError("no info about tcp ports in use")


def init_aria2c(args):
    logger = logging.getLogger("asyncdl")
    _info = get_listening_tcp()
    _in_use_aria2c_ports = sorted(traverse_obj(_info, ("aria2c", ..., "port")))
    if args.rpcport in _in_use_aria2c_ports:
        _port = _in_use_aria2c_ports[-1]
        for n in range(10):
            args.rpcport = _port + (n + 1) * 100
            if args.rpcport not in _info:
                break

    _cmd = f"aria2c --rpc-listen-port {args.rpcport} --enable-rpc "
    _cmd += "--rpc-max-request-size=2M --rpc-listen-all --quiet=true"
    _proc = subprocess.Popen(
        shlex.split(_cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )

    while _proc.poll() is None:
        if args.rpcport in traverse_obj(
            get_listening_tcp(), ("aria2c", ..., "port"), default=[]
        ):
            break
        time.sleep(1)

    if _proc.poll() is not None:
        raise ValueError(
            f"[init_aria2c] couldnt run aria2c in port {args.rpcport} - {_proc}"
        )

    logger.info(f"[init_aria2c] running on port: {args.rpcport}")

    return _proc


def get_httpx_client(config: Optional[dict] = None) -> httpx.Client:
    if not config:
        config = {}
    _config = CLIENT_CONFIG | config
    #logger.info(f'[get_client] {_config}')
    return httpx.Client(**_config)


def get_httpx_async_client(config: Optional[dict] = None) -> httpx.AsyncClient:
    if not config:
        config = {}
    return httpx.AsyncClient(**(CLIENT_CONFIG | config))


def get_driver(noheadless=True, **kwargs):
    if _driver := try_get(
        SeleniumInfoExtractor._get_driver(**kwargs | {"noheadless": noheadless}),
        lambda x: x[0] if x else None,
    ):
        return _driver


############################################################
# """                     IP/TORGUARD                    """
############################################################


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

    @classmethod
    def _set_config(cls, key, timeout=1):
        _proxies = {"all://": f"http://127.0.0.1:{key}"} if key else None
        _timeout = httpx.Timeout(timeout=timeout)
        return get_httpx_client(config={"proxies": _proxies, "timeout": _timeout})

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
    def get_ip(cls, client, api="ipify"):
        if api not in cls.URLS_API_GETMYIP:
            raise ValueError("[get_ip] api not supported")

        _urlapi = cls.URLS_API_GETMYIP[api]["url"]
        _keyapi = cls.URLS_API_GETMYIP[api]["key"]
        with limiter_0_01.ratelimit(api, delay=True):
            return try_get(client.get(_urlapi), lambda x: x.json().get(_keyapi))

    @classmethod
    def get_myiptryall(cls, client):
        exe = ThreadPoolExecutor(thread_name_prefix="getmyip")
        try:
            futures = [
                exe.submit(cls.get_ip, client, api=api) for api in cls.URLS_API_GETMYIP
            ]
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
        client = cls._set_config(key, timeout=timeout)
        try:
            if tryall:
                return cls.get_myiptryall(client)
            if not api:
                api = random.choice(list(cls.URLS_API_GETMYIP))
                return cls.get_ip(client, api=api)
        finally:
            if client:
                client.close()


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
    CONF_TORPROXIES_DOMAINS = [
        f"{cc}.secureconnect.me" for cc in CONF_TORPROXIES_COUNTRIES
    ]
    CONF_TORPROXIES_NOK = Path(PATH_LOGS, "bad_proxies.txt")

    EVENT = MySyncAsyncEvent("dummy")

    IPS_SSL = []

    logger = logging.getLogger("asyncdl")

    @classmethod
    def mytest_proxies_rt(cls, routing_table, timeout=2):
        cls.logger.info("[init_proxies] starting test proxies")
        bad_pr = []
        exe = ThreadPoolExecutor(thread_name_prefix="testproxrt")
        try:
            futures = {
                exe.submit(getmyip, key=_key, timeout=timeout): _key
                for _key in list(routing_table.keys())
            }

            for fut in as_completed(list(futures.keys())):
                if cls.EVENT.is_set():
                    break
                if not fut.exception() and is_ipaddr(_ip := fut.result()):
                    if _ip != routing_table[futures[fut]]:
                        cls.logger.debug(
                            f"[{futures[fut]}] test: {_ip} expect res: {routing_table[futures[fut]]}"
                        )
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
                _proc = subprocess.Popen(
                    shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                _proc.poll()
                if _proc.returncode:
                    cls.logger.error(f"[initprox] rc[{_proc.returncode}] to cmd[{cmd}]")
                    raise ValueError("init proxies error")
                else:
                    proc_gost.append(_proc)
            except ValueError:
                sanitize_killproc(proc_gost)
                proc_gost = []
                break
        return proc_gost

    @classmethod
    def mytest_proxies_raw(cls, list_ips, port=CONF_TORPROXIES_HTTPPORT, timeout=2):
        cmd_gost = [
            f"gost -L=:{CONF_PROXIES_BASE_PORT + 2000 + i} "
            + f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip}:{port}"
            for i, ip in enumerate(list_ips)
        ]
        routing_table = {
            CONF_PROXIES_BASE_PORT + 2000 + i: ip for i, ip in enumerate(list_ips)
        }

        if not (proc_gost := cls._init_gost(cmd_gost)):
            cls.logger.error("[init_proxies] error with gost commands")
            return False
        try:
            _res_bad = None
            if not cls.EVENT.is_set():
                _res_ps = subprocess.run(
                    ["ps"], encoding="utf-8", capture_output=True
                ).stdout
                cls.logger.debug(f"[init_proxies] %no%\n\n{_res_ps}")

                _res_bad = cls.mytest_proxies_rt(routing_table, timeout=timeout)
                _line_ps_pr = []
                for _ip in _res_bad:
                    if _temp := try_get(
                        re.search(rf".+{_ip}\:\d+", _res_ps),
                        lambda x: x.group() if x else None,
                    ):
                        _line_ps_pr.append(_temp)
                cls.logger.info(
                    f"[init_proxies] check in ps print equal number of bad ips: res_bad [{len(_res_bad)}] "
                    + f"ps_print [{len(_line_ps_pr)}]"
                )

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
        timeout=8,
        event=None,
    ) -> Tuple[List, Dict]:
        cls.logger.info("[init_proxies] start")

        try:
            if event:
                cls.EVENT = event

            cls.IPS_SSL = []

            if cls.EVENT.is_set():
                return [], {}

            for domain in cls.CONF_TORPROXIES_DOMAINS:
                cls.IPS_SSL += cls.get_ips(domain)

            _bad_ips = None
            cached_res = cls.CONF_TORPROXIES_NOK
            if cached_res.exists() and (
                (
                    datetime.now() - datetime.fromtimestamp(cached_res.stat().st_mtime)
                ).seconds
                < 7200
            ):  # every 2h we check the proxies
                with open(cached_res, "r") as f:
                    if _content := f.read():
                        _bad_ips = [_ip for _ip in _content.split("\n") if _ip]
            else:
                if cls.EVENT.is_set():
                    return [], {}
                if _bad_ips := cls.mytest_proxies_raw(
                    cls.IPS_SSL, port=port, timeout=timeout
                ):
                    if isinstance(_bad_ips, list):
                        for _ip in _bad_ips:
                            if _ip in cls.IPS_SSL:
                                cls.IPS_SSL.remove(_ip)
                else:
                    cls.logger.error("[init_proxies] test failed")
                    return [], {}

            _ip_main = random.choice(cls.IPS_SSL)

            cls.IPS_SSL.remove(_ip_main)

            if len(cls.IPS_SSL) < num * (size + 1):
                cls.logger.warning("[init_proxies] not enough IPs to generate sample")
                return [], {}

            _ips = random.sample(cls.IPS_SSL, num * (size + 1))

            def grouper(iterable, n, *, incomplete="fill", fillvalue=None):
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
                f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*i + j} "
                + f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[j]}:{port}"
                for j in range(size + 1)
                for i, ip in enumerate(FINAL_IPS)
            ]
            routing_table = {
                (CONF_PROXIES_BASE_PORT + 100 * i + j): ip[j]
                for j in range(size + 1)
                for i, ip in enumerate(FINAL_IPS)
            }

            cmd_gost_main = [
                f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*num + 99} "
                + f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{_ip_main}:{port}"
            ]
            routing_table[CONF_PROXIES_BASE_PORT + 100 * num + 99] = _ip_main

            cmd_gost = cmd_gost_s + cmd_gost_main

            if cls.EVENT.is_set():
                return [], {}

            cls.logger.debug(f"[init_proxies] {cmd_gost}")
            cls.logger.debug(f"[init_proxies] {routing_table}")

            if not (proc_gost := cls._init_gost(cmd_gost)):
                cls.logger.debug("[init_proxies] error with gost commands")
                return [], {}

            return proc_gost, routing_table

        finally:
            cls.logger.info("[init_proxies] done")

    def genwgconf(self, host, **kwargs):
        headers = {
            "User-Agent": CONF_FIREFOX_UA,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.5",
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
            TorGuardProxies.logger.error(
                f"[gen_wg_conf] {host} is not a torguard domain: xx.torguard.com"
            )

        cl = get_httpx_client()

        if ckies := kwargs.get("cookies"):
            reqckies = ckies.get("Request Cookies", ckies)
            for name, value in reqckies.items():
                cl.cookies.set(name, value, "torguard.net")
        else:
            for cookie in extract_cookies_from_browser("firefox"):
                if "torguard.net" in cookie.domain:
                    cl.cookies.set(
                        name=cookie.name, value=cookie.value, domain=cookie.domain
                    )  # type: ignore
        if info := kwargs.get("info"):
            data |= info
        else:
            headersform = {
                "User-Agent": CONF_FIREFOX_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
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
                    "https://torguard.net/tgconf.php?action=vpn-openvpnconfig",
                    headers=headersform,
                )
            token, tokk = re.findall(
                r'"(?:token|tokk)" value="([^"]+)"', resform.text
            ) or ["", ""]
            if token and tokk:
                data["token"] = token
                data["tokk"] = tokk
        urlpost = "https://torguard.net/generateconfig.php"
        with limiter_0_1.ratelimit("torguardconf", delay=True):
            respost = try_get(
                cl.post(urlpost, headers=headers, data=data), lambda x: x.json()
            )
        if (
            respost
            and respost.get("success") == "true"
            and (_config := respost.get("config"))
        ):
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
                        re.search(r"Endpoint = (?P<ip>[^\:]+)\:", _config),
                        lambda x: x.groupdict().get("ip"),
                    )
                    or ""
                )
                _file = (
                    data["oserver[]"].split(".")[0].upper()
                    + _ip.replace(".", "_")
                    + ".conf"
                )
            else:
                _file = (
                    (kwargs.get("pre", "") or "")
                    + data["server"].replace(".", "_")
                    + ".conf"
                )
            with open(f"/Users/antoniotorres/testing/{_file}", "w") as f:
                f.write(_config)
        else:
            TorGuardProxies.logger.error(
                f"[gen_wg_conf] {respost.get('error') if respost else 'error with host[{host}]: check cookies and tokens'}"
            )


def create_progress_bar(
    pbid: str,
    total: int | float,
    block_logging: bool = True,
    msg: str | None = None,
):
    group_pb = GroupProgressBar(sys.stdout, 5)
    return group_pb.add_pb(pbid, total, block_logging, msg)


def get_all_wd_conf(name=None):
    if name and "torguard.com" in name:
        ips = TorGuardProxies.get_ips(name)
        if ips:
            total = len(ips)
            _pre = name.split(".")[0].upper()
            proxies = TorGuardProxies()

            with create_progress_bar("torguard", total, msg="got conf") as pb:

                def getconf(_ip):
                    with contextlib.suppress(Exception):
                        proxies.genwgconf(_ip, pre=_pre)
                    pb.update_print("")

                with ThreadPoolExecutor(
                    thread_name_prefix="tgconf", max_workers=5
                ) as exe:
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
            logger = logging.getLogger("asyncdl")
            logger.exception(repr(e))
    else:
        print("Use ip or torguard domain xx.torguard.com")


############################################################
# """                     various                           """
############################################################


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
            translated_sentence = self.GoogleTranslate(
                sentence, src=self.src, dst=self.dst, timeout=self.timeout
            )
            fail_to_translate = translated_sentence[-1] == "\n"  # type: ignore
            while fail_to_translate and patience:
                translated_sentence = self.GoogleTranslate(
                    translated_sentence,
                    src=self.src,
                    dst=self.dst,
                    timeout=self.timeout,
                )
                if translated_sentence[-1] == "\n":  # type: ignore
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
        url = "https://translate.googleapis.com/translate_a/"
        params = f"single?client=gtx&sl={src}&tl={dst}&dt=t&q={text}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://translate.google.com",
        }

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


def translate_srt(filesrt, srclang, dstlang, strip=True):
    import srt

    with open(filesrt, "r") as f:
        _srt_text = f.read()
    _list_srt = list(srt.parse(_srt_text))

    lock = threading.Lock()
    trans = SentenceTranslator(src=srclang, dst=dstlang, patience=0)

    def worker(subt, i):
        if _temp := subt.content:
            start = _temp.startswith("# ")
            end = _temp.endswith(" #")
            if start:
                _temp = _temp.replace("# ", "")
            if end:
                _temp = _temp.replace(" #", "")
            if strip:
                _temp = [_temp.replace("\n", " ")]
            else:
                _temp = _temp.splitlines()

            _res = "\n".join([trans(_subt) for _subt in _temp])
            if not _res:
                with lock:
                    print(f"ERROR: {i} - {_temp}")
            if start:
                _res = f"# {_res}"
            if end:
                _res += " #"
            return _res
        else:
            with lock:
                print(f"ERROR: {i} - {subt.content}")

    with ThreadPoolExecutor(max_workers=16) as exe:
        futures = [exe.submit(worker, subt, i) for i, subt in enumerate(_list_srt)]

    for fut, sub in zip(futures, _list_srt):
        sub.content = fut.result()

    return srt.compose(_list_srt)


def print_delta_seconds(seconds):
    return ":".join(
        [
            _item.split(".")[0]
            for _item in f"{timedelta(seconds=seconds)}".split(":")[1:]
        ]
    )


def print_tasks(tasks):
    return "\n".join(
        [f"{task.get_name()} : {repr(task.get_coro()).split(' ')[2]}" for task in tasks]
    )


def print_threads(threads):
    return "\n".join(
        [f"{thread.getName()} : {repr(thread._target)}" for thread in threads]
    )


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
                subprocess.subprocess.run(
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
    def _int_or_none(el):
        try:
            return int(el) if el else None
        except (ValueError, TypeError, OverflowError):
            return None

    if isinstance(res, (list, tuple)):
        _res = [_int_or_none(_el) for _el in res]
        return _res if isinstance(res, list) else tuple(_res)
    else:
        return _int_or_none(res)


def str_or_none(res):
    return str(res) if res else None


def naturalsize(value, binary=False, format_="6.2f"):
    SUFFIXES = {
        "decimal": ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"),
        "binary": ("KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"),
    }
    if value is None:
        return "0"

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


def init_config(logging=True, test=False, log_name="asyncdl"):
    patch_http_connection_pool(maxsize=1000)
    patch_https_connection_pool(maxsize=1000)
    os.environ["MOZ_HEADLESS_WIDTH"] = "1920"
    os.environ["MOZ_HEADLESS_HEIGHT"] = "1080"
    if logging:
        if not init_logging or not LogContext:
            print('init config logging incomplete')
        logger = init_logging(log_name, test=test) if init_logging else logging.getLogger(log_name)
        logctx = LogContext() if LogContext else contextlib.nullcontext()
        return logger, logctx


class CountDowns:
    INPUT_TIMEOUT = 2
    DEFAULT_TIMEOUT = 30
    INTERV_TIME = 0.25
    N_PER_SECOND = 1 if INTERV_TIME >= 1 else int(1 / INTERV_TIME)
    PRINT_DIF_IN_SECS = 20
    _INPUT = Queue()

    def __init__(self, klass=None, events=None, logger=None):
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
            CountDowns.N_PER_SECOND = (
                1 if CountDowns.INTERV_TIME >= 1 else int(1 / CountDowns.INTERV_TIME)
            )
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

                    _input = CountDowns._INPUT.get(
                        block=True, timeout=CountDowns.INPUT_TIMEOUT
                    )
                    if _input == "":
                        _input = self.index_main
                    elif _input in self.countdowns:
                        self.logger.debug(f"{self._pre} input[{_input}] is index video")
                        self.countdowns[_input]["stop"].set()
                    else:
                        self.logger.debug(
                            f"{self._pre} input[{_input}] not index video"
                        )

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
                if self.klass:
                    self.klass._QUEUE[index].put_nowait(_msg)

        _events = self.outer_events + [self.countdowns[index]["stop"]]
        if event:
            _events += list(variadic(event))

        _res = None
        for i in range(self.N_PER_SECOND * n, 0, -1):
            send_queue(i)
            if _res := [getattr(ev, "name", "noname") for ev in _events if ev.is_set()]:
                break
            time.sleep(self.INTERV_TIME)

        if self.klass:
            self.klass._QUEUE[index].put_nowait("")

        if not _res:
            _res = ["TIMEOUT_COUNT"]

        self.logger.debug(f"{self.countdowns[index]['premsg']} return Count: {_res}")
        return _res

    def add(
        self, n: Optional[int] = None, index: Optional[str] = None, event=None, msg=None
    ):
        _premsg = f"{self._pre}"
        if msg:
            _premsg += msg

        if index in self.countdowns:
            self.logger.error(f"{_premsg} error: already in countdown")
            return ["ERROR"]
        if index is None:
            self.logger.error(f"{_premsg} error: index is None")
            return ["ERROR"]

        if n is not None and isinstance(n, int) and n > 3:
            timeout = n - 3
        else:
            timeout = self.DEFAULT_TIMEOUT - 3

        time.sleep(3)

        if event and event.is_set():
            self.logger.debug(f"{_premsg} finish event is set")
            return

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
    _LOCK = threading.Lock()
    _ENABLE_ECHO = True

    def __init__(
        self,
        pb,
        inputqueue=None,
        check: Optional[Callable] = None,
        logger=None,
        indexdl=None,
        timeout=60,
    ):
        self._pre = "[countdown][WAIT403]"
        self.check = check or (lambda: None)
        self.pb = pb

        self.timeout = timeout
        self.indexdl = indexdl

        self.inputq = inputqueue
        self.logger = logger or logging.getLogger("simplecd")

    def enable_echo(self, enable):
        with SimpleCountDown._LOCK:
            if enable and not SimpleCountDown._ENABLE_ECHO:
                fd = sys.stdin.fileno()
                new = termios.tcgetattr(fd)
                new[3] |= termios.ECHO
                SimpleCountDown._ENABLE_ECHO = True
                termios.tcsetattr(fd, termios.TCSANOW, new)

            elif not enable and SimpleCountDown._ENABLE_ECHO:
                fd = sys.stdin.fileno()
                new = termios.tcgetattr(fd)
                new[3] &= ~termios.ECHO
                SimpleCountDown._ENABLE_ECHO = False
                termios.tcsetattr(fd, termios.TCSANOW, new)

    def _wait_for_enter(
        self, sel: selectors.DefaultSelector, interval: Optional[int] = None
    ):
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
                    self.pb.update_print("Waiting")

                except Exception as e:
                    self.logger.debug(f"[{self.indexdl}] event is set {repr(e)}")
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


if sg:

    class FrontEndGUI:
        _PASRES_REPEAT = False
        _PASRES_EXIT = MySyncAsyncEvent("pasresexit")
        _LOCK = threading.Lock()
        _MAP = {
            "manip": ("init_manipulating", "manipulating"),
            "finish": ("error", "done", "stop"),
            "init": "init",
            "downloading": "downloading",
        }
        _SECS_DL = 25
        _SECS_PAUSE = 5

        def __init__(self, asyncdl):
            self.asyncdl = asyncdl
            self.logger = logging.getLogger("FEgui")
            self.list_finish = {}
            self.console_dl_status = False
            if self.asyncdl.args.rep_pause:
                FrontEndGUI._PASRES_REPEAT = True

            self.pasres_time_from_resume_to_pause = FrontEndGUI._SECS_DL
            self.pasres_time_in_pause = FrontEndGUI._SECS_PAUSE
            self.reset_repeat = False
            self.list_all_old = {
                "init": {},
                "downloading": {},
                "manip": {},
                "finish": {},
            }

            self.list_upt = {}
            self.list_res = {}
            self.stop_upt_window, self.fut_upt_window = try_get(
                self.upt_window_periodic(),
                lambda x: (x[0], self.asyncdl.add_task(x[1])))
            #self.asyncdl.add_task(self.fut_upt_window)
            self.exit_upt = MySyncAsyncEvent("exitupt")
            self.stop_pasres, self.fut_pasres = try_get(
                self.pasres_periodic(),
                lambda x: (x[0], self.asyncdl.add_task(x[1])))
            #self.asyncdl.add_task(self.fut_pasres)
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
                            [
                                el[1]
                                for el in sorted(
                                    list(list_init.values()), key=lambda x: x[0]
                                )
                            ]
                        )
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
                            + "\n\n-------END STATUS DL------------\n\n"
                        )
                        self.console_dl_status = False
                if "manip" in values["all"]:
                    if list_manip := values["all"]["manip"]:
                        upt = "\n\n" + "".join(list((list_manip.values())))
                    else:
                        upt = ""
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
                elif event in ["CtrlC"]:
                    print(event, values)
                    await self.asyncdl.cancel_all_dl()
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
                            + f"[time in pause] {self.pasres_time_in_pause}"
                        )

                    self.window_console["-IN-"].update(value="")

            async def _handle_numvideoworkers(values):
                if not values["-IN-"]:
                    sg.cprint("Please enter number")
                else:
                    if not (_nvidworkers := int_or_none(values["-IN-"].split(",")[0])):
                        sg.cprint("#vidworkers not an integer")
                    else:
                        if _nvidworkers <= 0:
                            sg.cprint("#vidworkers must be > 0")
                        elif self.asyncdl.list_dl:
                            _copy_list_dl = self.asyncdl.list_dl.copy()
                            if "," in values["-IN-"]:
                                _ind = int_or_none(values["-IN-"].split(",")[1:])
                                if None in _ind or any(
                                    _el not in _copy_list_dl for _el in _ind
                                ):
                                    sg.cprint("At least one DL index doesnt exist")
                                else:
                                    for _el in _ind:
                                        await _copy_list_dl[_el].change_numvidworkers(
                                            _nvidworkers
                                        )
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
                            _index_list = [
                                int(el)
                                for el in values["-IN-"].replace(" ", "").split(",")
                            ]

                        self.window_console["-IN-"].update(value="")

                    if _index_list:
                        if event in ["+PasRes", "-PasRes"]:
                            sg.cprint(
                                f"[pause-resume autom] before: {list(self.asyncdl.list_pasres)}"
                            )

                        info = []
                        _wait_tasks = []
                        for _index in _index_list:
                            if event == "MoveTopWaitingDL":
                                if not self.asyncdl.WorkersInit.exit.is_set():
                                    sg.cprint(
                                        "[move to top waiting list] cant process until every video "
                                        + "has been checked by init"
                                    )
                                else:
                                    await self.asyncdl.WorkersRun.move_to_waiting_top(
                                        _index
                                    )
                            if event == "StopCount":
                                CountDowns._INPUT.put_nowait(str(_index))
                            elif event == "+PasRes":
                                if self.asyncdl.list_dl[_index].info_dl["status"] in (
                                    "init",
                                    "downloading",
                                ):
                                    self.asyncdl.list_pasres.add(_index)
                            elif event == "-PasRes":
                                self.asyncdl.list_pasres.discard(_index)
                            elif event == "Pause":
                                await self.asyncdl.list_dl[_index].pause()
                            elif event == "Resume":
                                await self.asyncdl.list_dl[_index].resume()
                            elif event == "Reset":
                                if _tasks := await self.asyncdl.list_dl[
                                    _index
                                ].reset_from_console():
                                    _wait_tasks.extend(_tasks)
                            elif event == "Stop":
                                await self.asyncdl.list_dl[_index].stop("exit")
                            elif event in ["Info", "ToFile"]:
                                _info = json.dumps(
                                    self.asyncdl.list_dl[_index].info_dict
                                )
                                sg.cprint(f"[{_index}] info\n{_info}")
                                sg.cprint(
                                    f'[{_index}] status[{self.asyncdl.list_dl[_index].info_dl["status"]} - {self.asyncdl.list_dl[_index].info_dl["downloaders"][0].status}]'
                                    + f'filesize[{self.asyncdl.list_dl[_index].info_dl["downloaders"][0].filesize}]'
                                    + f'downsize[{self.asyncdl.list_dl[_index].info_dl["downloaders"][0].down_size}]'
                                    + f"pause[{self.asyncdl.list_dl[_index].pause_event.is_set()}]"
                                    + f"resume[{self.asyncdl.list_dl[_index].resume_event.is_set()}]"
                                    + f"stop[{self.asyncdl.list_dl[_index].stop_event.is_set()}]"
                                    + f"reset[{self.asyncdl.list_dl[_index].reset_event.is_set()}]"
                                )

                                info.append(_info)
                        # if _wait_tasks:
                        #     await asyncio.wait(_wait_tasks)
                        if event in ["+PasRes", "-PasRes"]:
                            sg.cprint(
                                f"[pause-resume autom] after: {list(self.asyncdl.list_pasres)}"
                            )

                        if event == "ToFile":
                            _launch_time = self.asyncdl.launch_time.strftime(
                                "%Y%m%d_%H%M"
                            )
                            _file = Path(Path.home(), "testing", f"{_launch_time}.json")
                            _data = {"entries": info}
                            with open(_file, "w") as f:
                                f.write(json.dumps(_data))
                            sg.cprint(f"saved to file: {_file}")

            sg.cprint(event, values)
            if event == sg.WIN_CLOSED:
                return "break"
            elif event in ["CtrlC"]:
                print("console", event, values)
                await self.asyncdl.cancel_all_dl()
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
                "ToFile",
                "Info",
                "Pause",
                "Resume",
                "Reset",
                "Stop",
                "+PasRes",
                "-PasRes",
                "StopCount",
                "MoveTopWaitingDL",
            ]:
                await _handle_event(values)

        async def gui(self):
            try:
                self.window_console = self.init_gui_console()
                self.window_root = self.init_gui_root()
                await asyncio.sleep(0)
                while True:
                    try:
                        window, event, values = sg.read_all_windows(timeout=50)
                        if event and event != sg.TIMEOUT_KEY and window:
                            _res = []
                            if window == self.window_console:
                                _res.append(await self.gui_console(event, values))
                            elif window == self.window_root:
                                _res.append(await self.gui_root(event, values))
                            if "break" in _res:
                                break
                    except asyncio.CancelledError as e:
                        self.logger.debug(f"[gui] inner exception {repr(e)}")
                        raise
                    except Exception as e:
                        self.logger.exception(f"[gui] inner exception {repr(e)}")
                    finally:
                        await asyncio.sleep(0)

            except Exception as e:
                self.logger.warning(f"[gui] outer exception {repr(e)}")
            finally:
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
                resizable=True,
            )
            # window.set_min_size(window.size)
            if ml_keys:
                for key in ml_keys:
                    window[key].expand(True, True, True)
            if to_front:
                window.bring_to_front()
            window.bind("<Control-KeyPress-c>", "CtrlC")

            return window

        def init_gui_root(self):
            sg.theme("SystemDefaultForReal")

            col_0 = sg.Column(
                [
                    [
                        sg.Text(
                            "WAITING TO DL", font="Any 14", expand_x=True, expand_y=True
                        )
                    ],
                    [
                        sg.Multiline(
                            default_text="\nWaiting for info",
                            size=(30, 40),
                            font=("Courier New Bold", 10),
                            write_only=True,
                            key="-ML0-",
                            autoscroll=True,
                            auto_refresh=True,
                            expand_x=True,
                            expand_y=True,
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
                            size=(200, 2),
                            font=("Courier New Bold", 12),
                            key="ST",
                            expand_x=True,
                            expand_y=True,
                        )
                    ]
                ],
                expand_x=True,
                expand_y=True,
            )

            col_1 = sg.Column(
                [
                    [
                        sg.Text(
                            "NOW DOWNLOADING",
                            font="Any 14",
                            expand_x=True,
                            expand_y=True,
                        )
                    ],
                    [
                        sg.Multiline(
                            default_text="\nWaiting for info",
                            size=(90, 40),
                            font=("Courier New Bold", 10),
                            write_only=True,
                            key="-ML1-",
                            autoscroll=True,
                            auto_refresh=True,
                            expand_x=True,
                            expand_y=True,
                        )
                    ],
                ],
                element_justification="l",
                expand_x=True,
                expand_y=True,
            )

            col_3 = sg.Column(
                [
                    [
                        sg.Text(
                            "NOW CREATING FILE        ",
                            font="Any 14",
                            expand_x=True,
                            expand_y=True,
                        )
                    ],
                    [
                        sg.Multiline(
                            default_text="\nWaiting for info",
                            size=(30, 40),
                            font=("Courier New Bold", 10),
                            write_only=True,
                            key="-ML3-",
                            autoscroll=True,
                            auto_refresh=True,
                            expand_x=True,
                            expand_y=True,
                        )
                    ],
                ],
                element_justification="l",
                expand_x=True,
                expand_y=True,
            )

            col_2 = sg.Column(
                [
                    [
                        sg.Text(
                            "DOWNLOADED/STOPPED/ERRORS",
                            font="Any 14",
                            expand_x=True,
                            expand_y=True,
                        )
                    ],
                    [
                        sg.Multiline(
                            default_text="\nWaiting for info",
                            size=(30, 40),
                            font=("Courier New Bold", 10),
                            write_only=True,
                            key="-ML2-",
                            autoscroll=True,
                            auto_refresh=True,
                            expand_x=True,
                            expand_y=True,
                        )
                    ],
                ],
                element_justification="l",
                expand_x=True,
                expand_y=True,
            )

            layout_root = [[col_00], [col_0, col_1, col_3, col_2]]

            return self.get_window(
                "async_downloader", layout_root, ml_keys=[f"-ML{i}-" for i in range(4)]
            )

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
                            autoscroll=True,
                        )
                    ],
                    [
                        sg.Checkbox(
                            "PauseRep",
                            key="-PASRES-",
                            default=FrontEndGUI._PASRES_REPEAT,
                            enable_events=True,
                        ),
                        sg.Checkbox(
                            "ResRep",
                            key="-RESETREP-",
                            default=False,
                            enable_events=True,
                        ),
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
                expand_y=True,
            )

            layout_pygui = [[col_pygui]]

            return self.get_window(
                "Console",
                layout_pygui,
                location=(0, 500),
                ml_keys=["-ML-"],
                to_front=True,
            )

        def _upt_status(self, st, _copy_list_dl, _waiting, _running):
            self.list_upt[st] = {}
            self.list_res[st] = {}
            if st == "init":
                _list_items = _waiting
                for i, index in enumerate(_list_items):
                    if self.asyncdl.list_dl[index].info_dl["status"] in self._MAP[st]:
                        self.list_res[st].update(
                            {index: (i, self.asyncdl.list_dl[index].print_hookup())}
                        )
            else:
                _list_items = _copy_list_dl if st != "downloading" else _running
                for index in _list_items:
                    if self.asyncdl.list_dl[index].info_dl["status"] in self._MAP[st]:
                        self.list_res[st].update(
                            {index: self.asyncdl.list_dl[index].print_hookup()}
                        )

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

                self.list_all_old |= self.list_upt.copy()

        @run_operation_in_executor_from_loop(name="uptwinthr")
        def upt_window_periodic(self, *args, **kwargs):
            self.logger.debug("[upt_window_periodic] start")
            stop_upt = kwargs["stop_event"]
            try:
                progress_timer = ProgressTimer()
                short_progress_timer = ProgressTimer()
                self.list_nwmon = []
                io_init = psutil.net_io_counters()
                speedometer = SpeedometerMA(
                    initial_bytes=io_init.bytes_recv, ave_time=0.5, smoothing=0.9
                )
                ds = None
                while not stop_upt.is_set():
                    if progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI):
                        io_upt = psutil.net_io_counters()
                        ds = speedometer(io_upt.bytes_recv)
                        msg = f"RECV: {naturalsize(speedometer.rec_bytes)}  "
                        msg += f'DL: {naturalsize(ds, format_="7.3f") + "ps" if ds > 1024 else "--"}'
                        self.update_window("all", nwmon=msg)
                        if short_progress_timer.has_elapsed(
                            seconds=10 * CONF_INTERVAL_GUI
                        ):
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
                _media = naturalsize(median(_speed_data))

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
                            timeout=self.pasres_time_from_resume_to_pause,
                        )
                        FrontEndGUI._PASRES_EXIT.clear()
                        if (
                            not FrontEndGUI._PASRES_REPEAT
                            or not self.asyncdl.list_pasres
                        ):
                            continue
                        if _waitres_nopause == "TIMEOUT" and (
                            _list := list(self.asyncdl.list_pasres)
                        ):
                            if not self.reset_repeat:
                                if _start_no_pause:
                                    sg.cprint(
                                        f"[time resume -> pause] {time.monotonic()-_start_no_pause}"
                                    )

                                self.window_console.write_event_value(
                                    "Pause", ",".join(list(map(str, _list)))
                                )
                                time.sleep(1)
                                self.logger.debug("[pasres_periodic]: pauses sent")
                                _start_pause = time.monotonic()
                                _waitres = wait_for_either(
                                    [stop_event, FrontEndGUI._PASRES_EXIT],
                                    timeout=self.pasres_time_in_pause,
                                )
                                FrontEndGUI._PASRES_EXIT.clear()
                                self.logger.debug(
                                    "[pasres_periodic]: start sending resumes"
                                )
                                if _waitres == "TIMEOUT":
                                    _time = self.pasres_time_in_pause / len(_list)
                                    for i, _el in enumerate(_list):
                                        self.window_console.write_event_value(
                                            "Resume", str(_el)
                                        )

                                        if i + 1 < len(_list):
                                            _waitres = wait_for_either(
                                                [stop_event, FrontEndGUI._PASRES_EXIT],
                                                timeout=random.uniform(
                                                    0.75 * _time, 1.25 * _time
                                                ),
                                            )
                                            FrontEndGUI._PASRES_EXIT.clear()
                                            if _waitres != "TIMEOUT":
                                                self.window_console.write_event_value(
                                                    "Resume",
                                                    ",".join(
                                                        list(map(str, _list[i + 1 :]))
                                                    ),
                                                )

                                                break

                                else:
                                    self.window_console.write_event_value(
                                        "Resume", ",".join(list(map(str, _list)))
                                    )

                                self.logger.debug(
                                    "[pasres_periodic]: resumes sent, start timer to next pause"
                                )

                                sg.cprint(
                                    f"[time in pause] {time.monotonic()-_start_pause}"
                                )
                                _start_no_pause = time.monotonic()

                            else:
                                self.window_console.write_event_value(
                                    "Reset", ",".join(list(map(str, _list)))
                                )
                    else:
                        _start_no_pause = None
                        time.sleep(CONF_INTERVAL_GUI)

            except Exception as e:
                self.logger.exception(f"[pasres_periodic]: error: {repr(e)}")
            finally:
                self.exit_pasres.set()
                self.logger.debug("[pasres_periodic] BYE")

        async def close(self):
            self.logger.debug("[close] start")
            try:
                self.stop_pasres.set()
                self.stop_upt_window.set()
                self.fut_upt_window.cancel()
                self.fut_pasres.cancel()
                await asyncio.sleep(0)
                self.logger.debug("[close] start to wait for uptwindows and pasres")

                await asyncio.wait([self.fut_upt_window, self.fut_pasres])

                self.logger.debug("[close] end to wait for uptwindows and pasres")

                self.task_gui.cancel()
                await asyncio.sleep(0)
                self.logger.debug("[close] start to wait for task gui")
                await asyncio.wait([self.task_gui])
                self.logger.debug("[close] end to wait for task gui")
                if hasattr(self, "window_console") and self.window_console:
                    self.window_console.close()
                    del self.window_console
                if hasattr(self, "window_root") and self.window_root:
                    self.window_root.close()
                    del self.window_root
            except Exception as e:
                self.logger.error(f"[close] error - {repr(e)}")
            finally:
                self.logger.debug("[close] bye")


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
        self.sync_to_async = functools.partial(
            sync_to_async,
            thread_sensitive=False,
            executor=ThreadPoolExecutor(thread_name_prefix="setupnw"),
        )

        self._tasks_init = {}
        if not self.asyncdl.args.nodl:
            if self.asyncdl.args.aria2c:
                _task_aria2c = self.asyncdl.add_task(
                    self.sync_to_async(init_aria2c)(self.asyncdl.args)
                )
                self._tasks_init |= {_task_aria2c: "aria2"}
            if self.asyncdl.args.enproxy:
                self.stop_proxy, self.fut_proxy = self.run_proxy_http()
                self.asyncdl.add_task(self.fut_proxy)
                _task_proxies = self.asyncdl.add_task(
                    self.sync_to_async(TorGuardProxies.init_proxies)(
                        event=self.asyncdl.end_dl
                    )
                )
                self._tasks_init |= {_task_proxies: "proxies"}

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
                self.asyncdl.STOP.set()
        self.init_ready.set()
        self.logger.debug(f"[init] init done, init_ready set {self.init_ready}")

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
                    "proxy_plugins.ProxyPoolByHostPlugin",
                ]
            ) as p:
                try:
                    self.logger.debug(p.flags)
                    stop_event.wait()
                except Exception:
                    self.logger.error("context manager proxy")
                    raise
        except Exception as e:
            self.logger.exception(f"[run_proxy] error - {repr(e)}")
            self.asyncdl.STOP.set()
        finally:
            self.shutdown_proxy.set()

    async def close(self):
        if not self.init_ready.is_set():
            await self.init_ready.async_wait()
        if self.asyncdl.args.enproxy:
            self.logger.debug("[close] proxy")
            self.stop_proxy.set()
            await asyncio.sleep(0)
            await asyncio.wait([self.fut_proxy])
            self.logger.debug("[close] OK shutdown")

            if self.proc_gost:
                self.logger.debug("[close] gost")
                await self.sync_to_async(sanitize_killproc)(self.proc_gost)

        if self.proc_aria2c:
            self.logger.debug("[close] start close aria2c")
            self.proc_aria2c.terminate()
            try:
                if self.proc_aria2c.stdout:
                    self.proc_aria2c.stdout.close()
                if self.proc_aria2c.stderr:
                    self.proc_aria2c.stderr.close()
                if self.proc_aria2c.stdin:
                    self.proc_aria2c.stdin.close()
            except Exception as e:
                self.logger.error(f"[close] aria2c {repr(e)}")
            finally:
                try:
                    await asyncio.sleep(1.01)
                    if self.proc_aria2c.poll() is None:
                        self.logger.debug("[close] wait aria2c")
                        await self.sync_to_async(self.proc_aria2c.wait)()
                except Exception as e:
                    self.logger.error(f"[close] aria2c {repr(e)}")
                finally:
                    self.logger.debug("[close] close aria2c done")

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
            await self.sync_to_async(self.proc_aria2c.wait)()

        _task_aria2c = [
            self.asyncdl.add_task(self.sync_to_async(init_aria2c)(self.asyncdl.args))
        ]
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
    info_dict: dict

    def clear(self):
        self.pause_event.clear()
        self.resume_event.clear()
        self.stop_event.clear()
        self.end_tasks.clear()
        self.reset_event.clear()


def run_proc(cmd):
    try:
        _cmd = shlex.split(cmd)
        return subprocess.run(
            _cmd,
            encoding="utf-8",
            capture_output=True,
            timeout=120,
        )
    except Exception:
        pass


def get_metadata_video(path):
    cmd = f"ffprobe -hide_banner -show_streams -show_format -print_format json {str(path)}"
    if proc := run_proc(cmd):
        return json.loads(proc.stdout)


def get_metadata_video_subt(language, info):
    if isinstance(info, str):
        info = get_metadata_video(info)
    return list(
        filter(
            lambda x: x["codec_type"] == "subtitle"
            and traverse_obj(x, ("tags", "language")) == language,
            info["streams"],
        )
    )


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
            "t7": Path("/Volumes/T7/videos"),
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
                "t7": {},
            }

            _upt_temp["last_time_sync"] = last_time_sync

            for key, val in videos_cached.items():
                _vol = getter(val)
                if not _vol:
                    self.logger.error(
                        f"found file with not registered volumen - {val} - {key}"
                    )
                else:
                    _upt_temp[_vol].update({key: val})

            shutil.copy(
                str(LocalStorage.local_storage), str(LocalStorage.prev_local_storage)
            )

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
        return xattr.getxattr(x, attr="user.dublincore.description").decode()

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
            self.ready_videos_cached, self.fut_videos_cached = self.get_videos_cached(
                local=True
            )
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
                                    self._videoscached.update(
                                        self._localstorage._data_from_file[_vol]
                                    )
                            else:
                                list_folders_to_scan[_folder] = _vol

                    else:
                        for _vol, _folder in self._localstorage.config_folders.items():
                            if not _folder.exists():  # comm failure
                                self.logger.error(
                                    f"Fail connect to [{_vol}], will use last info"
                                )
                                self._videoscached.update(
                                    self._localstorage._data_from_file[_vol]
                                )
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
                            ]

                            for file in files:
                                if not force_local and not file.is_symlink():
                                    with contextlib.suppress(Exception):
                                        _xattr_desc = _getxattr(file)
                                        if _xattr_desc:
                                            if not self._videoscached.get(_xattr_desc):
                                                self._videoscached.update(
                                                    {_xattr_desc: str(file)}
                                                )
                                            else:
                                                self._repeated_by_xattr.append(
                                                    {
                                                        _xattr_desc: [
                                                            self._videoscached[
                                                                _xattr_desc
                                                            ],
                                                            str(file),
                                                        ]
                                                    }
                                                )
                                _res = file.stem.split("_", 1)
                                if len(_res) == 2:
                                    _id = _res[0]
                                    _title = sanitize_filename(
                                        _res[1], restricted=True
                                    ).upper()
                                    _name = f"{_id}_{_title}"
                                else:
                                    _id = None
                                    _title = None
                                    _name = sanitize_filename(
                                        file.stem, restricted=True
                                    ).upper()

                                if _id and self.asyncdl.args.deep_aldl:
                                    insert_videoscached(_id, file)
                                else:
                                    insert_videoscached(_name, file)

                        except Exception as e:
                            self.logger.error(
                                f"[videos_cached][{list_folders_to_scan[folder]}]{repr(e)}"
                            )

                        else:
                            last_time_sync.update(
                                {
                                    list_folders_to_scan[folder]: str(datetime.now())
                                    if force_local
                                    else str(self.asyncdl.launch_time)
                                }
                            )

                    self._localstorage.dump_info(
                        self._videoscached, last_time_sync, local=force_local
                    )

                    self.logger.info(
                        f"[videos_cached] Total videos cached: [{len(self._videoscached)}]"
                    )

                    if not force_local:
                        self.asyncdl.videos_cached = self._videoscached.copy()

                    _finished.set()

                    if not force_local:
                        try:
                            if self._repeated:
                                self.logger.warning(
                                    "[videos_cached] Please check vid repeated in logs"
                                )
                                self.logger.debug(
                                    f"[videos_cached] videos repeated: \n {self._repeated}"
                                )

                            if self._dont_exist:
                                self.logger.warning(
                                    "[videos_cached] Pls check vid dont exist in logs"
                                )
                                self.logger.debug(
                                    f"[videos_cached] videos dont exist: \n{self._dont_exist}"
                                )

                            if self._repeated_by_xattr:
                                self.logger.warning(
                                    "[videos_cached] Please check vid repeated by xattr"
                                )
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
                                (
                                    int(self.asyncdl.launch_time.timestamp()),
                                    file.stat().st_mtime,
                                ),
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
                                (
                                    int(self.asyncdl.launch_time.timestamp()),
                                    _video_path.stat().st_mtime,
                                ),
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
                            + f'\t\t{" -> ".join([str(_l) for _l in _links_file])}'
                        )

                        for _link in _links_file[:-1]:
                            _link.unlink()
                            _link.symlink_to(_file)
                            _link._accessor.utime(
                                _link,
                                (
                                    int(self.asyncdl.launch_time.timestamp()),
                                    _file.stat().st_mtime,
                                ),
                                follow_symlinks=False,
                            )

                    if len(_links_video_path) > 2:
                        self.logger.debug(
                            "[videos_cached_deep]\nvideopath symlink: "
                            + f"{str(_video_path)}\n\t\t"
                            + f'{" -> ".join([str(_l) for _l in _links_video_path])}'
                        )

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
                        + f'{" -> ".join([str(_l) for _l in _links_video_path])}'
                    )


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
    w=5,
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
    headers=[],
    collection=[],
    dlcaching=False,
    path=None,
    caplinks=False,
    verbose=True,
    vv=False,
    quiet=False,
    aria2c=True,
    subt=True,
    xattr=True,
    nosymlinks=False,
    check_speed=True,
    deep_aldl=False,
    downloader_ytdl=False,
    use_path_pl=False,
    use_cookies=True,
    no_embed=False,
    rep_pause=False,
    rpcport=6800,
    enproxy=False,
    nocheckcert=True,
    outtmpl="%(id)s_%(title)s.%(ext)s",
)


def get_ytdl(_args):
    return init_ytdl(_args)