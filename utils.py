import argparse
import asyncio
import contextlib
import copy
import functools
import json
import logging
import logging.config
import random
import re
import shutil
import signal
import subprocess
import threading
import time
import PySimpleGUI as sg
import psutil
import proxy
import xattr
from statistics import median
from queue import Empty, Queue

from concurrent.futures import (
    ThreadPoolExecutor,
    wait as wait_thr,
    as_completed
)
from datetime import datetime

from pathlib import Path
from bisect import bisect
from typing import (
    List,
    Tuple,
    Union,
    Dict,
    Coroutine,
    Any,
    Iterable,
    cast
)

import queue

from asgiref.sync import (
    sync_to_async,
)
from ipaddress import ip_address
import httpx

from yt_dlp.extractor.commonwebdriver import (
    CONFIG_EXTRACTORS,
    SeleniumInfoExtractor,
    StatusStop,
    dec_on_exception,
    dec_retry_error,
    limiter_0_1,
    limiter_1,
    limiter_5,
    limiter_15,
    limiter_non,
    ReExtractInfo,
    ConnectError,
    StatusError503,
    my_dec_on_exception
)

from yt_dlp.extractor.nakedsword import NakedSwordBaseIE
from yt_dlp.mylogger import MyLogger
from yt_dlp.utils import (
    get_domain,
    js_to_json,
    prepend_extension,
    sanitize_filename,
    smuggle_url,
    traverse_obj,
    try_get,
    unsmuggle_url
)

from yt_dlp import YoutubeDL

assert unsmuggle_url
assert smuggle_url
assert prepend_extension
assert get_domain
assert NakedSwordBaseIE
assert CONFIG_EXTRACTORS
assert SeleniumInfoExtractor
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
assert cast

PATH_LOGS = Path(Path.home(), "Projects/common/logs")

CONF_DASH_SPEED_PER_WORKER = 102400

CONF_FIREFOX_PROFILE = "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/b33yk6rw.selenium"
CONF_FIREFOX_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/111.0"
CONF_HLS_SPEED_PER_WORKER = 102400 / 8  # 512000
CONF_HLS_RESET_403_TIME = 100
CONF_TORPROXIES_HTTPPORT = 7070
CONF_PROXIES_MAX_N_GR_HOST = 10  # 10
CONF_PROXIES_N_GR_VIDEO = 8  # 8
CONF_PROXIES_BASE_PORT = 12000

CONF_ARIA2C_MIN_SIZE_SPLIT = 1048576  # 1MB 10485760 #10MB
CONF_ARIA2C_SPEED_PER_CONNECTION = 102400  # 102400 * 1.5# 102400
CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED = _min = 240  # 120

CONF_ARIA2C_N_CHUNKS_CHECK_SPEED = _min//4  # 60
CONF_ARIA2C_TIMEOUT_INIT = 20
CONF_INTERVAL_GUI = 0.2

CONF_ARIA2C_EXTR_GROUP = ["tubeload", "redload", "highload", "embedo", "streamsb"]
CONF_AUTO_PASRES = ["doodstream"]


def put_sequence(q: Union[queue.Queue, asyncio.Queue], seq: Iterable) -> Union[queue.Queue, asyncio.Queue]:

    if seq:
        queue_ = getattr(q, 'queue', getattr(q, '_queue', None))
        assert queue_ is not None
        queue_.extend(seq)
    return q


class LocalStorage:

    from filelock import FileLock

    lock = FileLock(Path(PATH_LOGS, "files_cached.json.lock"))
    local_storage = Path(PATH_LOGS, "files_cached.json")
    prev_local_storage = Path(PATH_LOGS, "prev_files_cached.json")

    config_folders = {
        "local": Path(Path.home(), "testing"),
        "pandaext4": Path("/Volumes/Pandaext4/videos"),
        "datostoni": Path("/Volumes/DatosToni/videos"),
        "wd1b": Path("/Volumes/WD1B/videos"),
        "wd5": Path("/Volumes/WD5/videos"),
        "wd8_1": Path("/Volumes/WD8_1/videos"),
        "wd8_2": Path("/Volumes/WD8_2/videos"),
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
        """"
        Dump videos_cached info to FileExistsError
        """

        def getter(x):
            if "Pandaext4/videos" in x:
                return "pandaext4"
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
            "datostoni": {},
            "wd8_1": {},
            "wd8_2": {},
        }

        _upt_temp.update({"last_time_sync": last_time_sync})

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


class MySyncAsyncEvent:

    def __init__(self, name: Union[str, None] = None, initset: bool = False):
        if name:
            self.name = name
        self._cause = "noinfo"
        self.event = threading.Event()
        self.aevent = asyncio.Event()
        self._flag = False
        if initset:
            self.set()

    def __repr__(self):
        cls = self.__class__
        status = 'set' if self._flag else 'unset'
        _res = f"<{cls.__module__}.{cls.__qualname__} at {id(self):#x}: {status}"
        _res += f"\n\tname: {self.name if hasattr(self, 'name') else 'noname'}"
        _res += f"\n\tsync event: {repr(self.event)}\n\tasync event: {repr(self.aevent)}\n>"
        return _res

    def set(self, cause: Union[str, None] = "noinfo"):

        self.aevent.set()
        self.event.set()
        if cause is None:
            cause = "noinfo"
        self._cause = cause
        if not self._flag:
            self._flag = True

    def is_set(self) -> Union[str, bool]:
        """Return True if and only if the internal flag is true."""

        if self._flag:
            return self._cause
        else:
            return False

    def clear(self):

        self.aevent.clear()
        self.event.clear()
        self._flag = False
        self._cause = "noinfo"

    def wait(self, timeout: Union[float, None] = None) -> bool:
        return self.event.wait(timeout=timeout)

    async def async_wait(self):
        return await self.aevent.wait()


class ProgressTimer:
    TIMER_FUNC = time.monotonic

    def __init__(self):
        self._last_ts = self.TIMER_FUNC()

    def __repr__(self):
        return (f"{self.elapsed_seconds():.2f}")

    def reset(self):
        # self._last_ts += self.elapsed_seconds()
        self._last_ts = self.TIMER_FUNC()

    def elapsed_seconds(self) -> float:
        return self.TIMER_FUNC() - self._last_ts

    def has_elapsed(self, seconds: float) -> bool:
        assert seconds > 0.0
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


class SpeedometerMA:
    TIMER_FUNC = time.monotonic

    def __init__(self, initial_bytes: int = 0, upt_time: Union[int, float] = 1.0,
                 ave_time: Union[int, float] = 5.0):
        self.ts_data = [(self.TIMER_FUNC(), initial_bytes)]
        self.timer = ProgressTimer()
        self.last_value = None
        self.UPDATE_TIMESPAN_S = float(upt_time)
        self.AVERAGE_TIMESPAN_S = float(ave_time)

    def __call__(self, byte_counter: int):
        time_now = self.TIMER_FUNC()

        # only append data older than 50ms
        if time_now - self.ts_data[-1][0] > 0.05:
            self.ts_data.append((time_now, byte_counter))

        # remove older entries
        idx = max(0, bisect(self.ts_data, (time_now - self.AVERAGE_TIMESPAN_S,)) - 1)
        self.ts_data[0:idx] = ()

        diff_time = time_now - self.ts_data[0][0]
        speed = (byte_counter - self.ts_data[0][1]) / diff_time if diff_time else None
        if self.timer.has_elapsed(seconds=self.UPDATE_TIMESPAN_S):
            self.last_value = speed

        return self.last_value or speed


class SmoothETA:
    def __init__(self):
        self.last_value = None

    def __call__(self, value):
        if value <= 0:
            return 0

        time_now = time.monotonic()
        if self.last_value:
            predicted = self.last_value - time_now
            if predicted <= 0:
                deviation = float("inf")
            else:
                deviation = max(predicted, value) / min(predicted, value)

            if deviation < 1.25:
                return predicted

        self.last_value = time_now + value
        return value


class SignalHandler:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print(signum)
        print("Exiting gracefully")
        self.KEEP_PROCESSING = False


class long_operation_in_thread:
    def __init__(self, name: str) -> None:
        self.name = name  # name of thread for logging

    def __call__(self, func):
        name = self.name

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> MySyncAsyncEvent:
            stop_event = MySyncAsyncEvent(name)
            thread = threading.Thread(
                target=func, name=name, args=args,
                kwargs={"stop_event": stop_event, **kwargs}, daemon=True)
            thread.start()
            return stop_event
        return wrapper


############################################################
# """                     SYNC ASYNC                     """
############################################################


def wait_for_either(events, timeout=None):

    if not isinstance(events, Iterable):
        events = [events]

    if (_res := [getattr(ev, 'name', 'noname') for ev in events if ev.is_set()]):
        return _res[0]
    else:
        def check_timeout(_st, _n):
            if _n is None:
                return False
            else:
                return (time.monotonic() - _st >= _n)

        start = time.monotonic()
        while True:
            if (_res := [getattr(ev, 'name', 'noname') for ev in events if ev.is_set()]):
                return _res[0]
            elif check_timeout(start, timeout):
                return 'TIMEOUT'
            time.sleep(CONF_INTERVAL_GUI)


async def async_waitfortasks(
        fs: Union[Iterable, Coroutine, asyncio.Task, None] = None,
        timeout: Union[float, None] = None,
        events: Union[Iterable, asyncio.Event, MySyncAsyncEvent, None] = None,
        cancel_tasks: bool = True,
        **kwargs
) -> dict[str, Union[float, Exception, Iterable, asyncio.Task, str, Any]]:

    _final_wait = {}
    _tasks: dict[asyncio.Task, str] = {}

    _background_tasks = kwargs.get('background_tasks', set())

    if fs:
        if not isinstance(fs, Iterable):
            fs = [fs]
        for _fs in fs:
            if not isinstance(_fs, asyncio.Task):
                _el = asyncio.create_task(_fs)
                _background_tasks.add(_el)
                _el.set_name(f'[waitfortasks]{_fs.__name__}')
                _el.add_done_callback(_background_tasks.discard)
                _tasks.update({_el: "task"})
            else:
                _tasks.update({_fs: "task"})

        _one_task_to_wait_tasks = asyncio.create_task(
            asyncio.wait(_tasks, return_when=asyncio.ALL_COMPLETED))

        _background_tasks.add(_one_task_to_wait_tasks)
        _one_task_to_wait_tasks.add_done_callback(_background_tasks.discard)

        _final_wait.update({_one_task_to_wait_tasks: "tasks"})

    if events:
        if not isinstance(events, Iterable):
            events = [events]

        def getter(ev):
            if hasattr(ev, 'name'):
                return f"_{ev.name}"
            return ""

        _tasks_events = {}

        for event in events:
            if isinstance(event, asyncio.Event):
                _tasks_events.update(
                    {asyncio.create_task(event.wait()):
                     f"event{getter(event)}"})
            elif isinstance(event, MySyncAsyncEvent):
                _tasks_events.update(
                    {asyncio.create_task(event.async_wait()):
                     f"event{getter(event)}"})

            for _task in _tasks_events:
                _background_tasks.add(_task)
                _task.add_done_callback(_background_tasks.discard)

        _final_wait.update(_tasks_events)

    if not _final_wait:
        if timeout:
            _task_sleep = asyncio.create_task(asyncio.sleep(timeout*2))
            _background_tasks.add(_task_sleep)
            _task_sleep.add_done_callback(_background_tasks.discard)
            _tasks.update({_task_sleep: "task"})
            _final_wait.update(_tasks)
        else:
            return {"timeout": "nothing to await"}

    done, pending = await asyncio.wait(
        _final_wait, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)

    res: dict[str, Union[float, Exception,
                         Iterable, asyncio.Task, str, Any]] = {}

    try:
        if not done:
            if timeout:
                res = {"timeout": timeout}
            else:
                raise Exception("not done with no timeout")
        else:
            _task = done.pop()
            _label = _final_wait.get(_task, "")
            if _label.startswith("event"):

                def getname(x, task) -> Union[str, asyncio.Task]:
                    if "event_" in x:
                        return x.split("event_")[1]
                    else:
                        return task

                res = {"event": getname(_label, _task)}

            elif fs:
                d, p = _task.result()
                _results = [_d.result() for _d in d if not _d.exception()]
                if len(_results) == 1:
                    _results = _results[0]
                res = {"result": _results}

    except Exception as e:
        res = {"exception": e}
    finally:
        try:
            for p in pending:
                p.cancel()
                if _final_wait.get(p) == "tasks" and cancel_tasks:
                    for _task in _tasks:
                        _task.cancel()
                        pending.add(_task)
            if pending:
                await asyncio.wait(pending)

        except Exception:
            pass

        return res


@contextlib.asynccontextmanager
async def async_lock(lock: Union[threading.Lock, contextlib.nullcontext, None] = None):

    if (isinstance(lock, contextlib.nullcontext)) or not lock:
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


def wait_time(n: Union[int, float],
              event: Union[threading.Event,
                           MySyncAsyncEvent, None] = None):
    _started = time.monotonic()
    if not event:
        time.sleep(n)  # dummy
        return time.monotonic() - _started
    else:
        _res = event.wait(timeout=n)
        if not _res:
            return time.monotonic() - _started
        else:
            return


async def async_wait_until(timeout, cor=None, args=(None,), kwargs={}, interv=CONF_INTERVAL_GUI):
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


def wait_until(timeout, statement=None, args=(None,), kwargs={},  interv=CONF_INTERVAL_GUI):
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
# """                     SYNC ASYNC                     """
############################################################

############################################################
# """                     INIT                     """
############################################################


def init_logging(file_path=None):

    # PATH_LOGS = Path(Path.home(), "Projects/common/logs")
    if not file_path:
        config_file = Path(Path.home(), "Projects/common/logging.json")
    else:
        config_file = Path(file_path)

    with open(config_file) as f:
        config = json.loads(f.read())

    config["handlers"]["info_file_handler"]["filename"] = config["handlers"][
        "info_file_handler"
    ]["filename"].format(path_logs=str(PATH_LOGS))

    logging.config.dictConfig(config)

    for log_name, log_obj in logging.Logger.manager.loggerDict.items():
        if log_name.startswith("proxy"):
            logger = logging.getLogger(log_name)
            logger.setLevel(logging.INFO)

    logger = logging.getLogger("proxy.http.proxy.server")
    logger.setLevel(logging.WARNING)
    logger = logging.getLogger("proxy.core.base.tcp_server")
    logger.setLevel(logging.WARNING)
    logger = logging.getLogger("proxy.http.handler")
    logger.setLevel(logging.ERROR)


def init_argparser():

    parser = argparse.ArgumentParser(
        description="Async downloader videos / playlist videos HLS / HTTP"
    )
    parser.add_argument("-w", help="Number of DL workers",
                        default="5", type=int)
    parser.add_argument(
        "--winit",
        help="Number of init workers, default is same number for DL workers",
        default="0",
        type=int,
    )
    parser.add_argument(
        "-p", "--parts", help="Number of workers for each DL",
        default="16", type=int
    )
    parser.add_argument(
        "--format",
        help="Format preferred of the video in youtube-dl format",
        default="bv*+ba/b",
        type=str,
    )
    parser.add_argument(
        "--sort", help="Formats sort preferred",
        default="ext:mp4:mp4a", type=str
    )
    parser.add_argument(
        "--index", help="index of a video in a playlist",
        default=None, type=int
    )
    parser.add_argument(
        "--file", help="jsonfiles", action="append",
        dest="collection_files", default=[]
    )
    parser.add_argument(
        "--checkcert", help="checkcertificate",
        action="store_true", default=False
    )
    parser.add_argument("--ytdlopts", help="init dict de conf", default="",
                        type=str)
    parser.add_argument("--proxy", default=None, type=str)
    parser.add_argument("--useragent", default=CONF_FIREFOX_UA, type=str)
    parser.add_argument("--first", default=None, type=int)
    parser.add_argument("--last", default=None, type=int)
    parser.add_argument(
        "--nodl", help="not download", action="store_true", default=False
    )
    parser.add_argument("--headers", default="", type=str)
    parser.add_argument("-u", action="append", dest="collection", default=[])
    parser.add_argument(
        "--nodlcaching",
        help="dont get new cache videos dl, use previous",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--dlcaching",
        help="force to check external storage",
        action="store_true",
        default=False,
    )
    parser.add_argument("--path", default=None, type=str)
    parser.add_argument("--caplinks", action="store_true", default=False)
    parser.add_argument(
        "-v", "--verbose", help="verbose", action="store_true", default=False
    )
    parser.add_argument("--vv", help="verbose plus", action="store_true",
                        default=False)
    parser.add_argument(
        "-q", "--quiet", help="quiet", action="store_true", default=False
    )
    parser.add_argument(
        "--aria2c",
        help="use of external aria2c running in port [PORT]. By default PORT=6800. Set to 'no' to disable",
        default="6800",
        type=str,
    )
    parser.add_argument("--nosymlinks", action="store_true", default=False)
    parser.add_argument("--use-http-failover", action="store_true",
                        default=False)
    parser.add_argument("--use-path-pl", action="store_true", default=False)
    parser.add_argument("--use-cookies", action="store_true", default=False)
    parser.add_argument("--no-embed", action="store_true", default=False)
    parser.add_argument("--rep-pause", action="store_true", default=False)

    args = parser.parse_args()

    if args.winit == 0:
        args.winit = args.w

    if args.aria2c == "no":
        args.rpcport = None
        args.aria2c = False
    else:
        args.rpcport = int(args.aria2c)
        args.aria2c = True

    if args.path and len(args.path.split("/")) == 1:
        args.path = str(Path(Path.home(), "testing", args.path))

    if args.vv:
        args.verbose = True

    args.enproxy = True
    if args.proxy == "no":
        args.enproxy = False
        args.proxy = None

    if args.dlcaching:
        args.nodlcaching = False
    else:
        args.nodlcaching = True

    if args.checkcert:
        args.nocheckcert = False
    else:
        args.nocheckcert = True

    return args


def find_in_ps(pattern, value=None):
    res = subprocess.run(
            ["ps", "-u", "501", "-x", "-o", "pid,tty,command"],
            encoding="utf-8",
            capture_output=True,
        ).stdout
    mobj = re.findall(pattern, res)
    if not value or str(value) in mobj:
        return mobj


def init_aria2c(args):

    logger = logging.getLogger("asyncDL")

    if (mobj := find_in_ps(r"aria2c.+--rpc-listen-port ([^ ]+).+",
                           value=args.rpcport)):
        mobj.sort()
        args.rpcport = int(mobj[-1]) + 100

    _proc = subprocess.Popen(
        f"aria2c --rpc-listen-port {args.rpcport} --enable-rpc",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )

    _proc.poll()
    if (_proc.returncode not in
        (0, None) or not find_in_ps(r"aria2c.+--rpc-listen-port ([^ ]+).+",
                                    value=args.rpcport)):
        raise Exception(
            f"[init_aria2c] couldnt run aria2c in port {args.rpcport} - {_proc}")

    logger.info(f"[init_aria2c] running on port: {args.rpcport}")

    return _proc

############################################################
# """                     INIT                     """
############################################################

############################################################
# """                     IP PROXY                     """
############################################################


class myIP:
    URLS_API_GETMYIP = {
        "httpbin": {"url": "https://httpbin.org/get", "key": "origin"},
        "ipify": {"url": "https://api.ipify.org?format=json", "key": "ip"},
        "ipapi": {"url": "http://ip-api.com/json", "key": "query"}
    }

    @staticmethod
    def _get_rtt(ip):
        res = subprocess.run(
            ["ping", "-c", "10", "-q", "-S", "192.168.1.128", ip],
            encoding="utf-8",
            capture_output=True,
        ).stdout
        _tavg = try_get(re.findall(r"= [^\/]+\/([^\/]+)\/", res),
                        lambda x: float(x[0]))
        return {"ip": ip, "time": _tavg}

    @classmethod
    def get_ip(cls, key=None, timeout=1, api="ipify"):

        if api not in cls.URLS_API_GETMYIP:
            raise Exception("api not supported")

        _urlapi = cls.URLS_API_GETMYIP[api]['url']
        _keyapi = cls.URLS_API_GETMYIP[api]['key']

        try:

            _proxies = {'all://': f'http://127.0.0.1:{key}'} if key else None
            myip = try_get(httpx.get(
                _urlapi, timeout=httpx.Timeout(timeout=timeout),
                proxies=_proxies, follow_redirects=True),  # type: ignore
                lambda x: x.json().get(_keyapi))
            return myip
        except Exception as e:
            return repr(e)

    @classmethod
    def get_myiptryall(cls, key=None, timeout=1):

        def is_ipaddr(res):
            try:
                ip_address(res)
                return True
            except Exception:
                return False
        exe = ThreadPoolExecutor(thread_name_prefix="getmyip")
        futures = {exe.submit(cls.get_ip, key=key, timeout=timeout, api=api):
                   api for api in cls.URLS_API_GETMYIP}
        for el in as_completed(futures):
            if not el.exception() and is_ipaddr(_res := el.result()):
                exe.shutdown(wait=False, cancel_futures=True)
                return _res
            else:
                continue

    @classmethod
    def get_myip(cls, key=None, timeout=1):
        return cls.get_myiptryall(key=key, timeout=timeout)


def get_myip(key=None, timeout=2):
    return myIP.get_ip(key=key, timeout=timeout)


class TorGuardProxies:
    CONF_TORPROXIES_LIST_HTTPPORTS = [489, 23, 7070, 465, 993, 282, 778, 592]
    CONF_TORPROXIES_COUNTRIES = [
        "fn", "no", "bg", "pg", "it", "fr", "sp", "ire",
        "ice", "cz", "aus", "ger", "uk", "uk.man", "ro",
        "slk", "nl", "hg", "bul"
    ]
    CONF_TORPROXIES_DOMAINS = [
        f"{cc}.secureconnect.me"
        for cc in CONF_TORPROXIES_COUNTRIES
    ]
    CONF_TORPROXIES_NOK = Path(PATH_LOGS, 'bad_proxies.txt')

    @classmethod
    def test_proxies_rt(cls, routing_table, timeout=2):
        logger = logging.getLogger("torguardprx")
        logger.info("[init_proxies] starting test proxies")
        with ThreadPoolExecutor() as exe:
            futures = {exe.submit(get_myip, key=_key, timeout=timeout): _key
                       for _key in list(routing_table.keys())}

        bad_pr = []

        for fut in futures:
            _ip = fut.result()
            if _ip != routing_table[futures[fut]]:
                logger.debug(
                    f"[{futures[fut]}] test: {_ip} expect res: " +
                    f"{routing_table[futures[fut]]}")
                bad_pr.append(routing_table[futures[fut]])

        return bad_pr

    @classmethod
    def test_proxies_raw(cls, list_ips, port=CONF_TORPROXIES_HTTPPORT, timeout=2):
        logger = logging.getLogger("torguardprx")
        cmd_gost = [
            f"gost -L=:{CONF_PROXIES_BASE_PORT + 2000 + i} "
            f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip}:{port}"
            for i, ip in enumerate(list_ips)
        ]
        routing_table = {
            CONF_PROXIES_BASE_PORT + 2000 + i: ip
            for i, ip in enumerate(list_ips)
        }
        proc_gost = []
        for cmd in cmd_gost:
            _proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, shell=True)
            _proc.poll()
            if _proc.returncode:
                logger.error(
                    f"[init_proxies] returncode[{_proc.returncode}] to cmd[{cmd}]")
                raise Exception("init proxies error")
            else:
                proc_gost.append(_proc)
            time.sleep(0.05)
        _res_ps = subprocess.run(["ps"], encoding="utf-8", capture_output=True).stdout
        logger.debug(f"[init_proxies] %no%\n\n{_res_ps}")
        _res_bad = cls.test_proxies_rt(routing_table, timeout=timeout)
        _line_ps_pr = []
        for _ip in _res_bad:
            if (_temp := try_get(re.search(rf".+{_ip}\:\d+", _res_ps), lambda x: x.group() if x else None)):
                _line_ps_pr.append(_temp)
        logger.info(
            f"[init_proxies] check in ps print equal number of bad ips: res_bad [{len(_res_bad)}] " +
            f"ps_print [{len(_line_ps_pr)}]")

        for proc in proc_gost:
            proc.kill()
            proc.poll()

        cached_res = cls.CONF_TORPROXIES_NOK
        with open(cached_res, "w") as f:
            _test = "\n".join(_res_bad)
            f.write(_test)

        return _res_bad

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
        timeout=2
    ) -> Tuple[List, Dict]:

        logger = logging.getLogger("torguardprx")

        logger.info("[init_proxies] start")

        IPS_SSL = []

        for domain in cls.CONF_TORPROXIES_DOMAINS:
            IPS_SSL += cls.get_ips(domain)

        cached_res = cls.CONF_TORPROXIES_NOK
        if cached_res.exists() and (
            (datetime.now() - datetime.fromtimestamp(
                cached_res.stat().st_mtime)).seconds
            < 7200
        ):  # every 2h we check the proxies
            with open(cached_res, "r") as f:
                _content = f.read()
            _bad_ips = [_ip for _ip in _content.split("\n") if _ip]
        else:
            _bad_ips = cls.test_proxies_raw(IPS_SSL, port=port,
                                            timeout=timeout)

        for _ip in _bad_ips:
            if _ip in IPS_SSL:
                IPS_SSL.remove(_ip)

        _ip_main = random.choice(IPS_SSL)

        IPS_SSL.remove(_ip_main)

        _ips = random.sample(IPS_SSL, num * (size + 1))

        def grouper(iterable, n, *, incomplete='fill', fillvalue=None):
            from itertools import zip_longest
            args = [iter(iterable)] * n
            if incomplete == 'fill':
                return zip_longest(*args, fillvalue=fillvalue)
            if incomplete == 'strict':
                return zip(*args, strict=True)  # type: ignore
            if incomplete == 'ignore':
                return zip(*args)
            else:
                raise ValueError('Expected fill, strict, or ignore')

        FINAL_IPS = list(grouper(_ips, (size + 1)))

        cmd_gost_s = []

        routing_table = {}

        for j in range(size + 1):

            cmd_gost_s.extend(
                [
                    f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*i + j} "
                    f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[j]}:{port}"
                    for i, ip in enumerate(FINAL_IPS)
                ]
            )

            routing_table.update(
                {
                    (CONF_PROXIES_BASE_PORT + 100 * i + j): ip[j]
                    for i, ip in enumerate(FINAL_IPS)
                }
            )

        cmd_gost_main = [
            f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*num + 99} "
            f"-F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{_ip_main}:{port}"
        ]
        routing_table.update(
            {CONF_PROXIES_BASE_PORT + 100 * num + 99: _ip_main})

        cmd_gost_group = [
            f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*i + 50} -F=:8899"
            for i in range(num)
        ]

        cmd_gost = cmd_gost_s + cmd_gost_group + cmd_gost_main

        logger.debug(f"[init_proxies] {cmd_gost}")
        logger.debug(f"[init_proxies] {routing_table}")

        proc_gost = []

        try:
            for cmd in cmd_gost:
                logger.debug(cmd)
                _proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, shell=True)
                _proc.poll()
                if _proc.returncode:
                    logger.error(
                        f"[init_proxies] returncode[{_proc.returncode}] to cmd[{cmd}]"
                    )
                    raise Exception("init proxies error")
                else:
                    proc_gost.append(_proc)
                time.sleep(0.05)

            logger.info("[init_proxies] done")
            return proc_gost, routing_table

        except Exception as e:
            logger.exception(repr(e))
            if proc_gost:
                for proc in proc_gost:
                    try:
                        proc.kill()
                        proc.poll()
                    except Exception:
                        pass
            return [], {}

############################################################
# """                     IP PROXY                     """
############################################################

############################################################
# """                     YTDLP                           """
############################################################


def ies_close(ies):
    if not ies:
        return
    for ie, ins in ies.items():
        if close := getattr(ins, "close", None):
            try:
                close()
            except Exception:
                pass


def get_extractor(url, ytdl):

    logger = logging.getLogger('asyncdl')
    ies = ytdl._ies
    for ie_key, ie in ies.items():
        try:
            if ie.suitable(url) and (ie_key != "Generic"):
                return (ie_key, ie)
        except Exception as e:
            logger.exception(f'[get_extractor] fail with {ie_key} - {repr(e)}')
    return ("Generic", ies["Generic"])


class myYTDL(YoutubeDL):
    def __init__(self, params: Union[None, dict] = None, auto_init: Union[bool, str] = True, **kwargs):
        self._close: bool = kwargs.get("close", True)
        self.executor: ThreadPoolExecutor = kwargs.get(
            "executor", ThreadPoolExecutor(thread_name_prefix="myYTDL"))
        super().__init__(params=params, auto_init=auto_init)  # type: ignore

    def __exit__(self, *args):

        super().__exit__(*args)
        if self._close:
            self.close()

    async def __aenter__(self):
        return super().__enter__()

    async def __aexit__(self, *args):
        self.__exit__(*args)

    def is_playlist(self, url):
        ie_key, ie = get_extractor(url, self)
        if ie_key == "Generic":
            return (True, ie_key)
        else:
            return (ie._RETURN_TYPE == 'playlist', ie_key)

    async def stop(self):
        _stop = self.params.get('stop')
        if _stop:
            _stop.set()
            await asyncio.sleep(0)
        _stop_dl = self.params.get('stop_dl')
        if _stop_dl:
            for _, _stop in _stop_dl.items():
                _stop.set()
                await asyncio.sleep(0)

    def close(self):
        ies_close(self._ies_instances)

    def extract_info(self, *args, **kwargs) -> Union[dict, None]:
        return super().extract_info(*args, **kwargs)

    def process_ie_result(self, *args, **kwargs) -> dict:
        return super().process_ie_result(*args, **kwargs)

    def sanitize_info(self, *args, **kwargs) -> dict:
        return YoutubeDL.sanitize_info(*args, **kwargs)  # type: ignore

    async def async_extract_info(self, *args, **kwargs) -> dict:
        return await sync_to_async(
            self.extract_info, executor=self.executor)(*args, **kwargs)

    async def async_process_ie_result(self, *args, **kwargs) -> dict:
        return await sync_to_async(
            self.process_ie_result, executor=self.executor)(*args, **kwargs)


class ProxyYTDL(YoutubeDL):
    def __init__(self, **kwargs):
        opts = kwargs.get("opts", {})
        proxy = kwargs.get("proxy", None)
        quiet = kwargs.get("quiet", True)
        verbose = kwargs.get("verbose", False)
        verboseplus = kwargs.get("verboseplus", False)
        self._close = kwargs.get("close", True)
        self.executor = kwargs.get(
            "executor", ThreadPoolExecutor(thread_name_prefix="proxyYTDL"))
        opts["quiet"] = quiet
        opts["verbose"] = verbose
        opts["verboseplus"] = verboseplus
        opts["logger"] = MyLogger(
            logging.getLogger("proxyYTDL"),
            quiet=opts["quiet"],
            verbose=opts["verbose"],
            superverbose=opts["verboseplus"],
        )
        opts["proxy"] = proxy

        super().__init__(params=opts,
                         auto_init="no_verbose_header")  # type: ignore

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self._close:
            self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        self.close()

    def is_playlist(self, url):
        ie_key, ie = get_extractor(url, self)
        if ie_key == "Generic":
            return (True, ie_key)
        else:
            return (ie._RETURN_TYPE == 'playlist', ie_key)

    async def stop(self):
        _stop = self.params.get('stop')
        if _stop:
            _stop.set()
            await asyncio.sleep(0)
        _stop_dl = self.params.get('stop_dl')
        if _stop_dl:
            for _, _stop in _stop_dl.items():
                _stop.set()
                await asyncio.sleep(0)

    def close(self):
        ies_close(self._ies_instances)

    def extract_info(self, *args, **kwargs) -> Union[dict, None]:
        return super().extract_info(*args, **kwargs)

    def process_ie_result(self, *args, **kwargs) -> dict:
        return super().process_ie_result(*args, **kwargs)

    def sanitize_info(self, *args, **kwargs) -> dict:
        return YoutubeDL.sanitize_info(*args, **kwargs)  # type: ignore

    async def async_extract_info(self, *args, **kwargs) -> dict:
        return await sync_to_async(
            self.extract_info, executor=self.executor)(*args, **kwargs)

    async def async_process_ie_result(self, *args, **kwargs) -> dict:
        return await sync_to_async(
            self.process_ie_result, executor=self.executor)(*args, **kwargs)


def is_playlist_extractor(url, ytdl):

    ie_key, ie = get_extractor(url, ytdl)

    if ie_key == "Generic":
        return (True, "Generic")

    ie_name = (
        _iename.lower()
        if type(_iename := getattr(ie, "IE_NAME", "")) is str
        else ""
    )

    ie_tests = str(getattr(ie, "_TESTS", ""))

    _is_pl = any("playlist" in _ for _ in [ie_key.lower(), ie_name, ie_tests])

    return (_is_pl, ie_key)


def init_ytdl(args):

    logger = logging.getLogger("yt_dlp")

    headers = {
        "User-Agent": args.useragent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive",
        "Accept-Language": "en,es-ES;q=0.5",
        "Accept-Encoding": "gzip, deflate",
    }

    ytdl_opts = {
        "retries": 1,
        "extractor_retries": 1,
        "http_headers": headers,
        "proxy": args.proxy,
        "logger": MyLogger(
            logger,
            quiet=args.quiet,
            verbose=args.verbose,
            superverbose=args.vv),
        "verbose": args.verbose,
        "quiet": args.quiet,
        "format": args.format,
        "format_sort": [args.sort],
        "nocheckcertificate": args.nocheckcert,
        "subtitleslangs": ["all"],
        "convertsubtitles": "srt",
        "continuedl": True,
        "updatetime": False,
        "ignore_no_formats_error": True,
        "ignoreerrors": False,
        "no_abort_on_errors": False,
        "extract_flat": "in_playlist",
        "no_color": True,
        "usenetrc": True,
        "skip_download": True,
        "writesubtitles": True,
        "restrictfilenames": True,
        "user_agent": args.useragent,
        "verboseplus": args.vv,
        "sem": {},
        "stop_dl": {},
        "stop": threading.Event(),
        "lock": threading.Lock(),
        "embed": not args.no_embed,
    }

    if args.use_cookies:
        ytdl_opts.update(
            {"cookiesfrombrowser": ("firefox", CONF_FIREFOX_PROFILE, None)}
        )

    if args.ytdlopts:
        ytdl_opts.update(json.loads(js_to_json(args.ytdlopts)))

    ytdl = myYTDL(params=ytdl_opts, auto_init='no_verbose_header')

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

############################################################
# """                     YTDLP                           """
############################################################


def print_tasks(tasks):
    return "\n".join(
        [f"{task.get_name()} : {repr(task.get_coro()).split(' ')[2]}"
         for task in tasks]
    )


def print_threads(threads):
    return "\n".join(
        [f"{thread.getName()} : {repr(thread._target)}" for thread in threads]
    )


def none_to_zero(item):
    return 0 if not item else item


def get_chain_links(f):
    _links = []
    _links.append(f)
    _f = f
    while True:
        if _f.is_symlink():
            _link = _f.readlink()
            _links.append(_link)
            _f = _link
        else:
            break
    return _links


def kill_processes(logger=None, rpcport=None):

    def _log(msg):
        logger.info(msg) if logger else print(msg)

    try:

        term = (
            (subprocess.run(["tty"], encoding="utf-8",
                            capture_output=True).stdout)
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
            rf"(\d+)\s+(?:\?\?|{term})\s+((?:.+browsermob-proxy --port.+|" +
            rf"{_aria2cstr}|geckodriver.+|.+mitmdump.+|java -Dapp.name=browsermob-proxy.+|/Applications/" +
            r"Firefox.app/Contents/MacOS/firefox-bin.+))",
            res,
        )
        mobj2 = re.findall(
            rf"\d+\s+(?:\?\?|{term})\s+/Applications/Firefox.app/Contents/" +
            r"MacOS/firefox-bin.+--profile (/var/folders/[^\ ]+) ",
            res,
        )
        mobj3 = re.findall(rf"(\d+)\s+(?:\?\?|{term})\s+((?:.+async_all\.py))",
                           res)
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


def naturalsize(value, binary=False, gnu=False, format_="6.2f"):

    SUFFIXES = {
        "decimal": ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"),
        "binary": ("KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"),
        "gnu": "KMGTPEZY",
    }

    if gnu:
        suffix = SUFFIXES["gnu"]
    elif binary:
        suffix = SUFFIXES["binary"]
    else:
        suffix = SUFFIXES["decimal"]

    base = 1024 if (gnu or binary) else 1000
    _bytes = float(value)
    abs_bytes = abs(_bytes)

    if abs_bytes == 1 and not gnu:
        return f"{abs_bytes:{format_}} KB"
    elif abs_bytes < base and not gnu:
        return f"{abs_bytes:{format_}} KB"
    elif abs_bytes < base and gnu:
        return f"{abs_bytes:{format_}} B"

    for i, s in enumerate(suffix):
        unit = base ** (i + 2)
        if abs_bytes < unit and not gnu:
            return f"{(base*abs_bytes/unit):{format_}} {s}"
        elif abs_bytes < unit and gnu:
            return f"{(base * abs_bytes / unit):{format_}}{s}"
    if gnu:
        return f"{(base * abs_bytes / unit):{format_}}{s}"  # type: ignore
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
    from urllib3 import connectionpool, poolmanager

    class MyHTTPConnectionPool(connectionpool.HTTPConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPConnectionPool, self).__init__(*args, **kwargs)

    poolmanager.pool_classes_by_scheme[  # type: ignore
        "http"] = MyHTTPConnectionPool


def patch_https_connection_pool(**constructor_kwargs):
    """
    This allows to override the default parameters of the
    HTTPConnectionPool constructor.
    For example, to increase the poolsize to fix problems
    with "HttpSConnectionPool is full, discarding connection"
    call this function with maxsize=16 (or whatever size
    you want to give to the connection pool)
    """
    from urllib3 import connectionpool, poolmanager

    class MyHTTPSConnectionPool(connectionpool.HTTPSConnectionPool):
        def __init__(self, *args, **kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPSConnectionPool, self).__init__(*args, **kwargs)

    poolmanager.pool_classes_by_scheme[  # type: ignore
        "https"] = MyHTTPSConnectionPool


def check_if_dl(info_dict, videos):

    if (not (_id := info_dict.get("id")) or
            not (_title := info_dict.get("title"))):
        return False

    _title = sanitize_filename(_title, restricted=True).upper()
    vid_name = f"{_id}_{_title}"

    return videos.get(vid_name)

############################################################
# """                     PYSIMPLEGUI                     """
############################################################


class TimeoutOccurred(Exception):
    pass


class CountDowns:

    INPUT_TIMEOUT = 2
    DEFAULT_TIMEOUT = 30
    INTERV_TIME = 0.25
    N_PER_SECOND = 1 if INTERV_TIME >= 1 else int(1 / INTERV_TIME)
    PRINT_DIF_IN_SECS = 20
    LF = '\n'
    PROMPT = ''
    _INPUT = Queue()

    def __init__(self, klass, events=None, logger=None):

        self._pre = '[countdown][WAIT503]'
        self.klass = klass
        if not events:
            self.outer_events = []
        elif isinstance(events, (list, tuple)):
            self.outer_events = list(events)
        else:
            self.outer_events = [events]

        self.kill_input = MySyncAsyncEvent('killinput')
        self.logger = logger if logger else logging.getLogger('asyncdl')
        #  atexit.register(self.enable_echo, True)
        self.index_main = None
        self.countdowns = {}
        self.exe = ThreadPoolExecutor(thread_name_prefix='countdown')
        self.futures = {}
        self.lock = threading.Lock()
        self.start_input()

    def start_input(self):
        if 'input' not in self.futures:
            self.futures['input'] = self.exe.submit(self.inputimeout)

    def clean(self):

        self.kill_input.set()
        _futures = [self.futures['input']]
        for _index, _count in self.countdowns.items():
            if _count['status'] == 'running':
                _count['stop'].set()
                _futures.append(self.futures[_index])
        wait_thr(_futures)
        self.futures = {}
        if self.countdowns:
            self.logger.debug(f'{self._pre} COUNTDOWNS:\n{self.countdowns}')
            self.countdowns = {}

    def setup(self, interval=None, print_secs=None):
        if interval and interval <= 1:
            CountDowns.INTERV_TIME = interval
            CountDowns.N_PER_SECOND = 1 if CountDowns.INTERV_TIME >= 1 else int(1 / CountDowns.INTERV_TIME)
        if print_secs:
            CountDowns.PRINT_DIF_IN_SECS = print_secs

    def inputimeout(self):

        self.logger.debug(f'{self._pre} start input')
        _res = None

        while True:

            try:
                _res = [getattr(ev, 'name', 'noname')
                        for ev in self.outer_events + [self.kill_input] if ev.is_set()]

                if _res:
                    break

                _input = CountDowns._INPUT.get(block=True, timeout=CountDowns.INPUT_TIMEOUT)
                if _input == '':
                    _input = self.index_main
                if _input in self.countdowns:
                    self.logger.debug(f'{self._pre} input[{_input}] is index video')
                    self.countdowns[_input]['stop'].set()
                else:
                    self.logger.debug(f'{self._pre} input[{_input}] not index video')

            except Empty:
                pass
            except Exception as e:
                self.logger.exception(f'{self._pre} {repr(e)}')

        if not _res:
            time.sleep(self.INTERV_TIME)
            _res = ["TIMEOUT_INPUT"]

        self.logger.debug(f'{self._pre} return Input: {_res}')
        return _res

    def start_countdown(self, n, index, event=None):

        def send_queue(x):
            if ((x == self.N_PER_SECOND*n) or (x % self.N_PER_SECOND) == 0):
                _msg = f"{self.countdowns[index]['premsg']} {x//self.N_PER_SECOND}"
                self.klass._QUEUE[index].put_nowait(_msg)

        _res = None
        _events = self.outer_events + [self.countdowns[index]['stop']]
        if event:
            _events += [event]

        for i in range(self.N_PER_SECOND*n, 0, -1):
            send_queue(i)
            _res = [getattr(ev, 'name', 'noname') for ev in _events if ev.is_set()]
            if _res:
                break

            time.sleep(self.INTERV_TIME)

        self.klass._QUEUE[index].put_nowait('')

        if not _res:
            _res = ["TIMEOUT_COUNT"]
        self.logger.debug(f"{self.countdowns[index]['premsg']} return Count: {_res}")
        return _res

    def add(self, n=None, index=None, event=None, msg=None):

        _premsg = f'{self._pre}'
        if msg:
            _premsg += msg

        if n is not None and isinstance(n, int) and n > 3:
            timeout = n - 3
        else:
            timeout = self.DEFAULT_TIMEOUT - 3
        time.sleep(3)

        with self.lock:
            if not self.index_main:
                self.index_main = index
        self.logger.info(f'{_premsg} index_main[{self.index_main}]')

        self.countdowns[index] = {
                'index': index,
                'premsg': _premsg,
                'timeout': timeout,
                'status': 'running',
                'stop': MySyncAsyncEvent(f"killcounter[{index}]")}

        _fut = self.exe.submit(self.start_countdown, timeout, index, event=event)
        self.futures[index] = _fut
        self.countdowns[index]['fut'] = _fut

        self.logger.debug(f'{_premsg} added counter \n{self.countdowns}')

        done, _ = wait_thr([_fut])

        self.countdowns[index]['status'] = 'done'

        with self.lock:
            if self.index_main == index:
                self.index_main = None

        _res = ["ERROR"]
        if done:
            for d in done:
                try:
                    _res = d.result()
                    if 'stop' in _res:
                        _res = None
                except Exception as e:
                    self.logger.exception(f'{_premsg} error {repr(e)}')

        self.logger.debug(f'{_premsg} finish wait for counter: {_res}')
        return _res


def init_gui_root():

    #  logger = logging.getLogger("init_gui_root")

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

    window_root = sg.Window(
        "async_downloader",
        layout_root,
        alpha_channel=0.99,
        location=(0, 0),
        finalize=True,
        resizable=True,
    )
    window_root.set_min_size(window_root.size)

    window_root["-ML0-"].expand(True, True, True)
    window_root["-ML1-"].expand(True, True, True)
    window_root["-ML2-"].expand(True, True, True)
    window_root["-ML3-"].expand(True, True, True)

    return window_root


def init_gui_console(pasres_value):

    #  logger = logging.getLogger("init_gui_cons")

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
                    default=pasres_value,
                    enable_events=True,
                ),
                sg.Checkbox(
                    "ResRep",
                    key="-RESETREP-",
                    default=False,
                    enable_events=True,
                ),
                # sg.Checkbox(
                #     "WkInit", key="-WKINIT-", default=True, enable_events=True
                # ),
                sg.Button("+PasRes"),
                sg.Button("-PasRes"),
                sg.Button("DLStatus", key="-DL-STATUS"),
                sg.Button("Info"),
                sg.Button("ToFile"),
                sg.Button("+runwk", key="IncWorkerRun"),
                sg.Button("-runwk", key="DecWorkerRun"),
                sg.Button("#vidwk", key="NumVideoWorkers"),
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

    window_console = sg.Window(
        "Console",
        layout_pygui,
        alpha_channel=0.99,
        location=(0, 500),
        finalize=True,
        resizable=True,
    )
    window_console.set_min_size(window_console.size)
    window_console["-ML-"].expand(True, True, True)

    window_console.bring_to_front()

    return window_console


class FrontEndGUI:

    _PASRES_REPEAT = False
    _PASRES_EXIT = MySyncAsyncEvent("pasresexit")

    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger('FEgui')
        self.list_finish = {}
        self.console_dl_status = False
        if self.asyncdl.args.rep_pause:
            FrontEndGUI._PASRES_REPEAT = True

        self.pasres_time_from_resume_to_pause = 35
        self.pasres_time_in_pause = 8
        self.reset_repeat = False
        self.list_all_old = {
            'init': {},
            'downloading': {},
            'manip': {},
            'finish': {}
        }
        # self.dl_media_str = None
        self.stop = MySyncAsyncEvent("stopfegui")
        self.exit_gui = MySyncAsyncEvent("exitgui")
        self.stop_upt_window = self.upt_window_periodic()
        self.exit_upt = MySyncAsyncEvent("exitupt")
        self.stop_pasres = self.pasres_periodic()
        self.exit_pasres = MySyncAsyncEvent("exitpasres")

        _task = asyncio.create_task(self.gui())
        self.asyncdl.background_tasks.add(_task)
        _task.add_done_callback(self.asyncdl.background_tasks.discard)

    @classmethod
    def pasres_break(cls):
        if FrontEndGUI._PASRES_REPEAT:
            FrontEndGUI._PASRES_REPEAT = False
            FrontEndGUI._PASRES_EXIT.set()
            time.sleep(1)
            return True
        else:
            return False

    @classmethod
    def pasres_continue(cls):
        if not FrontEndGUI._PASRES_REPEAT:
            FrontEndGUI._PASRES_EXIT.clear()
            FrontEndGUI._PASRES_REPEAT = True

    async def gui_root(self, event, values):

        try:
            if 'kill' in event or event == sg.WIN_CLOSED:
                return 'break'
            elif event == 'nwmon':
                self.window_root['ST'].update(values['nwmon'])
            elif event == 'all':
                self.window_root['ST'].update(values['all']['nwmon'])
                if 'init' in values['all']:
                    list_init = values['all']['init']
                    if list_init:
                        upt = '\n\n' + ''.join(list(list_init.values()))
                    else:
                        upt = ''
                    self.window_root['-ML0-'].update(value=upt)
                if 'downloading' in values['all']:
                    list_downloading = values['all']['downloading']
                    _text = ['\n\n-------DOWNLOADING VIDEO------------\n\n']
                    if list_downloading:
                        _text.extend(list(list_downloading.values()))
                    upt = ''.join(_text)
                    self.window_root['-ML1-'].update(value=upt)
                    if self.console_dl_status:
                        upt = '\n'.join(list_downloading.values())
                        sg.cprint(
                            f'\n\n-------STATUS DL----------------\n\n{upt}' +
                            '\n\n-------END STATUS DL------------\n\n')
                        self.console_dl_status = False
                if 'manipulating' in values['all']:
                    list_manipulating = values['all']['manipulating']
                    _text = []
                    if list_manipulating:
                        _text.extend(
                            ["\n\n-------CREATING FILE------------\n\n"])
                        _text.extend(list(list_manipulating.values()))
                    if _text:
                        upt = ''.join(_text)
                    else:
                        upt = ''
                    self.window_root['-ML3-'].update(value=upt)

                if 'finish' in values['all']:
                    self.list_finish.update(values['all']['finish'])

                    if self.list_finish:
                        upt = '\n\n' + ''.join(list(self.list_finish.values()))
                    else:
                        upt = ''

                    self.window_root['-ML2-'].update(value=upt)

            elif event in ('error', 'done', 'stop'):
                self.list_finish.update(values[event])

                if self.list_finish:
                    upt = '\n\n' + ''.join(list(self.list_finish.values()))
                else:
                    upt = ''

                self.window_root['-ML2-'].update(value=upt)

        except Exception as e:
            self.logger.exception(f'[gui_root] {repr(e)}')

    async def gui_console(self, event, values):

        sg.cprint(event, values)
        if event == sg.WIN_CLOSED:
            return 'break'
        elif event in ['Exit']:
            self.logger.debug('[gui_console] event Exit')
            await self.asyncdl.cancel_all_dl()
        elif event in ['-PASRES-']:
            if not values['-PASRES-']:
                FrontEndGUI._PASRES_REPEAT = False
            else:
                FrontEndGUI._PASRES_REPEAT = True
        elif event in ['-RESETREP-']:
            if not values['-RESETREP-']:
                self.reset_repeat = False
            else:
                self.reset_repeat = True
        elif event in ['-DL-STATUS']:
            self.asyncdl.print_pending_tasks()
            if not self.console_dl_status:
                self.console_dl_status = True
        elif event in ['IncWorkerRun']:
            self.asyncdl.WorkersRun.add_worker()
            sg.cprint(
                f'Workers: {self.asyncdl.WorkersRun.max}'
            )
        elif event in ['DecWorkerRun']:
            self.asyncdl.WorkersRun.del_worker()
            sg.cprint(
                f'Workers: {self.asyncdl.WorkersRun.max}'
            )
        elif event in ['TimePasRes']:
            if not values['-IN-']:
                sg.cprint('[pause-resume autom] Please enter number')
                sg.cprint(
                    f'[pause-resume autom] {list(self.asyncdl.list_pasres)}')
            else:
                timers = [timer.strip() for timer in values['-IN-'].split(',')]
                if len(timers) > 2:
                    sg.cprint('max 2 timers')
                else:
                    if any(
                        [
                            (not timer.isdecimal() or int(timer) < 0)
                            for timer in timers
                        ]
                    ):
                        sg.cprint('not an integer, or negative')
                    else:
                        if len(timers) == 2:
                            self.pasres_time_from_resume_to_pause = int(
                                timers[0]
                            )
                            self.pasres_time_in_pause = int(timers[1])
                        else:
                            self.pasres_time_from_resume_to_pause = int(
                                timers[0]
                            )
                            self.pasres_time_in_pause = int(timers[0])

                        sg.cprint(
                            f'[time to resume] {self.pasres_time_from_resume_to_pause} ' +
                            f'[time in pause] {self.pasres_time_in_pause}')

                self.window_console['-IN-'].update(value='')
        elif event in ['NumVideoWorkers']:
            if not values['-IN-']:
                sg.cprint('Please enter number')
            else:
                if not values['-IN-'].split(',')[0].isdecimal():
                    sg.cprint('#vidworkers not an integer')
                else:
                    _nvidworkers = int(values['-IN-'].split(',')[0])
                    if _nvidworkers <= 0:
                        sg.cprint('#vidworkers must be > 0')
                    else:
                        if self.asyncdl.list_dl:
                            _copy_list_dl = self.asyncdl.list_dl.copy()
                            if ',' not in values['-IN-']:
                                self.asyncdl.args.parts = _nvidworkers
                                for _, dl in _copy_list_dl.items():
                                    await dl.change_numvidworkers(_nvidworkers)
                            else:
                                _ind = int(values['-IN-'].split(',')[1])
                                if _ind in _copy_list_dl:
                                    await _copy_list_dl[_ind].change_numvidworkers(_nvidworkers)
                                else:
                                    sg.cprint('DL index doesnt exist')

                        else:
                            sg.cprint('DL list empty')

                self.window_console['-IN-'].update(value='')
        elif event in [
            'ToFile',
            'Info',
            'Pause',
            'Resume',
            'Reset',
            'Stop',
            '+PasRes',
            '-PasRes',
            'StopCount'
        ]:
            if not self.asyncdl.list_dl:
                sg.cprint('DL list empty')

            else:
                _copy_list_dl = self.asyncdl.list_dl.copy()
                _index_list = []
                if (_values := values.get(event)):  # from thread pasres
                    _index_list = [int(el) for el in _values.split(',')]
                elif (not (_values := values['-IN-']) or _values.lower() == 'all'):
                    _index_list = [
                        int(dl.index) for _, dl in _copy_list_dl.items()]
                    self.window_console['-IN-'].update(value='')
                else:
                    if any([
                            any([
                                    not el.isdecimal(), int(el) == 0,
                                    int(el) > len(_copy_list_dl)])
                            for el in values['-IN-'].replace(' ', '').split(',')]):

                        sg.cprint('incorrect numbers of dl')
                    else:
                        _index_list = [int(el) for el in values['-IN-'].replace(' ', '').split(',')]
                    self.window_console['-IN-'].update(value='')

                if _index_list:
                    if event in ['+PasRes', '-PasRes']:
                        sg.cprint(f'[pause-resume autom] before: {list(self.asyncdl.list_pasres)}')
                    info = []
                    for _index in _index_list:
                        if event == 'StopCount':
                            CountDowns._INPUT.put_nowait(str(_index))
                        elif event == '+PasRes':
                            self.asyncdl.list_pasres.add(_index)
                        elif event == '-PasRes':
                            self.asyncdl.list_pasres.discard(_index)
                        elif event == 'Pause':
                            await self.asyncdl.list_dl[_index].pause()
                        elif event == 'Resume':
                            await self.asyncdl.list_dl[_index].resume()
                        elif event == 'Reset':
                            await self.asyncdl.list_dl[
                                _index].reset_from_console()
                        elif event == 'Stop':
                            await self.asyncdl.list_dl[_index].stop()
                        elif event in ['Info', 'ToFile']:
                            _thr = getattr(
                                self.asyncdl.list_dl[_index].info_dl['downloaders'][0],
                                'throttle', None)
                            sg.cprint(f'[{_index}] throttle [{_thr}]')
                            _info = json.dumps(
                                self.asyncdl.list_dl[_index].info_dict)
                            sg.cprint(f'[{_index}] info\n{_info}')
                            info.append(_info)

                        await asyncio.sleep(0)

                    if event in ['+PasRes', '-PasRes']:
                        sg.cprint(f'[pause-resume autom] after: {list(self.asyncdl.list_pasres)}')

                    if event == 'ToFile':
                        _launch_time = self.asyncdl.launch_time.strftime('%Y%m%d_%H%M')
                        _file = Path(Path.home(), 'testing', f'{_launch_time}.json')
                        _data = {'entries': info}
                        with open(_file, "w") as f:
                            f.write(json.dumps(_data))

                        sg.cprint(f"saved to file: {_file}")

    async def gui(self):

        try:

            self.window_console = init_gui_console(FrontEndGUI._PASRES_REPEAT)
            self.window_root = init_gui_root()
            await asyncio.sleep(0)

            while not self.stop.is_set():

                window, event, values = sg.read_all_windows(timeout=0)

                if not window or not event or event == sg.TIMEOUT_KEY:
                    await asyncio.sleep(0)
                    continue

                _res = []
                if window == self.window_console:
                    _res.append(await self.gui_console(event, values))
                elif window == self.window_root:
                    _res.append(await self.gui_root(event, values))

                if 'break' in _res:
                    break

                await asyncio.sleep(0)

        except BaseException as e:
            if not isinstance(e, asyncio.CancelledError):
                self.logger.exception(
                    f'[gui] {repr(e)}'
                )
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:
            self.exit_gui.set()
            self.logger.debug('[gui] BYE')

    def update_window(self, status, nwmon=None):
        list_upt = {}
        list_res = {}

        trans = {
            'manip': ('init_manipulating', 'manipulating'),
            'finish': ('error', 'done', 'stop'),
            'init': 'init',
            'downloading': 'downloading'
        }

        if status == 'all':
            _status = ('init', 'downloading', 'manip', 'finish')
        else:
            if isinstance(status, str):
                _status = (status,)
            else:
                _status = status

        for st in _status:
            list_upt[st] = {}
            list_res[st] = {}

            _copy_list_dl = self.asyncdl.list_dl.copy()

            for i, dl in _copy_list_dl.items():

                if dl.info_dl['status'] in trans[st]:
                    list_res[st].update({i: dl.print_hookup()})

            if list_res[st] == self.list_all_old[st]:
                del list_upt[st]
            else:
                list_upt[st] = list_res[st]
        if nwmon:
            list_upt['nwmon'] = nwmon

        if hasattr(self, 'window_root') and self.window_root:
            self.window_root.write_event_value('all', list_upt)

        for st, val in self.list_all_old.items():
            if st not in list_res:
                list_res.update({st: val})

        self.list_all_old = list_res

    @long_operation_in_thread(name='uptwinthr')
    def upt_window_periodic(self, *args, **kwargs):

        self.logger.debug('[upt_window_periodic] start')
        stop_upt = kwargs['stop_event']
        try:
            progress_timer = ProgressTimer()
            short_progress_timer = ProgressTimer()
            self.list_nwmon = []
            init_bytes_recv = psutil.net_io_counters().bytes_recv
            speedometer = SpeedometerMA(initial_bytes=init_bytes_recv)
            ds = None
            while not stop_upt.is_set():

                if self.asyncdl.list_dl:

                    if progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI):
                        _recv = psutil.net_io_counters().bytes_recv
                        ds = speedometer(_recv)
                        msg = f'RECV: {naturalsize(_recv - init_bytes_recv,True)}  ' +\
                              f'DL: {naturalsize(ds,True)}ps'

                        self.update_window('all', nwmon=msg)
                        if short_progress_timer.has_elapsed(
                                seconds=10*CONF_INTERVAL_GUI):
                            self.list_nwmon.append((datetime.now(), ds))
                    else:
                        time.sleep(CONF_INTERVAL_GUI/4)
                else:
                    time.sleep(CONF_INTERVAL_GUI)
                    progress_timer.reset()
                    short_progress_timer.reset()

        except Exception as e:
            self.logger.exception(f'[upt_window_periodic]: error: {repr(e)}')
        finally:
            if self.list_nwmon:
                try:

                    def _strdate(el):
                        _secs = el[0].second + (el[0].microsecond / 1000000)
                        return f'{el[0].strftime("%H:%M:")}{_secs:06.3f}'

                    _str_nwmon = ', '.join(
                        [
                            f'{_strdate(el)}'
                            for el in self.list_nwmon
                        ]
                    )
                    self.logger.debug(
                        f'[upt_window_periodic] nwmon {len(self.list_nwmon)}]\n{_str_nwmon}')
                except Exception as e:
                    self.logger.exception(f'[upt_window_periodic] {repr(e)}')

            self.exit_upt.set()
            self.logger.debug('[upt_window_periodic] BYE')

    def get_dl_media(self):
        if self.list_nwmon:
            _media = naturalsize(median([el[1] for el in self.list_nwmon]), binary=True)
            return f'DL MEDIA: {_media}ps'

    @long_operation_in_thread(name='pasresthr')
    def pasres_periodic(self, *args, **kwargs):

        self.logger.debug('[pasres_periodic] START')
        stop_event = kwargs['stop_event']
        _start_no_pause = None
        try:
            while not stop_event.is_set():

                if self.asyncdl.list_pasres and FrontEndGUI._PASRES_REPEAT:

                    _waitres_nopause = wait_for_either(
                        [stop_event, FrontEndGUI._PASRES_EXIT], timeout=self.pasres_time_from_resume_to_pause)
                    FrontEndGUI._PASRES_EXIT.clear()
                    if not FrontEndGUI._PASRES_REPEAT:
                        continue
                    if _waitres_nopause == "TIMEOUT" and (_list := list(self.asyncdl.list_pasres)):

                        if not self.reset_repeat:
                            if _start_no_pause:
                                sg.cprint(f'[time resume -> pause] {time.monotonic()-_start_no_pause}')

                            self.window_console.write_event_value(
                                'Pause', ','.join(list(map(str, _list))))
                            time.sleep(1)
                            self.logger.debug('[pasres_periodic]: pauses sent')
                            _start_pause = time.monotonic()
                            _waitres = wait_for_either([stop_event, FrontEndGUI._PASRES_EXIT], timeout=self.pasres_time_in_pause)
                            FrontEndGUI._PASRES_EXIT.clear()
                            self.logger.debug('[pasres_periodic]: start sending resumes')
                            if _waitres == 'TIMEOUT':
                                _time = self.pasres_time_in_pause / len(_list)
                                for _el in _list:
                                    self.window_console.write_event_value('Resume', str(_el))

                                    #  wait_time(random.uniform(0.75 * _time, 1.25 * _time), event=stop_event)
                                    if wait_for_either(
                                        [stop_event, FrontEndGUI._PASRES_EXIT],
                                            timeout=random.uniform(0.75*_time, 1.25*_time)) != "TIMEOUT":

                                        self.window_console.write_event_value('Resume', ','.join(list(map(str, _list))))
                                        break

                                #  wait_for_either(
                                # [stop_event, FrontEndGUI._PASRES_EXIT], timeout=self.pasres_time_from_resume_to_pause)

                            else:
                                # if 'pasresexit' in _waitres:
                                #     FrontEndGUI._PASRES_EXIT.clear()

                                self.window_console.write_event_value(
                                    'Resume', ','.join(list(map(str, _list))))

                            self.logger.debug('[pasres_periodic]: resumes sent, start timer to next pause')
                            sg.cprint(f'[time in pause] {time.monotonic()-_start_pause}')
                            _start_no_pause = time.monotonic()

                        else:
                            self.window_console.write_event_value(
                                'Reset', ','.join(list(map(str, _list))))
                            #   wait_time(
                            #     self.pasres_time_from_resume_to_pause,
                            #     event=stop_event
                            # )
                else:
                    _start_no_pause = None
                    time.sleep(CONF_INTERVAL_GUI)

        except Exception as e:
            self.logger.exception(f'[pasres_periodic]: error: {repr(e)}')
        finally:
            self.exit_pasres.set()
            self.logger.debug('[pasres_periodic] BYE')

    async def close(self):

        self.stop_pasres.set()
        await asyncio.sleep(0)
        self.stop_upt_window.set()
        await asyncio.sleep(0)
        self.logger.debug("[close] start to wait for exit_pasres")
        await self.exit_pasres.async_wait()
        self.logger.debug("[close] end to wait for exit_pasres")
        self.logger.debug("[close] start to wait for exit_upt")
        await self.exit_upt.async_wait()
        self.logger.debug("[close] end to wait for exit_upt")
        self.stop.set()
        await asyncio.sleep(0)
        self.logger.debug("[close] start to wait for exit_gui")
        await self.exit_gui.async_wait()
        self.logger.debug("[close] end to wait for exit_gui")
        if hasattr(self, 'window_console') and self.window_console:
            self.window_console.close()
            del self.window_console
        if hasattr(self, 'window_root') and self.window_root:
            self.window_root.close()
            del self.window_root


class NWSetUp:

    def __init__(self, asyncdl):

        self.asyncdl = asyncdl
        self.logger = logging.getLogger('setupnw')
        self.shutdown_proxy = MySyncAsyncEvent("shutdownproxy")
        self.routing_table = {}
        self.proc_gost = []
        self.proc_aria2c = None
        self.exe = ThreadPoolExecutor(thread_name_prefix='setupnw')

        self._tasks_init = {}
        if self.asyncdl.args.aria2c:
            ainit_aria2c = sync_to_async(init_aria2c, executor=self.exe)
            _task_aria2c = asyncio.create_task(ainit_aria2c(self.asyncdl.args))
            self.asyncdl.background_tasks.add(_task_aria2c)
            _task_aria2c.add_done_callback(self.asyncdl.background_tasks.discard)
            _tasks_init_aria2c = {
                _task_aria2c: 'aria2'
            }
            self._tasks_init.update(_tasks_init_aria2c)
        if self.asyncdl.args.enproxy:
            self.stop_proxy = self.run_proxy_http()
            ainit_proxies = sync_to_async(
                TorGuardProxies.init_proxies, executor=self.exe)
            _task_proxies = asyncio.create_task(ainit_proxies())
            self.asyncdl.background_tasks.add(_task_proxies)
            _task_proxies.add_done_callback(self.asyncdl.background_tasks.discard)
            _task_init_proxies = {_task_proxies: 'proxies'}
            self._tasks_init.update(_task_init_proxies)

    async def init(self):

        if self._tasks_init:
            done, _ = await asyncio.wait(self._tasks_init)
            for task in done:
                try:
                    if self._tasks_init[task] == 'aria2':
                        self.proc_aria2c = task.result()
                    else:
                        self.proc_gost, self.routing_table = task.result()
                        self.asyncdl.ytdl.params[
                            'routing_table'] = self.routing_table
                except Exception as e:
                    self.logger.exception(f'[init] {repr(e)}')

    @long_operation_in_thread(name='proxythr')
    def run_proxy_http(self, *args, **kwargs):

        stop_event: MySyncAsyncEvent = kwargs['stop_event']
        log_level = kwargs.get('log_level', 'INFO')
        try:
            with proxy.Proxy(
                [
                    '--log-level',
                    log_level,
                    '--plugins',
                    'proxy.plugin.ProxyPoolByHostPlugin',
                ]
            ) as p:

                try:
                    self.logger.debug(p.flags)
                    stop_event.wait()
                except BaseException:
                    self.logger.error('context manager proxy')
        finally:
            self.shutdown_proxy.set()

    async def close(self):

        if self.asyncdl.args.enproxy:
            self.logger.debug('[close] proxy')
            self.stop_proxy.set()
            await asyncio.sleep(0)
            self.logger.debug('[close] waiting for http proxy shutdown')
            self.shutdown_proxy.wait()
            self.logger.debug('[close] OK shutdown')

            if self.proc_gost:
                self.logger.debug('[close] gost')
                for proc in self.proc_gost:
                    try:
                        proc.kill()
                    except BaseException as e:
                        self.logger.exception(f'[close] {repr(e)}')

        if self.proc_aria2c:
            self.logger.debug('[close] aria2c')
            self.proc_aria2c.kill()


class LocalVideos:
    def __init__(self, asyncdl, deep=False):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger('videoscached')
        self.deep = deep
        self._videoscached = {}
        self._repeated = []
        self._dont_exist = []
        self._repeated_by_xattr = []
        self._localstorage = LocalStorage()
        self.file_ready: MySyncAsyncEvent = self.get_videos_cached()

    async def aready(self):
        while not self.file_ready.is_set():
            await asyncio.sleep(0)

    def ready(self):
        self.file_ready.wait()

    def upt_local(self):
        self.file_ready.clear()
        self._videoscached = {}
        self._repeated = []
        self._dont_exist = []
        self._repeated_by_xattr = []
        self.file_ready = self.get_videos_cached(local=True)

    @long_operation_in_thread(name='vidcachthr')
    def get_videos_cached(self, *args, **kwargs):

        """
        In local storage, files are saved wihtin the file files.cached.json
        in 5 groups each in different volumnes.
        If any of the volumes can't be accesed in real time, the
        local storage info of that volume will be used.
        """

        _finished: MySyncAsyncEvent = kwargs['stop_event']

        force_local = kwargs.get('local', False)

        self.logger.debug(
            f"[videos_cached] start scanning - nodlcaching[{self.asyncdl.args.nodlcaching}] - local[{force_local}]")

        last_time_sync = {}

        try:

            with self._localstorage.lock:

                self._localstorage.load_info()

                list_folders_to_scan = {}

                last_time_sync = self._localstorage._last_time_sync

                if self.asyncdl.args.nodlcaching or force_local:
                    for _vol, _folder in self._localstorage.config_folders.items():
                        if _vol != 'local':
                            if not force_local:
                                self._videoscached.update(
                                    self._localstorage._data_from_file[_vol])
                        else:
                            list_folders_to_scan.update({_folder: _vol})

                else:
                    for _vol, _folder in self._localstorage.config_folders.items():
                        if not _folder.exists():  # comm failure
                            self.logger.error(f'Fail connect to [{_vol}], will use last info')
                            self._videoscached.update(self._localstorage._data_from_file[_vol])
                        else:
                            list_folders_to_scan.update({_folder: _vol})

                for folder in list_folders_to_scan:

                    try:

                        files = [
                            file
                            for file in folder.rglob('*')
                            if file.is_file()
                            and not file.stem.startswith('.')
                            and (file.suffix.lower() in
                                 ('.mp4', '.mkv', '.zip'))
                        ]

                        for file in files:

                            if not force_local:
                                if not file.is_symlink():
                                    try:
                                        _xattr_desc = xattr.getxattr(
                                            file, 'user.dublincore.description').decode()
                                        if not self._videoscached.get(_xattr_desc):
                                            self._videoscached.update({_xattr_desc: str(file)})
                                        else:
                                            self._repeated_by_xattr.append(
                                                {_xattr_desc: [self._videoscached[_xattr_desc], str(file)]})
                                    except Exception:
                                        pass

                            _res = file.stem.split('_', 1)
                            if len(_res) == 2:
                                _id = _res[0]
                                _title = sanitize_filename(_res[1], restricted=True).upper()
                                _name = f'{_id}_{_title}'
                            else:
                                _name = sanitize_filename(file.stem, restricted=True).upper()

                            if not (_video_path_str := self._videoscached.get(_name)):
                                self._videoscached.update({_name: str(file)})

                            else:
                                _video_path = Path(_video_path_str)
                                if _video_path != file:

                                    if (
                                        not file.is_symlink()
                                        and not _video_path.is_symlink()
                                    ):

                                        # only if both are hard files we have
                                        # to do something, so lets report it
                                        # in repeated files
                                        self._repeated.append(
                                            {
                                                'title': _name,
                                                'indict': _video_path_str,
                                                'file': str(file),
                                            }
                                        )

                                    if self.deep:
                                        self.deep_check(_name, file, _video_path)

                    except Exception as e:
                        self.logger.error(
                            f'[videos_cached][{list_folders_to_scan[folder]}]{repr(e)}')

                    else:
                        last_time_sync.update(
                            {list_folders_to_scan[folder]:
                             str(self.asyncdl.launch_time) if not force_local else str(datetime.now())})

                self._localstorage.dump_info(self._videoscached, last_time_sync, local=force_local)

                self.logger.info(f'[videos_cached] Total videos cached: [{len(self._videoscached)}]')

                if not force_local:
                    self.asyncdl.videos_cached = self._videoscached.copy()

                _finished.set()

                if not force_local:
                    try:

                        if self._repeated:
                            self.logger.warning(
                                '[videos_cached] Please check vid rep in logs')
                            self.logger.debug(
                                f'[videos_cached] videos repeated: \n {self._repeated}')

                        if self._dont_exist:
                            self.logger.warning(
                                '[videos_cached] Pls check vid dont exist in logs')
                            self.logger.debug(
                                f'[videos_cached] videos dont exist: \n{self._dont_exist}')

                        if self._repeated_by_xattr:
                            self.logger.warning(
                                '[videos_cached] Pls check vid repeated by xattr)')
                            self.logger.debug(
                                f'[videos_cached] videos repeated by xattr: \n{self._repeated_by_xattr}')

                    except Exception as e:
                        self.logger.exception(f'[videos_cached] {repr(e)}')

        except Exception as e:
            self.logger.exception(f'[videos_cached] {repr(e)}')

    def deep_check(self, _name, file, _video_path):

        if (
            not file.is_symlink()
            and _video_path.is_symlink()
        ):
            _links = get_chain_links(_video_path)
            if _links[-1] == file:

                if len(_links) > 2:  # chain of at least 2 symlinks
                    self.logger.debug(
                        '[videos_cached_deep]\nfile not symlink: ' +
                        f'{str(file)}\nvideopath symlink: ' +
                        f'{str(_video_path)}\n\t\t' +
                        f'{" -> ".join([str(_l) for _l in _links])}')

                    for _link in _links[0:-1]:
                        _link.unlink()
                        _link.symlink_to(file)
                        _link._accessor.utime(
                            _link,
                            (int(self.asyncdl.launch_time.timestamp()), file.stat().st_mtime),
                            follow_symlinks=False)

                    self._videoscached.update({_name: str(file)})

                else:

                    self.logger.debug(
                        '[videos_cached_deep] \n**file not symlink: ' +
                        f'{str(file)}\nvideopath symlink: ' +
                        f'{str(_video_path)}\n\t\t' +
                        f'{" -> ".join([str(_l) for _l in _links])}')

        elif (

            file.is_symlink()
            and not _video_path.is_symlink()
        ):
            _links = get_chain_links(file)
            if _links[-1] == _video_path:
                if len(_links) > 2:
                    self.logger.debug(
                        '[videos_cached]\nfile symlink: ' +
                        f'{str(file)}\n\t\t' +
                        f'{" -> ".join([str(_l) for _l in _links])}\n' +
                        f'videopath not symlink: {str(_video_path)}')

                    for _link in _links[0:-1]:
                        _link.unlink()
                        _link.symlink_to(_video_path)
                        _link._accessor.utime(
                            _link,
                            (int(self.asyncdl.launch_time.timestamp()), _video_path.stat().st_mtime),
                            follow_symlinks=False)

                self._videoscached.update({_name: str(_video_path)})
                if not _video_path.exists():
                    self._dont_exist.append(
                        {
                            'title': _name,
                            'file_not_exist': str(_video_path),
                            'links': [str(_l) for _l in _links[0:-1]],
                        })
            else:

                self.logger.debug(
                    f'[videos_cached_deep]\n**file symlink: {str(file)}\n' +
                    f'\t\t{" -> ".join([str(_l) for _l in _links])}\n' +
                    f'videopath not symlink: {str(_video_path)}')

        else:

            _links_file = get_chain_links(file)
            _links_video_path = get_chain_links(_video_path)
            if (_file := _links_file[-1]) == _links_video_path[-1]:
                if len(_links_file) > 2:
                    self.logger.debug(
                        f'[videos_cached_deep]\nfile symlink: {str(file)}\n' +
                        f'\t\t{" -> ".join([str(_l) for _l in _links_file])}')

                    for _link in _links_file[0:-1]:
                        _link.unlink()
                        _link.symlink_to(_file)
                        _link._accessor.utime(
                            _link,
                            (int(self.asyncdl.launch_time.timestamp()), _file.stat().st_mtime),
                            follow_symlinks=False)

                if len(_links_video_path) > 2:
                    self.logger.debug(
                        '[videos_cached_deep]\nvideopath symlink: ' +
                        f'{str(_video_path)}\n\t\t' +
                        f'{" -> ".join([str(_l) for _l in _links_video_path])}')

                    for _link in _links_video_path[0:-1]:
                        _link.unlink()
                        _link.symlink_to(_file)
                        _link._accessor.utime(
                            _link,
                            (int(self.asyncdl.launch_time.timestamp()), _file.stat().t_mtime,),
                            follow_symlinks=False)

                self._videoscached.update({_name: str(_file)})

                if not _file.exists():
                    self._dont_exist.append(
                        {
                            "title": _name,
                            "file_not_exist": str(_file),
                            "links": [
                                        str(_l) for _l in
                                        (_links_file[0:-1] + _links_video_path[0:-1])]
                        })

            else:
                self.logger.debug(
                    '[videos_cached_deep]\n**file symlink: ' +
                    f'{str(file)}\n\t\t' +
                    f'{" -> ".join([str(_l) for _l in _links_file])}\n' +
                    f'videopath symlink: {str(_video_path)}\n\t\t' +
                    f'{" -> ".join([str(_l) for _l in _links_video_path])}')

    def get_files_same_id(self):

        config_folders = {
            "local": Path(Path.home(), "testing"),
            "pandaext4": Path("/Volumes/Pandaext4/videos"),
            "datostoni": Path("/Volumes/DatosToni/videos"),
            "wd1b": Path("/Volumes/WD1B/videos"),
            "wd5": Path("/Volumes/WD5/videos"),
            "wd8_1": Path("/Volumes/WD8_1/videos"),
        }

        list_folders = []

        for _vol, _folder in config_folders.items():
            if not _folder.exists():
                self.logger.error(
                    f"failed {_vol}:{_folder}, let get previous info saved in previous files")

            else:
                list_folders.append(_folder)

        files_cached = []
        for folder in list_folders:

            self.logger.info(">>>>>>>>>>>STARTS " + str(folder))

            files = []
            try:

                files = [
                    file
                    for file in folder.rglob("*")
                    if file.is_file()
                    and not file.is_symlink()
                    and "videos/_videos/" not in str(file)
                    and not file.stem.startswith(".")
                    and (file.suffix.lower() in
                         (".mp4", ".mkv", ".ts", ".zip"))
                ]

            except Exception as e:
                self.logger.error(f"[get_files_cached][{folder}] {repr(e)}")

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
                        _res_dict[el[0]] = set([el[1], item[1]])
                    else:
                        _res_dict[el[0]].update([el[1], item[1]])
        _ord_res_dict = sorted(_res_dict.items(), key=lambda x: len(x[1]))
        return _ord_res_dict

############################################################
# """                     PYSIMPLEGUI                    """
############################################################


def _for_print_entry(entry):
    if not entry:
        return
    _entry = copy.deepcopy(entry)

    if _formats := _entry.get("formats"):

        _new_formats = []
        for _format in _formats:
            if len(_formats) > 5:
                _id, _prot = _format["format_id"], _format["protocol"]
                _format = {"format_id": _id, ...: ..., "protocol": _prot}

            else:
                if _frag := _format.get("fragments"):
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
        return
    _info = copy.deepcopy(info)
    if _entries := _info.get("entries"):
        _info["entries"] = [_for_print_entry(_el) for _el in _entries]
        return _info
    else:
        return _for_print_entry(_info)


def _for_print_videos(videos):
    if not videos:
        return
    _videos = copy.deepcopy(videos)

    if isinstance(videos, dict):

        for _, _values in _videos.items():
            if _info := traverse_obj(_values, "video_info"):
                _values["video_info"] = _for_print(_info)

        return _videos

    elif isinstance(videos, list):
        _videos = [_for_print(_vid) for _vid in _videos]
        return _videos
