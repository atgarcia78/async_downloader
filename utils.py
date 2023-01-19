import argparse
import asyncio
import contextlib
import contextvars
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
from concurrent.futures import(
    ThreadPoolExecutor,
    wait as waitfut,
    FIRST_COMPLETED as first_completed_fut)
from datetime import datetime, timedelta

from pathlib import Path
from bisect import bisect
from typing import Optional, List, Tuple, Union, Dict, Coroutine, Any, Callable, TypeVar, Awaitable, Iterable, cast

from asgiref.sync import (

    sync_to_async,
)

PATH_LOGS = Path(Path.home(), "Projects/common/logs")

CONF_DASH_SPEED_PER_WORKER = 102400

CONF_FIREFOX_PROFILE = "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/b33yk6rw.selenium"
CONF_HLS_SPEED_PER_WORKER = 102400 / 8  # 512000
CONF_HLS_RESET_403_TIME = 80

CONF_TORPROXIES_HTTPPORT = 7070

CONF_PROXIES_MAX_N_GR_HOST = 10 # 10
CONF_PROXIES_N_GR_VIDEO = 8  # 8
CONF_PROXIES_BASE_PORT = 12000

CONF_ARIA2C_MIN_SIZE_SPLIT = 1048576  # 1MB 10485760 #10MB
CONF_ARIA2C_SPEED_PER_CONNECTION = 102400  # 102400 * 1.5# 102400
CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED = 240#120
CONF_ARIA2C_N_CHUNKS_CHECK_SPEED = CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED//4#60
CONF_ARIA2C_TIMEOUT_INIT = 20
CONF_INTERVAL_GUI = 0.2

CONF_ARIA2C_EXTR_GROUP = ["tubeload", "redload", "highload", "embedo"]
CONF_AUTO_PASRES = ["doodstream"]


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

        with open(LocalStorage.local_storage, "r") as f:
            self._data_from_file = json.load(f)

        for _key, _data in self._data_from_file.items():
            if _key in list(LocalStorage.config_folders.keys()):
                self._data_for_scan.update(_data)
            elif "last_time_sync" in _key:
                self._last_time_sync.update(_data)
            else:
                self.logger.error(
                    f"found key not registered volumen - {_key}"
                )

    def dump_info(self, videos_cached, last_time_sync):
        
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

        _temp = {
            "last_time_sync": {},
            "local": {},
            "wd5": {},
            "wd1b": {},
            "pandaext4": {},
            "datostoni": {},
            "wd8_1": {},
            "wd8_2": {},
        }

        _temp.update({"last_time_sync": last_time_sync})

        for key, val in videos_cached.items():

            _vol = getter(val)
            if not _vol:
                self.logger.error(
                    f"found file with not registered volumen - {val} - {key}"
                )
            else:
                _temp[_vol].update({key: val})

        shutil.copy(
            str(LocalStorage.local_storage), str(LocalStorage.prev_local_storage)
        )

        with open(LocalStorage.local_storage, "w") as f:
            json.dump(_temp, f)

        self._data_from_file = _temp

class MySyncAsyncEvent:

    def __init__(self, name: Union[str, None]=None):
        if name:
            self.name = name
        self._cause = "noinfo"
        self.event = threading.Event()
        self.aevent = asyncio.Event()
        self._flag = False

    def set(self, cause: Union[str, None]="noinfo"):

        self.aevent.set()
        self.event.set()
        if cause == None:
            cause = "noinfo"
        self._cause = cause
        if not self._flag:
            self._flag = True

    def is_set(self)->Union[str, bool]:
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

    def wait(self, timeout: Union[float, None]=None)->bool:
        return self.event.wait(timeout=timeout)

    async def async_wait(self):
        return await self.aevent.wait()
    
class ProgressTimer:
    TIMER_FUNC = time.monotonic

    def __init__(self):
        self._last_ts = self.TIMER_FUNC()

    def __repr__(self):
        return(f"{self.elapsed_seconds():.2f}")

    def reset(self):
        #self._last_ts += self.elapsed_seconds()
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

class SpeedometerMA:
    TIMER_FUNC = time.monotonic
    #UPDATE_TIMESPAN_S = 1.0#CONF_INTERVAL_GUI#1.0
    #AVERAGE_TIMESPAN_S = 5.0#5.0

    def __init__(self, initial_bytes: int=0, upt_time: Union[int, float]=1.0, ave_time: Union[int, float]=5.0):
        self.ts_data = [(self.TIMER_FUNC(), initial_bytes)]
        self.timer = ProgressTimer()
        self.last_value = None
        self.UPDATE_TIMESPAN_S = float(upt_time)#1.0#CONF_INTERVAL_GUI#1.0
        self.AVERAGE_TIMESPAN_S = float(ave_time)#5.0#5.0

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
        self.name = name
    
    def __call__(self, func):
        name = self.name
        @functools.wraps(func)
        def wrapper(*args, **kwargs)->threading.Event:
            stop_event = threading.Event()
            thread = threading.Thread(target=func, name=name, args=args, kwargs={"stop_event": stop_event, **kwargs}, daemon=True)
            thread.start()
            return stop_event
        return wrapper


############################################################
#"""                     SYNC ASYNC                     """
############################################################

async def async_waitfortasks(
        fs: Union[Iterable, Coroutine, asyncio.Task, None] = None, 
        timeout: Union[float, None] = None, 
        events: Union[Iterable, asyncio.Event, MySyncAsyncEvent, None] = None,
        cancel_tasks=True
)->dict[str, Union[float, Exception, Iterable, asyncio.Task, str, Any]]:
    
    _final_wait = {}    
    _tasks: dict[asyncio.Task, str] = {}
    
    if fs:        
        if not isinstance(fs, Iterable):
            fs = [fs]
        for _el in fs:
            if not isinstance(_el, asyncio.Task):
                _el = asyncio.create_task(_el)
            
            _tasks.update({_el: "task"})

        _one_task_to_wait_tasks = asyncio.create_task(asyncio.wait(_tasks, return_when=asyncio.ALL_COMPLETED))

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
                _tasks_events.update({asyncio.create_task(event.wait()): f"event{getter(event)}"})
            elif isinstance(event, MySyncAsyncEvent):
                _tasks_events.update({asyncio.create_task(event.async_wait()): f"event{getter(event)}"})

        
        _final_wait.update(_tasks_events)
           
    if not _final_wait:
        if timeout:
            _tasks.update({asyncio.create_task(asyncio.sleep(timeout*2)): "task"})
            _final_wait.update(_tasks)
        else:
            return {"timeout": "nothing to await"}
            
    done, pending = await asyncio.wait(_final_wait, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)

    res: dict[str, Union[float, Exception, Iterable, asyncio.Task, str, Any]] = {}
    
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
                
                def getname(x, task)->Union[str, asyncio.Task]:
                    if "event_" in x:
                        return x.split("event_")[1]
                    else: return task

                res = {"event": getname(_label, _task)}

            elif fs:
                d, p = _task.result()
                _results = [_d.result() for _d in d if not _d.exception()]
                if len(_results) == 1: _results = _results[0] 
                res = {"result": _results}
    except Exception as e:        
        res = {"exception": e}    
    finally:
        try:
            if cancel_tasks:
                for p in pending:
                    p.cancel()
                if not res.get("result"):
                    for _task in _tasks:
                        _task.cancel()
                        pending.add(_task)
                if pending:
                    await asyncio.wait(pending)
            else:
                if res.get("result"):
                    for p in pending:
                        p.cancel()
                    await asyncio.wait(pending)
                else:
                    pending_ev = []
                    for p in pending:
                        _label = _final_wait.get(p, "")
                        if _label.startswith("event"): 
                            p.cancel()
                            pending_ev.append(p)
                    if pending_ev:
                        await asyncio.wait(pending_ev)
                    res.update({"pending" : _tasks})
        except Exception:
            pass
    return res


@contextlib.asynccontextmanager
async def async_lock(lock: Union[threading.Lock, contextlib.nullcontext, None]=None):
    
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

def wait_time(n: Union[int, float], event: Union[threading.Event, MySyncAsyncEvent, None] = None):
    _started = time.monotonic()
    if not event:
        time.sleep(n)  # dummy
        return time.monotonic() - _started
    else:
        _res = event.wait(timeout=n)
        if not _res:
            return time.monotonic() - _started
        else: return

async def async_wait_until(timeout, cor=None, args=(None,), kwargs={}, interv=CONF_INTERVAL_GUI):
    _started = time.monotonic()

    if not cor:

        async def _cor(*args, **kwargs):
            return True

    else:
        _cor = cor

    while not (await _cor(*args, **kwargs)):
        if (_t := (time.monotonic() - _started)) >= timeout:
            raise TimeoutError()
        else:
            await async_wait_time(interv)

def wait_until(timeout, statement=None, args=(None,), kwargs={}, interv=CONF_INTERVAL_GUI):
    _started = time.monotonic()

    if not statement:

        def func(*args, **kwargs):
            return True

    else:
        func = statement

    while not func(*args, **kwargs):
        if (_t := (time.monotonic() - _started)) >= timeout:
            raise TimeoutError()
        else:
            time.sleep(interv)


############################################################
#"""                     SYNC ASYNC                     """
############################################################


############################################################
#"""                     INIT                     """
############################################################

def init_logging(file_path=None):

    #PATH_LOGS = Path(Path.home(), "Projects/common/logs")
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


def init_argparser():

    UA_LIST = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:108.0) Gecko/20100101 Firefox/108.0"
    ]

    parser = argparse.ArgumentParser(
        description="Async downloader videos / playlist videos HLS / HTTP"
    )
    parser.add_argument("-w", help="Number of DL workers", default="5", type=int)
    parser.add_argument(
        "--winit",
        help="Number of init workers, default is same number for DL workers",
        default="0",
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
        "--sort", help="Formats sort preferred", default="ext:mp4:mp4a", type=str
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
    parser.add_argument("--proxy", default=None, type=str)
    parser.add_argument("--useragent", default=UA_LIST[0], type=str)
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
    parser.add_argument("--vv", help="verbose plus", action="store_true", default=False)
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
    parser.add_argument("--use-http-failover", action="store_true", default=False)
    parser.add_argument("--use-path-pl", action="store_true", default=False)
    parser.add_argument("--use-cookies", action="store_true", default=False)
    parser.add_argument("--no-embed", action="store_true", default=False)

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
        _path = Path(Path.home(), "testing", args.path)
        args.path = str(_path)

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

    if (mobj:=find_in_ps(r"aria2c.+--rpc-listen-port ([^ ]+).+", value=args.rpcport)):
        mobj.sort()
        args.rpcport = int(mobj[-1]) + 100

    _proc = subprocess.Popen(
        f"aria2c --rpc-listen-port {args.rpcport} --enable-rpc",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )
    
    _proc.poll()
    if _proc.returncode not in (0,None) or not find_in_ps(r"aria2c.+--rpc-listen-port ([^ ]+).+", value=args.rpcport):
        raise Exception(f"[init_aria2c] couldnt run aria2c in port {args.rpcport} - {_proc}")
    
    logger.info(f"[init_aria2c] {_proc} - running on port: {args.rpcport}")
    
    return _proc



############################################################
#"""                     INIT                     """
############################################################


############################################################
#"""                     IP PROXY                     """
############################################################

from concurrent.futures import as_completed
from ipaddress import ip_address
import httpx

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
        _tavg = try_get(re.findall(r"= [^\/]+\/([^\/]+)\/", res), lambda x: float(x[0]))
        return {"ip": ip, "time": _tavg}
    
    @classmethod
    def get_ip(cls, key=None, timeout=1, api="ipify"):

        if api not in cls.URLS_API_GETMYIP:
            raise Exception("api not supported")

        _urlapi = cls.URLS_API_GETMYIP[api]['url']
        _keyapi = cls.URLS_API_GETMYIP[api]['key']


        try:
            
            _proxies = {'all://': f'http://127.0.0.1:{key}'} if key != None else None
            myip = try_get(httpx.get(_urlapi, timeout=httpx.Timeout(timeout=timeout), proxies=_proxies, follow_redirects=True), lambda x: x.json().get(_keyapi)) # type: ignore
            return myip
        except Exception as e:
            return repr(e)

    @classmethod
    def get_myiptryall(cls, key=None, timeout=1):

        def is_ipaddr(res):
            try:
                ip_address(res)
                return True
            except Exception as e:
                return False
        exe = ThreadPoolExecutor(thread_name_prefix="getmyip")
        futures = {exe.submit(cls.get_ip, key=key, timeout=timeout, api=api): api for api in cls.URLS_API_GETMYIP}
        for el in as_completed(futures):
            if not el.exception() and is_ipaddr(_res:=el.result()):
                exe.shutdown(wait=False, cancel_futures=True)
                return _res
            else: continue

    @classmethod
    def get_myip(cls, key=None, timeout=1):
        return cls.get_myiptryall(key=key, timeout=timeout)


def get_myip(key=None, timeout=2):
    return myIP.get_ip(key=key, timeout=timeout)


class TorGuardProxies:
    CONF_TORPROXIES_LIST_HTTPPORTS = [489, 23, 7070, 465, 993, 282, 778, 592]
    CONF_TORPROXIES_COUNTRIES = ["fn", "no", "bg", "pg", "it", "fr", "sp", "ire", "ice", "cz", "aus", "ger", "uk", "uk.man", "ro", "slk", "nl", "hg", "bul"]
    CONF_TORPROXIES_DOMAINS = [f"{cc}.secureconnect.me" for cc in CONF_TORPROXIES_COUNTRIES]
    CONF_TORPROXIES_NOK = Path(PATH_LOGS, 'bad_proxies.txt')
    
    @classmethod
    def test_proxies_rt(cls, routing_table, timeout=2):
        logger = logging.getLogger("torguardprx")
        logger.info(f"[init_proxies] starting test proxies")
        with ThreadPoolExecutor() as exe:
            futures = {exe.submit(get_myip, key=_key, timeout=timeout): _key for _key in list(routing_table.keys())}

        bad_pr = []

        for fut in futures:
            _ip = fut.result()       
            if _ip != routing_table[futures[fut]]:
                logger.info(f"[{futures[fut]}] test: {_ip} expect res: {routing_table[futures[fut]]}")
                bad_pr.append(routing_table[futures[fut]])

        return bad_pr

    @classmethod
    def test_proxies_raw(cls, list_ips, port=CONF_TORPROXIES_HTTPPORT, timeout=2):
        logger = logging.getLogger("torguardprx")
        cmd_gost = [
            f"gost -L=:{CONF_PROXIES_BASE_PORT + 2000 + i} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip}:{port}"
            for i, ip in enumerate(list_ips)
        ]
        routing_table = {
            CONF_PROXIES_BASE_PORT + 2000 + i: ip for i, ip in enumerate(list_ips)
        }
        proc_gost = []
        for cmd in cmd_gost:

            _proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            _proc.poll()
            if _proc.returncode:
                logger.error(f"[init_proxies] returncode[{_proc.returncode}] to cmd[{cmd}]")
                raise Exception("init proxies error")
            else:
                proc_gost.append(_proc)
            time.sleep(0.05)
        _res_ps = subprocess.run(["ps"], encoding="utf-8", capture_output=True).stdout
        logger.debug(f"[init_proxies] %no%\n\n{_res_ps}")
        _res_bad = cls.test_proxies_rt(routing_table, timeout=timeout)
        _line_ps_pr = []
        for _ip in _res_bad:
            if (_temp:=try_get(re.search(rf".+{_ip}\:\d+", _res_ps), lambda x: x.group() if x else None)):
                _line_ps_pr.append(_temp)    
        logger.info(f"[init_proxies] check in ps print equal number of bad ips: res_bad [{len(_res_bad)}] ps_print [{len(_line_ps_pr)}]")
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
    def init_proxies(cls, num=CONF_PROXIES_MAX_N_GR_HOST, size=CONF_PROXIES_N_GR_VIDEO, port=CONF_TORPROXIES_HTTPPORT)->Tuple[List, Dict]:

        logger = logging.getLogger("torguardprx")

        logger.info(f"[init_proxies] start")

        IPS_SSL = []
        
        for domain in cls.CONF_TORPROXIES_DOMAINS:
            IPS_SSL += cls.get_ips(domain)

        cached_res = cls.CONF_TORPROXIES_NOK
        if cached_res.exists() and (
            (datetime.now() - datetime.fromtimestamp(cached_res.stat().st_mtime)).seconds
            < 7200
        ):  # every 2h we check the proxies
            with open(cached_res, "r") as f:
                _content = f.read()
            _bad_ips = [_ip for _ip in _content.split("\n") if _ip]
        else:
            _bad_ips = cls.test_proxies_raw(IPS_SSL, port)

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
                    f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*i + j} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[j]}:{port}"
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
            f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*num + 99} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{_ip_main}:{port}"
        ]
        routing_table.update({CONF_PROXIES_BASE_PORT + 100 * num + 99: _ip_main})

        cmd_gost_group = [
            f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*i + 50} -F=:8899" for i in range(num)
        ]

        cmd_gost = cmd_gost_s + cmd_gost_group + cmd_gost_main

        logger.debug(f"[init_proxies] {cmd_gost}")
        logger.debug(f"[init_proxies] {routing_table}")

        proc_gost = []

        try:
            for cmd in cmd_gost:
                logger.debug(cmd)
                _proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
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
                    except Exception as e:
                        pass
            return [], {}
            

############################################################
#"""                     IP PROXY                     """
############################################################


############################################################
#"""                     YTDLP                           """
############################################################
from yt_dlp.extractor.commonwebdriver import (
    CONFIG_EXTRACTORS,
    SeleniumInfoExtractor,
    StatusStop,
    dec_on_exception,
    dec_retry_error,
    limiter_1,
    limiter_5,
    limiter_15,
    limiter_non,
    ReExtractInfo,
    ConnectError,
    StatusError503,
    my_dec_on_exception,
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
    unsmuggle_url,
)
from yt_dlp import YoutubeDL

def ies_close(ies):
    if not ies:
        return
    for ie, ins in ies.items():
        if close := getattr(ins, "close", None):
            try:
                close()                    
            except Exception as e:
                pass
                
class myYTDL(YoutubeDL):
    def __init__(self, *args, **kwargs):
        self.close: bool = kwargs.get("close", True)
        self.executor: ThreadPoolExecutor = kwargs.get("executor", ThreadPoolExecutor(thread_name_prefix="myYTDL"))
        super().__init__(*args, **kwargs) # type: ignore

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self.close:
            ies_close(self._ies_instances)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        ies_close(self._ies_instances)
    
    
    def shutdown(self):
        ies_close(self._ies_instances)


    def extract_info(self, *args, **kwargs)->Union[dict, None]:
        return super().extract_info(*args, **kwargs)

    def process_ie_result(self, *args, **kwargs)->dict:
        return super().process_ie_result(*args, **kwargs)
    
    def sanitize_info(self, *args, **kwargs)->dict:
        return YoutubeDL.sanitize_info(*args, **kwargs)  # type: ignore 
    
    async def async_extract_info(self, *args, **kwargs)->dict:
        return await sync_to_async(self.extract_info, executor=self.executor)(*args, **kwargs)  
        
    async def async_process_ie_result(self, *args, **kwargs)->dict:
        return await sync_to_async(self.process_ie_result, executor=self.executor)(*args, **kwargs)  

class ProxyYTDL(YoutubeDL):
    def __init__(self, **kwargs):
        opts = kwargs.get("opts", {})
        proxy = kwargs.get("proxy", None)
        quiet = kwargs.get("quiet", True)
        verbose = kwargs.get("verbose", False)
        verboseplus = kwargs.get("verboseplus", False)
        self.close = kwargs.get("close", True)
        self.executor = kwargs.get("executor", ThreadPoolExecutor(thread_name_prefix="proxyYTDL"))
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

        super().__init__(params=opts, auto_init="no_verbose_header") # type:  ignore

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self.close:
            ies_close(self._ies_instances)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        ies_close(self._ies_instances)

    
    def shutdown(self):
        ies_close(self._ies_instances)    
        
    def extract_info(self, *args, **kwargs)->Union[dict, None]:
        return super().extract_info(*args, **kwargs)

    def process_ie_result(self, *args, **kwargs)->dict:
        return super().process_ie_result(*args, **kwargs)
    
    def sanitize_info(self, *args, **kwargs)->dict:
        return YoutubeDL.sanitize_info(*args, **kwargs)  # type: ignore 
    
    async def async_extract_info(self, *args, **kwargs)->dict:
        return await sync_to_async(self.extract_info, executor=self.executor)(*args, **kwargs)  
        
    async def async_process_ie_result(self, *args, **kwargs)->dict:
        return await sync_to_async(self.process_ie_result, executor=self.executor)(*args, **kwargs)  

def get_extractor(url, ytdl):

    ies = ytdl._ies
    for ie_key, ie in ies.items():
        if ie.suitable(url) and (ie_key != "Generic"):
            return (ie_key, ie)
    return ("Generic", ies["Generic"])

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
        "logger": MyLogger(logger, quiet=args.quiet, verbose=args.verbose, superverbose=args.vv),
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
        "winit": args.winit,
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

    ytdl = myYTDL(params=ytdl_opts, auto_init="no_verbose_header")

    logger.debug(f"ytdl opts:\n{ytdl.params}")

    return ytdl

def get_format_id(info_dict, _formatid)->dict:

    if not info_dict: return {}
    
    if _req_fts := info_dict.get("requested_formats"):
        for _ft in _req_fts:
            if _ft["format_id"] == _formatid:
                return _ft
    elif _req_ft := info_dict.get("format_id"):
        if _req_ft == _formatid:
            return info_dict
    return {}


############################################################
#"""                     YTDLP                           """
############################################################


def print_tasks(tasks):
    return "\n".join(
        [f"{task.get_name()} : {repr(task.get_coro()).split(' ')[2]}" for task in tasks]
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
            _aria2cstr = f"aria2cDUMMY"
        mobj = re.findall(
            rf"(\d+)\s+(?:\?\?|{term})\s+((?:.+browsermob-proxy --port.+|{_aria2cstr}|geckodriver.+|java -Dapp.name=browsermob-proxy.+|/Applications/Firefox.app/Contents/MacOS/firefox-bin.+))",
            res,
        )
        mobj2 = re.findall(
            rf"\d+\s+(?:\?\?|{term})\s+/Applications/Firefox.app/Contents/MacOS/firefox-bin.+--profile (/var/folders/[^\ ]+) ",
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
        # _log(f"[kill_processes_proxy]\n{mobj3}")
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
        return f"{(base * abs_bytes / unit):{format_}}{s}" # type: ignore
    return f"{(base*abs_bytes/unit):{format_}} {s}" # type: ignore

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

    poolmanager.pool_classes_by_scheme["http"] = MyHTTPConnectionPool # type: ignore

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

    poolmanager.pool_classes_by_scheme["https"] = MyHTTPSConnectionPool # type: ignore


def check_if_dl(info_dict, videos):

    if not (_id := info_dict.get("id")) or not (_title := info_dict.get("title")):
        return False

    _title = sanitize_filename(_title, restricted=True).upper()
    vid_name = f"{_id}_{_title}"

    return videos.get(vid_name)


############################################################
#"""                     PYSIMPLEGUI                     """
############################################################

import PySimpleGUI as sg

def init_gui_root():

    logger = logging.getLogger("init_gui_root")

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

def init_gui_console():

    
    logger = logging.getLogger("init_gui_cons")
    


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
                    default=False,
                    enable_events=True,
                ),
                sg.Checkbox(
                    "ResRep",
                    key="-RESETREP-",
                    default=False,
                    enable_events=True,
                ),
                sg.Checkbox(
                    "WkInit", key="-WKINIT-", default=True, enable_events=True
                ),
                sg.Button("+PasRes"),
                sg.Button("-PasRes"),
                sg.Button("DLStatus", key="-DL-STATUS"),
                sg.Button("Info"),
                sg.Button("ToFile"),
                sg.Button("+runwk", key="IncWorkerRun"),
                sg.Button("-runwk", key="DecWorkerRun"),
                sg.Button("#vidwk", key="NumVideoWorkers"),
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


############################################################
#"""                     PYSIMPLEGUI                     """
############################################################


def wait_for_change_ip(logger):

    _old_ip = get_myip(timeout=5)
    logger.info(f"old ip: {_old_ip}")
    _proc_kill = subprocess.run(["pkill", "TorGuardDesktopQt"])
    if _proc_kill.returncode:
        logger.error(f"error when closing TorGuard {_proc_kill}")
    else:
        n = 0
        _new_ip = None
        while n < 5:
            time.sleep(2)

            _new_ip = get_myip(timeout=5)
            logger.info(f"[{n}] {_new_ip}")
            if _new_ip and (_old_ip != _new_ip):
                break
            else:
                n += 1
                _new_ip = None
        _old_ip = _new_ip
        logger.info(f"new old ip: {_old_ip}")
        _proc_open = subprocess.run(["open", "/Applications/Torguard.app"])
        time.sleep(5)
        n = 0
        _new_ip = None
        while n < 5:            
            logger.info("try to get ip")
            _new_ip = get_myip(timeout=5)
            logger.info(f"[{n}] {_new_ip}")
            if _new_ip and (_old_ip != _new_ip):
                return True
            else:
                n += 1
                _new_ip = None
                time.sleep(2)
                
def cmd_extract_info(url, proxy=None, pl=False, upt=False):
    if pl:
        opt = "-J"
    else:
        opt = "-j"
    if proxy:
        pr = f"--proxy {proxy} "
    else:
        pr = ""
    cmd = f"yt-dlp {opt} {pr}{url}"
    print(cmd)
    if not upt:
        res = subprocess.run(cmd.split(" "), encoding="utf-8", capture_output=True)
        if res.returncode != 0:
            raise Exception(res.stderr)
        else:
            return json.loads(res.stdout.splitlines()[0])

async def async_cmd_extract_info(url, proxy=None, pl=False, logger=None):
    if pl:
        opt = "-J"
    else:
        opt = "-j"
    if proxy:
        pr = f"--proxy {proxy} "
    else:
        pr = ""
    cmd = f"yt-dlp -v {opt} {pr}{url}"
    if not logger:
        logger = logging.getLogger("test")
    logger.info(cmd)

    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

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

