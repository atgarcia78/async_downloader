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
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from itertools import zip_longest
from pathlib import Path
from queue import Queue
from bisect import bisect

PATH_LOGS = Path(Path.home(), "Projects/common/logs")

CONF_DASH_SPEED_PER_WORKER = 102400

 #1048576 #512000 #1048576 #4194304
CONF_HLS_SPEED_PER_WORKER = 102400/2#512000
CONF_PROXIES_MAX_N_GR_HOST = 10
CONF_PROXIES_N_GR_VIDEO = 8
CONF_PROXIES_BASE_PORT = 12000
CONF_ARIA2C_MIN_SIZE_SPLIT = 1048576 #1MB 10485760 #10MB
CONF_ARIA2C_SPEED_PER_CONNECTION = 102400 # 102400 * 1.5# 102400
CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED = 120
CONF_ARIA2C_N_CHUNKS_CHECK_SPEED = 60
CONF_ARIA2C_TIMEOUT_INIT = 20
CONF_INTERVAL_GUI = 0.2

CONF_ARIA2C_EXTR_GROUP = ['tubeload', 'redload', 'highload', 'embedo']


def wait_for_change_ip(logger):
    _old_ip = json.loads(subprocess.run(f"curl -s https://httpbin.org/get".split(' '), encoding='utf-8', capture_output=True).stdout).get('origin')
    logger.info(f"old ip: {_old_ip}")
    _proc_kill = subprocess.run(['pkill', 'TorGuardDesktopQt'])
    if _proc_kill.returncode:
        logger.error(f"error when closing TorGuard {_proc_kill}")
    else:
        n = 0
        while n < 5:
            time.sleep(2)
            _new_ip = json.loads(subprocess.run(f"curl -s https://httpbin.org/get".split(' '), encoding='utf-8', capture_output=True).stdout).get('origin')
            logger.info(f"[{n}] {_new_ip}")
            if _old_ip != _new_ip: break
            else: n += 1
        _old_ip = _new_ip
        logger.info(f"new old ip: {_old_ip}")
        _proc_open = subprocess.run(['open', '/Applications/Torguard.app'])
        time.sleep(5)
        n = 0
        while n < 5:
            logger.info("try to get ip")
            _proc_ip = subprocess.run(f"curl -s https://httpbin.org/get".split(' '), encoding='utf-8', capture_output=True)
            _new_ip = json.loads(_proc_ip.stdout).get('origin')
            logger.info(f"[{n}] {_new_ip}")
            if _old_ip != _new_ip: return True
            else:
                n += 1
                time.sleep(2)


def cmd_extract_info(url, proxy=None, pl=False, upt=False):
    if pl: opt = "-J"
    else: opt = "-j"
    if proxy: pr = f"--proxy {proxy} "
    else: pr = ""
    cmd = f"yt-dlp {opt} {pr}{url}"
    print(cmd)
    if not upt:
        res = subprocess.run(cmd.split(" "), encoding='utf-8', capture_output=True)
        if res.returncode != 0:
            raise Exception(res.stderr)
        else: return(json.loads(res.stdout.splitlines()[0]))

async def async_cmd_extract_info(url, proxy=None, pl=False, logger=None):
    if pl: opt = "-J"
    else: opt = "-j"
    if proxy: pr = f"--proxy {proxy} "
    else: pr = ""
    cmd = f"yt-dlp -v {opt} {pr}{url}"
    if not logger:
        logger = logging.getLogger("test")
    logger.info(cmd)

    proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            
    async def read_stream(stream):
        
        try:
            
            _buffer = ""
            while not proc.returncode:
                try:                        
                    line = await stream.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    continue

                if line: 
                    _line = line.decode('utf-8')                    
                    logger.info(_line)
                    _buffer += _line

                else:
                    break

        except Exception as e:
            logger.exception(repr(e))
            
    await asyncio.gather(read_stream(proc.stderr), proc.wait())

    _info = await proc.stdout.read()
    return json.loads(_info)


class MySem(asyncio.Semaphore):
    
    def __init__(self, *args, **kwargs):
        
        self.dl = kwargs.pop('dl')
        super().__init__(*args, **kwargs)

    async def __aenter__(self):
        
        if self._value <= 0:
            logger.debug(f"{self.dl.premsg} waiting for SEM")
            await self.acquire()
            logger.debug(f"{self.dl.premsg} entry SEM")

        else:
            await self.acquire()

        return None

    def reset(self, n):
        for i in range(len(self._waiters)):
            self._wake_up_next()
        self._value = n

class MyEvent(asyncio.Event):

    def set(self, cause="noinfo"):
        
        super().set()
        self._cause = cause

    def is_set(self):
        """Return True if and only if the internal flag is true."""
        if self._value: return self._cause
        else: return self._value




class ProgressTimer:
    TIMER_FUNC = time.monotonic

    def __init__(self):
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
    UPDATE_TIMESPAN_S = 1.0
    AVERAGE_TIMESPAN_S = 5.0

    def __init__(self, initial_bytes=0):
        self.ts_data = [(self.TIMER_FUNC(), initial_bytes)]
        self.timer = ProgressTimer()
        self.last_value = None

    def __call__(self, byte_counter):
        time_now = self.TIMER_FUNC()

        # only append data older than 50ms
        if time_now - self.ts_data[-1][0] > 0.05:
            self.ts_data.append((time_now, byte_counter))

        # remove older entries
        idx = max(0, bisect(self.ts_data, (time_now - self.AVERAGE_TIMESPAN_S, )) - 1)
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
                deviation = float('inf')
            else:
                deviation = max(predicted, value) / min(predicted, value)

            if deviation < 1.25:
                return predicted

        self.last_value = time_now + value
        return value




try:
    import proxy
    _SUPPORT_PROXY = True
except Exception:
    _SUPPORT_PROXY = False


try:
    import aria2p
    from aria2p.utils import human_readable_timedelta
    _SUPPORT_ARIA2P = True
except Exception:
     _SUPPORT_ARIA2P = False

try:
    import httpx
    _SUPPORT_HTTPX = True
except Exception:
    _SUPPORT_HTTPX = False

try:
    import PySimpleGUI as sg
    _SUPPORT_PYSIMP = True
except Exception:
    _SUPPORT_PYSIMP = False

try:
    from yt_dlp import YoutubeDL
    from yt_dlp.extractor.commonwebdriver import (CONFIG_EXTRACTORS,
                                                  SeleniumInfoExtractor,
                                                  StatusStop, dec_on_exception,
                                                  dec_retry_error, limiter_1,
                                                  limiter_5, limiter_15,
                                                  limiter_non, ReExtractInfo, ConnectError,
                                                  StatusError503)
    from yt_dlp.mylogger import MyLogger
    from yt_dlp.utils import (get_domain, js_to_json, prepend_extension,
                              sanitize_filename, smuggle_url, traverse_obj,
                              try_get, unsmuggle_url)
    
    _SUPPORT_YTDL = True
except Exception:
    _SUPPORT_YTDL = False


try:    
    from filelock import FileLock
    _SUPPORT_FILELOCK = True
except Exception:
    _SUPPORT_FILELOCK = False


def _for_print_entry(entry):
    if not entry: return
    _entry = copy.deepcopy(entry)

    if (_formats:=_entry.get('formats')):

        _new_formats = []
        for _format in _formats:
            if len(_formats) > 5:
                _id, _prot = _format['format_id'],  _format['protocol']
                _format = {'format_id':_id, ...:..., 'protocol': _prot}

            else:
                if (_frag:=_format.get('fragments')):
                    _format['fragments'] = [_frag[0], ..., _frag[-1]]
            _new_formats.append(_format)


        _entry['formats'] = _new_formats

    if (_formats:=_entry.get('requested_formats')):

        _new_formats = []
        for _format in _formats:
            if (_frag:=_format.get('fragments')):
                _format['fragments'] = [_frag[0], ..., _frag[-1]]
            _new_formats.append(_format)

        _entry['requested_formats'] = _new_formats

    if (_frag:=_entry.get('fragments')):

        _entry['fragments'] = [_frag[0], ..., _frag[-1]]

    return _entry

def _for_print(info):
    if not info: return
    _info = copy.deepcopy(info)
    if (_entries:=_info.get('entries')):
        _info['entries'] = [_for_print_entry(_el) for _el in _entries]
        return _info
    else: 
        return _for_print_entry(_info)

def _for_print_videos(videos):
    if not videos: return  
    _videos = copy.deepcopy(videos)
    
    if isinstance(videos, dict):
        
        for _, _values in _videos.items():
            if (_info:=traverse_obj(_values, 'video_info')):
                _values['video_info'] = _for_print(_info)
                
        return _videos
    
    elif isinstance(videos, list):
        _videos = [_for_print(_vid) for _vid in _videos]
        return _videos

def sync_to_async(func, executor=None):
    @functools.wraps(func)
    async def run_in_executor(*args, **kwargs):
        loop = asyncio.get_event_loop()
        pfunc = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run_in_executor


async def async_ex_in_executor(executor, func, /, *args, **kwargs):
    loop = kwargs.get('loop', asyncio.get_running_loop())
    ctx = contextvars.copy_context()
    _kwargs = {k:v for k,v in kwargs.items() if k != 'loop'}
    func_call = functools.partial(ctx.run, func, *args, **_kwargs)        
    return await loop.run_in_executor(executor, func_call)
    

from multiprocess import Process as MPProcess
from multiprocess import Queue as MPQueue


def long_operation_in_process(func):            
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        queue = MPQueue()
        kwargs['queue'] = queue
        proc = MPProcess(target=func, args=args, kwargs=kwargs)
        proc.start()
        return(proc, queue)
        # try:
        #     res = queue.get(timeout=60)
        #     proc.join()
        #     proc.close()
        #     return res
        # except BaseException as e:
        #     logging.getLogger('op_in_proc').exception(repr(r))
    return wrapper 

def long_operation_in_thread(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        stop_event = threading.Event()
        kwargs['stop_event'] = stop_event          
        thread = threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True)
        thread.start()
        return stop_event
    return wrapper 

@contextlib.asynccontextmanager
async def async_lock(executor, lock):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, lock.acquire)
    try:
        yield  # the lock is held
    finally:
        lock.release()

async def async_wait_time(n, events=None):   
    
    if not events: events = [asyncio.Event()] #dummy
    _started = time.monotonic()
    while not any([_ev.is_set() for _ev in events]):
        if (_t:=(time.monotonic() - _started)) >= n:
            return _t
        else:
            await asyncio.sleep(0)
    return 
            
def wait_time(n, event=None):
    _started = time.monotonic()
    if not event: 
        event = threading.Event() #dummy  
    
    while not event.is_set():
        if (_t:=(time.monotonic() - _started)) >= n:
            return _t
        else:
            time.sleep(CONF_INTERVAL_GUI)
    return

async def async_wait_until(timeout, cor=None, args=(None,), kwargs={}, interv=CONF_INTERVAL_GUI):
    _started = time.monotonic()
     
    if not cor: 
        async def _cor(*args, **kwargs):
            return True
    else: _cor = cor

    while not (await _cor(*args, **kwargs)):
        if (_t:=(time.monotonic() - _started)) >= timeout:
            raise TimeoutError()
        else:
            await async_wait_time(interv)
    

def wait_until(timeout, statement=None, args=(None,), kwargs={}, interv=CONF_INTERVAL_GUI):
     _started = time.monotonic()
     
     if not statement: 
        def func(*args, **kwargs):
            return True
     else: func = statement

     while not func(*args, **kwargs):
        if (_t:=(time.monotonic() - _started)) >= timeout:
            raise TimeoutError()
        else:
            time.sleep(interv)
    

class SignalHandler:
    
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print(signum)
        print("Exiting gracefully")
        self.KEEP_PROCESSING = False
 
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
        return self.last / (1 - beta ** self.calls) if self.calls else self.last

class OutputLogger:
    def __init__(self, name="root", level="INFO"):
        self.logger = logging.getLogger(name)
        self.name = self.logger.name
        self.level = getattr(logging, level)
        self._redirector = contextlib.redirect_stdout(self)

    def write(self, msg):
        if msg and not msg.isspace():
            self.logger.log(self.level, msg)

    def flush(self): pass

    def __enter__(self):
        self._redirector.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # let contextlib do any exception handling here
        self._redirector.__exit__(exc_type, exc_value, traceback)

def init_logging(file_path=None):


    PATH_LOGS = Path(Path.home(), "Projects/common/logs")
    if not file_path:
        config_file = Path(Path.home(), "Projects/common/logging.json")
    else:
        config_file = Path(file_path)
    
    with open(config_file) as f:
        config = json.loads(f.read())
    
    config['handlers']['info_file_handler']['filename'] = config['handlers']['info_file_handler']['filename'].format(path_logs = str(PATH_LOGS))
    
    logging.config.dictConfig(config)
    
    for log_name, log_obj in logging.Logger.manager.loggerDict.items():
        if log_name.startswith('proxy'):
            logger = logging.getLogger(log_name)
            logger.setLevel(logging.INFO)
    
    logger = logging.getLogger('proxy.http.proxy.server')
    logger.setLevel(logging.WARNING)
            
def rclone_init_args():
    
    parser = argparse.ArgumentParser(description="wrapper reclone")
    parser.add_argument("--orig", help="orig dirs", action="append", dest="origfolders", default=[])
    parser.add_argument("--dest", help="dest dir", default="", type=str)
    parser.add_argument("--transfers", default=4, type=int)
    parser.add_argument("--direct", action="store_true", default=False)
    
    args = parser.parse_args()
    
    return args
    
def init_argparser():
    
    UA_LIST = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:107.0) Gecko/20100101 Firefox/107.0"]

    parser = argparse.ArgumentParser(description="Async downloader videos / playlist videos HLS / HTTP")
    parser.add_argument("-w", help="Number of DL workers", default="5", type=int)
    parser.add_argument("--winit", help="Number of init workers, default is same number for DL workers", default="0", type=int)
    parser.add_argument("-p", "--parts", help="Number of workers for each DL", default="16", type=int)
    parser.add_argument("--format", help="Format preferred of the video in youtube-dl format", default="bv*+ba/b", type=str)
    parser.add_argument("--sort", help="Formats sort preferred", default="ext:mp4:mp4a", type=str)
    parser.add_argument("--index", help="index of a video in a playlist", default=None, type=int)
    parser.add_argument("--file", help="jsonfiles", action="append", dest="collection_files", default=[])
    parser.add_argument("--checkcert", help="checkcertificate", action="store_true", default=False)
    parser.add_argument("--ytdlopts", help="init dict de conf", default="", type=str)
    parser.add_argument("--proxy", default=None, type=str)
    parser.add_argument("--useragent", default=UA_LIST[0], type=str)
    parser.add_argument("--first", default=None, type=int)
    parser.add_argument("--last", default=None, type=int)
    parser.add_argument("--nodl", help="not download", action="store_true", default=False)   
    parser.add_argument("--headers", default="", type=str)  
    parser.add_argument("-u", action="append", dest="collection", default=[])   
    parser.add_argument("--nodlcaching", help="dont get new cache videos dl, use previous", action="store_true", default=False)
    parser.add_argument("--dlcaching", help="force to check external storage", action="store_true", default=False)
    parser.add_argument("--path", default=None, type=str)    
    parser.add_argument("--caplinks", action="store_true", default=False)    
    parser.add_argument("-v", "--verbose", help="verbose", action="store_true", default=False)
    parser.add_argument("--vv", help="verbose plus", action="store_true", default=False)
    parser.add_argument("-q", "--quiet", help="quiet", action="store_true", default=False)
    parser.add_argument("--aria2c", help="use of external aria2c running in port [PORT]. By default PORT=6800. PORT 0 to disable", default=-1, type=int)
    parser.add_argument("--nosymlinks", action="store_true", default=False)
    parser.add_argument("--use-http-failover", action="store_true", default=False)
    parser.add_argument("--use-path-pl", action="store_true", default=False)
    parser.add_argument("--use-cookies",action="store_true", default=False)
    parser.add_argument("--no-embed",action="store_true", default=False)
 

    args = parser.parse_args()
    
    if args.winit == 0:
        args.winit = args.w
    
    if args.aria2c == -1:
        args.aria2c = True
        args.rpcport = 6800
    elif args.aria2c == 0:
        args.rpcport = None
        args.aria2c = False
    else: 
        args.rpcport = args.aria2c
        args.aria2c = True        

    if args.path and len(args.path.split("/")) == 1:
        _path = Path(Path.home(), "testing", args.path)
        args.path = str(_path)
        
    if args.vv:
        args.verbose = True
        
    if args.proxy == 'no':
        args.proxy = 0
        
    if args.dlcaching:
        args.nodlcaching = False
    else:
        args.nodlcaching = True

    if args.checkcert:
        args.nocheckcert = False
    else: args.nocheckcert = True
            
    return args

if _SUPPORT_PROXY:
    
    @long_operation_in_thread
    def run_proxy_http(*args, **kwargs):
        #with proxy.Proxy(['--log-level', log_level, '--plugins', 'proxy.plugin.cache.CacheResponsesPlugin', '--plugins', 'proxy.plugin.ProxyPoolByHostPlugin']) as p:
        log_level = kwargs.get("log_level", "INFO")
        stop_event = kwargs.get("stop_event")
        with proxy.Proxy(['--log-level', log_level, '--plugins', 'proxy.plugin.ProxyPoolByHostPlugin']) as p:
            try:
                logger = logging.getLogger("proxy")
                logger.debug(p.flags)
                while not stop_event.is_set():
                    time.sleep(CONF_INTERVAL_GUI)
            except BaseException:
                logger.error("context manager proxy")    
            
if _SUPPORT_ARIA2P:

    
    def init_aria2c(args):
        
        logger = logging.getLogger("asyncDL")
        res = subprocess.run(["ps", "-u", "501", "-x", "-o" , "pid,tty,command"], encoding='utf-8', capture_output=True).stdout
        mobj = re.findall(r"aria2c.+--rpc-listen-port ([^ ]+).+", res)
        if mobj:
            if str(args.rpcport) in mobj:
                mobj.sort()
                args.rpcport = int(mobj[-1]) + 100
                
        #subprocess.run(["aria2c","--rpc-listen-port",f"{args.rpcport}", "--enable-rpc","--daemon"])
        _proc = subprocess.Popen(f"aria2c --rpc-listen-port {args.rpcport} --enable-rpc", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        sem = True
        while sem:
            _proc.poll()
            for line in _proc.stdout:
                _line = line.decode('utf-8').strip()
                if not _line: break
                else:                   
                    sem = False
                    break
               
        logger.info(f"aria2c running on port: {args.rpcport} ")
       
        cl = aria2p.API(aria2p.Client(port=args.rpcport))
        opts = cl.get_global_options()
        logger.debug(f"aria2c options:\n{opts._struct}")
        del opts
        del cl

        return _proc

        # _aria2cstr = f"aria2c.+--rpc-listen-port {args.rpcport}.+"
        # res = subprocess.run(["ps", "-u", "501", "-x", "-o" , "pid,command"], encoding='utf-8', capture_output=True).stdout
        # mobj = re.findall(rf'(\d+)\s+({_aria2cstr})', res)
        # return(mobj[0][0])
        
def grouper(iterable, n, *, incomplete='fill', fillvalue=None):
    args = [iter(iterable)] * n
    if incomplete == 'fill':
        return zip_longest(*args, fillvalue=fillvalue)
    if incomplete == 'strict':
        return zip(*args, strict=True)
    if incomplete == 'ignore':
        return zip(*args)
    else:
        raise ValueError('Expected fill, strict, or ignore')      
          

def get_myip(key=None):
    _proxy = ""
    if key:
        _proxy = f"-x http://127.0.0.1:{key} "
    proc_curl = subprocess.Popen(f"curl {_proxy}-s https://api.ipify.org?format=json".split(' '), stdout=subprocess.PIPE,)
    return(subprocess.run(['jq', '-r', '.ip'], stdin=proc_curl.stdout, encoding='utf-8', capture_output=True).stdout.strip())

def test_proxies_rt(routing_table):
    logger = logging.getLogger("asyncdl")
        
    logger.info(f"[init_proxies] starting test proxies")
    
    with ThreadPoolExecutor() as exe:
        futures = {exe.submit(get_myip, _key): _key for _key in list(routing_table.keys())}
    
    bad_pr = []

    for fut in futures:
        _ip = fut.result()
        #logger.debug(f"[{futures[fut]} test: {_ip} expect res: {routing_table[futures[fut]]} > {_ip == routing_table[futures[fut]]}")
        if _ip != routing_table[futures[fut]]: 
            logger.info(f"[{futures[fut]}] test: {_ip} expect res: {routing_table[futures[fut]]}")
            bad_pr.append(routing_table[futures[fut]])
                                
    return(bad_pr)
     
def test_proxies_raw(list_ips, port=7070):
    logger = logging.getLogger("asyncdl")
    cmd_gost = [f"gost -L=:{CONF_PROXIES_BASE_PORT + 2000 + i} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip}:{port}" for i, ip in enumerate(list_ips)]
    routing_table = {CONF_PROXIES_BASE_PORT + 2000 + i: ip for i, ip in enumerate(list_ips)} 
    proc_gost = []
    for cmd in cmd_gost:
            
        #logger.info(cmd)
        _proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        _proc.poll()
        if _proc.returncode:
            logger.error(f"[init_proxies] returncode[{_proc.returncode}] to cmd[{cmd}]")
            raise Exception("init proxies error")
        else: proc_gost.append(_proc)
        time.sleep(0.05)
    _res_ps = subprocess.run(['ps'], encoding='utf-8', capture_output=True).stdout
    logger.debug(f"[init_proxies] %no%\n\n{_res_ps}")    
    _res_bad = test_proxies_rt(routing_table)
    for _ip in _res_bad:
        _line_ps_pr = re.search(rf'.+{_ip}\:\d+', _res_ps).group()
        logger.info(f"[init_proxies] check in ps print: {_line_ps_pr}")        
    for proc in proc_gost:
        proc.kill()
     
    cached_res = Path(Path.home(), "Projects/common/logs/bad_proxies.txt")
    with open(cached_res, "w") as f:
        _test = '\n'.join(_res_bad)
        f.write(_test)
           
    return _res_bad
        
def get_ips(name):
    res = subprocess.run(f"dscacheutil -q host -a name {name}".split(' '), encoding='utf-8', capture_output=True).stdout
    return(re.findall(r"ip_address: (.+)", res))    
           

def init_proxies(num, size, port=7070):
    
    logger = logging.getLogger("asyncDL")  
    
    subprocess.run(["flush-dns"])
    
    IPS_SSL = []
    #NL
    IPS_SSL += get_ips('nl.secureconnect.me')
    #SWE
    IPS_SSL += get_ips('swe.secureconnect.me')
    #NO    
    #IPS_SSL += get_ips('no.secureconnect.me')
    #BG
    IPS_SSL += get_ips('bg.secureconnect.me')
    #PG
    #IPS_SSL += get_ips('pg.secureconnect.me')
    #IT
    #IPS_SSL += get_ips('it.secureconnect.me')
    #FR
    IPS_SSL += get_ips('fr.secureconnect.me') 
    #UK
    IPS_SSL += get_ips('uk.secureconnect.me') + get_ips('uk.man.secureconnect.me')
    #DE
    #IPS_SSL += get_ips('ger.secureconnect.me')
    
    cached_res = Path(Path.home(), "Projects/common/logs/bad_proxies.txt")
    if cached_res.exists() and ((datetime.now() - datetime.fromtimestamp(cached_res.stat().st_mtime)).seconds < 7200): #every 2h we check the proxies
        with open(cached_res, "r") as f:
            _content = f.read()
        _bad_ips = [_ip for _ip in _content.split('\n') if _ip]
    else:
        _bad_ips = test_proxies_raw(IPS_SSL,port)

    for _ip in _bad_ips:
        if _ip in IPS_SSL: IPS_SSL.remove(_ip)
    
    _ip_main = random.choice(IPS_SSL)
    
    IPS_SSL.remove(_ip_main)

    _ips = random.sample(IPS_SSL, num * (size + 1))
    
    FINAL_IPS = list(grouper(_ips, (size + 1)))
    
    cmd_gost_s = []
    
    routing_table = {}
    
    for j in range(size + 1):
            
        cmd_gost_s.extend([f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*i + j} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[j]}:{port}" for i, ip in enumerate(FINAL_IPS)])
        
        routing_table.update({(CONF_PROXIES_BASE_PORT + 100*i + j):ip[j] for i, ip in enumerate(FINAL_IPS)})
    
    cmd_gost_main = [f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*num + 99} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{_ip_main}:7070"]    
    routing_table.update({CONF_PROXIES_BASE_PORT + 100*num + 99:_ip_main})
    

    cmd_gost_group =  [f"gost -L=:{CONF_PROXIES_BASE_PORT + 100*i + 50} -F=:8899" for i in range(num)] 

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
                logger.error(f"[init_proxies] returncode[{_proc.returncode}] to cmd[{cmd}]")
                raise Exception("init proxies error")
            else: proc_gost.append(_proc)
            time.sleep(0.05)
            
        logger.info("[init_proxies] done")
        return proc_gost, routing_table
    except Exception:
        if proc_gost:
            for proc in proc_gost:
                try:
                    proc.kill()
                except Exception as e:
                    pass
        raise
        
        
if _SUPPORT_YTDL:

    class ProxyYTDL(YoutubeDL):
        
        def __init__(self, **kwargs):
            opts=kwargs.get('opts', {})
            proxy=kwargs.get('proxy', None)
            quiet=kwargs.get('quiet', True)
            verbose=kwargs.get('verbose', False)            
            verboseplus=kwargs.get('verboseplus', False)
            self.close=kwargs.get('close', True)
            self.executor=kwargs.get('executor', ThreadPoolExecutor())
            opts['quiet'] = quiet
            opts['verbose'] = verbose
            opts['verboseplus'] = verboseplus
            opts['logger'] = MyLogger(logging.getLogger("async-ytdl"),
                                             quiet=opts['quiet'], verbose=opts['verbose'], superverbose=opts['verboseplus'])
            opts['proxy'] = proxy
            
            super().__init__(params=opts, auto_init='no_verbose_header')
        
        def __enter__(self):
            return self

        def __exit__(self, *args, **kwargs):
            if self.close:
                ies = self._ies_instances            
                if not ies: return                    
                for ie, ins in ies.items():                
                    if (close:=getattr(ins, 'close', None)):
                        try:
                            close()                                        
                        except Exception as e:
                            pass

        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args, **kwargs):
            ies = self._ies_instances            
            if not ies: return                    
            for ie, ins in ies.items():                
                if (close:=getattr(ins, 'close', None)):
                    try:
                        close()                                        
                    except Exception as e:
                        pass
        
        def extract_info(self, url):
            return super().extract_info(url, download=False)
        
        async def async_extract_info(self, url):
            return await async_ex_in_executor(self.executor, self.extract_info, url)
            
    def get_extractor(url, ytdl):
        
        ies = ytdl._ies   
        for ie_key, ie in ies.items():
            if ie.suitable(url) and (ie_key != 'Generic'):
                return (ie_key, ie)                
        return('Generic', ies['Generic'])
        
    def is_playlist_extractor(url, ytdl):    
            
        ie_key, ie = get_extractor(url, ytdl)
        
        if ie_key == 'Generic':
            return(True, 'Generic')   
            
        ie_name = _iename.lower() if type(_iename:=getattr(ie, 'IE_NAME', '')) is str else ""
        
        ie_tests = str(getattr(ie, '_TESTS', ''))
        
        _is_pl = any("playlist" in _ for _ in [ie_key.lower(), ie_name, ie_tests])
        
        return(_is_pl, ie_key)

    def init_ytdl(args):

        logger = logging.getLogger("yt_dlp")
        
        proxy = None
        if args.proxy and args.proxy != 0:     
            sch = args.proxy.split("://")
            if len(sch) == 2:
                if sch[0] != 'http':
                    logger.error("Proxy is not valid, should be http")
                else: proxy = args.proxy
            else:
                proxy = f"http://{args.proxy}"
                                
        headers = {
            "User-Agent": args.useragent, 
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Connection": "keep-alive",
            "Accept-Language": "en,es-ES;q=0.5",
            "Accept-Encoding": "gzip, deflate"
        }
        
        ytdl_opts = { 
            "retries": 1,
            "extractor_retries": 1,
            "http_headers": headers,
            "proxy" : proxy,        
            "logger" : MyLogger(logger, quiet=args.quiet, 
                                verbose=args.verbose, superverbose=args.vv),
            "verbose": args.verbose,
            "quiet": args.quiet,
            "format" : args.format,
            "format_sort" : [args.sort],
            "nocheckcertificate" : args.nocheckcert,
            "subtitleslangs": ['all'],
            "convertsubtitles": 'srt',
            "continuedl": True,
            "updatetime": False,
            "ignoreerrors": False, 
            "no_abort_on_errors": False,        
            "extract_flat": "in_playlist",        
            "no_color" : True,
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
            "embed": not args.no_embed                    
        }
        
        if args.use_cookies:
            ytdl_opts.update(
                {
                    "cookiesfrombrowser": ('firefox', '/Users/antoniotorres/Library/Application Support/Firefox/Profiles/b33yk6rw.selenium', None)
                })
        
        if args.ytdlopts: 
            ytdl_opts.update(json.loads(js_to_json(args.ytdlopts)))
            
        ytdl = YoutubeDL(ytdl_opts, auto_init="no_verbose_header")
        
        logger.debug(f"ytdl opts:\n{ytdl.params}")   
        
        return ytdl

    def get_format_id(info_dict, _formatid):
                
        if (_req_fts:=info_dict.get('requested_formats')):
            for _ft in _req_fts:
                if _ft['format_id'] == _formatid:
                    return _ft
        elif (_req_ft:=info_dict.get('format_id')):
            if _req_ft == _formatid:
                return info_dict

    def get_files_same_id():
    
        logger = logging.getLogger('check')
        
        config_folders = {'local': Path(Path.home(), "testing"), 'pandaext4': Path("/Volumes/Pandaext4/videos"), 'datostoni': Path("/Volumes/DatosToni/videos"), 'wd1b': Path("/Volumes/WD1B/videos"), 'wd5': Path("/Volumes/WD5/videos"), 'wd8_1': Path("/Volumes/WD8_1/videos")}

        list_folders = []

        for _vol,_folder in config_folders.items():
            if not _folder.exists():
                logger("failed {_folder}, let get previous info saved in previous files")
                
            else: list_folders.append(_folder)

        files_cached = []
        for folder in list_folders:

            logger.info('>>>>>>>>>>>STARTS ' + str(folder))


            try:

                files = [file for file in folder.rglob('*')
                        if file.is_file() and not file.is_symlink() and not 'videos/_videos/' in str(file) and not file.stem.startswith('.') and (file.suffix.lower() in ('.mp4', '.mkv', '.ts', '.zip'))]

            except Exception as e:
                logger.info(f"[get_files_cached][{folder}] {repr(e)}")


            for file in files:

                _res = file.stem.split('_', 1)
                if len(_res) == 2:
                    _id = _res[0]

                else:
                    _id = sanitize_filename(file.stem, restricted=True).upper()

                files_cached.append((_id, str(file)))

        _res_dict  = {}
        for el in files_cached:
            for item in files_cached:
                if (el != item) and (item[0] == el[0]):
                    if not _res_dict.get(el[0]): _res_dict[el[0]] = set([el[1], item[1]])
                    else: _res_dict[el[0]].update([el[1], item[1]])
        _ord_res_dict = sorted(_res_dict.items(), key=lambda x: len(x[1]))
        return _ord_res_dict

if _SUPPORT_FILELOCK:
    
    class LocalStorage:           
            
        lslogger = logging.getLogger('LocalStorage')
        lock = FileLock(Path(PATH_LOGS, "files_cached.json.lock"))
        local_storage = Path(PATH_LOGS, "files_cached.json")
        prev_local_storage = Path(PATH_LOGS, "prev_files_cached.json")
        
        config_folders = {"local": Path(Path.home(), "testing"), "pandaext4": Path("/Volumes/Pandaext4/videos"), 
                        "datostoni": Path("/Volumes/DatosToni/videos"), "wd1b": Path("/Volumes/WD1B/videos"),
                        "wd5": Path("/Volumes/WD5/videos"), "wd8_1": Path("/Volumes/WD8_1/videos"), "wd8_2": Path("/Volumes/WD8_2/videos")}
        
        def __init__(self, paths=None):
            
            self._data_from_file = {} #data struct per vol
            self._data_for_scan = {} #data ready for scan
            self._last_time_sync = {}
            
            if paths:
                if not isinstance(paths, list):
                    paths = [paths]
                
                LocalStorage.config_folders.extend(paths)        
            
        
        @lock
        def load_info(self):
            
            with open(LocalStorage.local_storage,"r") as f:
                self._data_from_file = json.load(f)     
                    
            for _key,_data in self._data_from_file.items():
                if (_key in list(LocalStorage.config_folders.keys())):
                    self._data_for_scan.update(_data)
                elif "last_time_sync" in _key:
                    self._last_time_sync.update(_data)
                else:
                    LocalStorage.lslogger.error(f"found key not registered volumen - {_key}")
        
        @lock
        def dump_info(self, videos_cached, last_time_sync):
                            
            def getter(x):
                if 'Pandaext4/videos' in x: return 'pandaext4'
                elif 'WD5/videos' in x: return 'wd5'
                elif 'WD1B/videos' in x: return 'wd1b'
                elif 'antoniotorres/testing' in x: return 'local'
                elif 'DatosToni/videos' in x: return 'datostoni'
                elif 'WD8_1/videos' in x: return 'wd8_1'
                elif 'WD8_2/videos' in x: return 'wd8_2'

            if videos_cached:
                self._data_for_scan = videos_cached.copy()
            if last_time_sync:
                self._last_time_sync = last_time_sync.copy()
            
            _temp = {"last_time_sync": {}, "local": {}, "wd5": {}, "wd1b": {}, "pandaext4": {}, "datostoni": {},  "wd8_1": {}, "wd8_2": {}}
            
            _temp.update({"last_time_sync": last_time_sync})                    
    
            for key,val in videos_cached.items():                   
                
                _vol = getter(val)
                if not _vol:
                    LocalStorage.lslogger.error(f"found file with not registered volumen - {val} - {key}")
                else:
                    _temp[getter(val)].update({key: val})
                    
                    
            shutil.copy(str(LocalStorage.local_storage), str(LocalStorage.prev_local_storage))                  

            with open(LocalStorage.local_storage, "w") as f:
                json.dump(_temp,f)
                
            self._data_from_file = _temp
            
def print_tasks(tasks):
    #return [f"{task.get_name()} : {str(task.get_coro()).split(' ')[0]}\n" for task in tasks]
    return "\n".join([f"{task.get_name()} : {repr(task.get_coro()).split(' ')[2]}" for task in tasks])

def print_threads(threads):
    return "\n".join([f"{thread.getName()} : {repr(thread._target)}" for thread in threads])

def none_to_zero(item):
    return(0 if not item else item)

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
    
    try:
        def _log(msg):
            logger.info(msg) if logger else print(msg)
            
        term = (subprocess.run(["tty"], encoding='utf-8', capture_output=True).stdout).splitlines()[0].replace("/dev/", "")
        res = subprocess.run(["ps", "-u", "501", "-x", "-o" , "pid,tty,command"], encoding='utf-8', capture_output=True).stdout
        if rpcport: _aria2cstr = f"aria2c.+--rpc-listen-port {rpcport}.+"
        else: _aria2cstr = f"aria2cDUMMY"
        mobj = re.findall(rf'(\d+)\s+(?:\?\?|{term})\s+((?:.+browsermob-proxy --port.+|{_aria2cstr}|geckodriver.+|java -Dapp.name=browsermob-proxy.+|/Applications/Firefox.app/Contents/MacOS/firefox-bin.+))', res)
        mobj2 = re.findall(rf'\d+\s+(?:\?\?|{term})\s+/Applications/Firefox.app/Contents/MacOS/firefox-bin.+--profile (/var/folders/[^\ ]+) ', res)
        mobj3 = re.findall(rf'(\d+)\s+(?:\?\?|{term})\s+((?:.+async_all\.py))', res)
        if mobj:
            proc_to_kill = list(set(mobj))                    
            results = [subprocess.run(["kill","-9",f"{process[0]}"], encoding='utf-8', capture_output=True) for process in proc_to_kill]
            _debugstr  = [f"pid: {proc[0]}\n\tcommand: {proc[1]}\n\tres: {res}" for proc, res in zip(proc_to_kill, results)]
            _log("[kill_processes]\n" + '\n'.join(_debugstr))
        else: 
            _log("[kill_processes] No processes found to kill")
        #_log(f"[kill_processes_proxy]\n{mobj3}")
        if len(mobj3) > 1:
            proc_to_kill = mobj3[1:]                    
            results = [subprocess.run(["kill","-9",f"{process[0]}"], encoding='utf-8', capture_output=True) for process in proc_to_kill]
            _debugstr  = [f"pid: {proc[0]}\n\tcommand: {proc[1]}\n\tres: {res}" for proc, res in zip(proc_to_kill, results)]
            _log("[kill_processes_proxy]\n" + '\n'.join(_debugstr))
            
        if mobj2:
            for el in mobj2:
                try:
                    shutil.rmtree(el, ignore_errors=True)
                except Exception as e:
                    _log(f"[kill_processes] error: {repr(e)}")
    except Exception as e:
        _log(f"[kill_processes_proxy]: {repr(e)}")
        raise

            
def foldersize(folder):
    #devuelve en bytes size folder
    return sum(file.stat().st_size for file in Path(folder).rglob('*') if file.is_file())

def folderfiles(folder):
    count = 0
    for file in Path(folder).rglob('*'):
        if file.is_file(): count += 1
        
    return count

def int_or_none(res):
    return int(res) if res else None

def naturalsize(value, binary=False, gnu=False, format_="6.2f"):
    """Format a number of bytes like a human readable filesize (e.g. 10 kB).

    By default, decimal suffixes (kB, MB) are used.

    Non-GNU modes are compatible with jinja2's `filesizeformat` filter.

    Examples:
        ```pycon
        >>> naturalsize(3000000)
        '3.0 MB'
        >>> naturalsize(300, False, True)
        '300B'
        >>> naturalsize(3000, False, True)
        '2.9K'
        >>> naturalsize(3000, False, True, "%.3f")
        '2.930K'
        >>> naturalsize(3000, True)
        '2.9 KiB'

        ```
    Args:
        value (int, float, str): Integer to convert.
        binary (bool): If `True`, uses binary suffixes (KiB, MiB) with base
            2<sup>10</sup> instead of 10<sup>3</sup>.
        gnu (bool): If `True`, the binary argument is ignored and GNU-style
            (`ls -sh` style) prefixes are used (K, M) with the 2**10 definition.
        format (str): Custom formatter.

    Returns:
        str: Human readable representation of a filesize.
    """
    
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
        return f"{(base * abs_bytes / unit):{format_}}{s}"
    return f"{(base*abs_bytes/unit):{format_}} {s}"

def print_norm_time(time):
    """ Time in secs """
    
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    
    return f"{hour:.0f}h:{minutes:.0f}min:{seconds:.0f}secs"

def get_values_regex(str_reg_list, str_content, *_groups, not_found=None):
    
    for str_reg in str_reg_list:
    
        mobj = re.search(str_reg, str_content)
        if mobj:
            res = mobj.group(*_groups)
            return res
        
    return not_found

if _SUPPORT_PYSIMP:

    def init_gui_root():
    
        try:
            
            logger = logging.getLogger("asyncDL")
            
            sg.theme("SystemDefaultForReal")
            
            col_0 = sg.Column([
                                [sg.Text("WAITING TO DL", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(70, 40), font=("Courier New Bold", 10), write_only=True, key='-ML0-', autoscroll=True, auto_refresh=True)]
            ], element_justification='l', expand_x=True, expand_y=True)
            
            col_00 = sg.Column([                                 
                                [sg.Text("Waiting for info", size=(80, 2), font=("Courier New Bold", 12), key='ST')]
            ])
            col_1 = sg.Column([
                                [sg.Text("NOW DOWNLOADING/CREATING FILE", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(90, 35), font=("Courier New Bold", 11), write_only=True, key='-ML1-', autoscroll=True, auto_refresh=True)],
                                [sg.Multiline(default_text = "Waiting for info", size=(90, 5), font=("Courier New Bold", 10), write_only=True, key='-ML3-', autoscroll=True, auto_refresh=True)]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            col_2 = sg.Column([
                                [sg.Text("DOWNLOADED/STOPPED/ERRORS", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(70, 40), font=("Courier New Bold", 10), write_only=True, key='-ML2-', autoscroll=True, auto_refresh=True)]
            ], element_justification='r', expand_x=True, expand_y=True)
            
            layout_root = [ [col_00], 
                            [col_0, col_1, col_2] ]
            
            window_root = sg.Window('async_downloader', layout_root, alpha_channel=0.99, location=(0, 0), finalize=True, resizable=True)
            window_root.set_min_size(window_root.size)
            
            window_root['-ML0-'].expand(True, True, True)
            window_root['-ML1-'].expand(True, True, True)
            window_root['-ML2-'].expand(True, True, True)
            window_root['-ML3-'].expand(True, True, True)
            
            return window_root
            
        except Exception as e:
            logger.exception(f'[init_gui] error {repr(e)}')

    def init_gui_console():
        
        try:
            
            logger = logging.getLogger("asyncDL")
            
            sg.theme("SystemDefaultForReal")
            
            col_pygui = sg.Column([
                                    [sg.Text('Select DL', font='Any 14')],
                                    [sg.Input(key='-IN-', font='Any 10', focus=True)],
                                    [sg.Multiline(size=(50, 12), font='Any 10', write_only=True, key='-ML-', reroute_cprint=True, auto_refresh=True, autoscroll=True)],
                                    [sg.Checkbox('PasRes', key='-PASRES-', default=False, enable_events=True), sg.Checkbox('WkInit', key='-WKINIT-', default=True, enable_events=True), sg.Button('+PasRes'), sg.Button('-PasRes'), sg.Button('DLStatus', key='-DL-STATUS'), sg.Button('Info'), sg.Button('ToFile'), sg.Button('+runwk', key='IncWorkerRun'), sg.Button('#vidwk', key='NumVideoWorkers'), sg.Button('TimePasRes'), sg.Button('Pause'), sg.Button('Resume'), sg.Button('Reset'), sg.Button('Stop'), sg.Button('Exit')]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            layout_pygui = [ [col_pygui] ]

            window_console = sg.Window('Console', layout_pygui, alpha_channel=0.99, location=(0, 500), finalize=True, resizable=True)
            window_console.set_min_size(window_console.size)
            window_console['-ML-'].expand(True, True, True)
            
            window_console.bring_to_front()
            
            return window_console
        
        except Exception as e:
            logger.exception(f'[init_gui] error {repr(e)}')

    def init_gui_rclone():
    
        try:
            
            logger = logging.getLogger("rclone-san")
            
            sg.theme("SystemDefaultForReal")
            
            col_00 = sg.Column([                                 
                                [sg.Text("Waiting for info", size=(150, 2), font='Any 10', key='ST')]
            ])
            
            col_0 = sg.Column([
                                [sg.Text("NOW RCLONE", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML0-', autoscroll=True, auto_refresh=True)]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            col_1 = sg.Column([
                                [sg.Text("NOW MOVING", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML1-', autoscroll=True, auto_refresh=True)]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            col_2 = sg.Column([
                                [sg.Text("FINISHED", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML2-', autoscroll=True, auto_refresh=True)]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            layout_root = [ [col_00],
                           [col_0, col_1, col_2] ]
            
            window_root = sg.Window('rclone', layout_root, alpha_channel=0.99, location=(0, 0), finalize=True, resizable=True)
            window_root.set_min_size(window_root.size)
            
            window_root['-ML0-'].expand(True, True, True)
            window_root['-ML1-'].expand(True, True, True)
            window_root['-ML2-'].expand(True, True, True)

            return(window_root)
        
        except Exception as e:
            logger.exception(f'[init_gui] error {repr(e)}')

 
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
        def __init__(self, *args,**kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPConnectionPool, self).__init__(*args,**kwargs)
    poolmanager.pool_classes_by_scheme['http'] = MyHTTPConnectionPool
    
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
        def __init__(self, *args,**kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPSConnectionPool, self).__init__(*args,**kwargs)
    poolmanager.pool_classes_by_scheme['https'] = MyHTTPSConnectionPool
    
def get_ip_proxy():
    with open(Path(Path.home(),"Projects/common/ipproxies.json"), "r") as f:
        return(random.choice(json.load(f)))

if _SUPPORT_HTTPX:

    def send_http_request(url, **kwargs):
        
        logger = logging.getLogger("test")

        try:
            _type = kwargs.get('_type', "GET")
            headers = kwargs.get('headers', None)
            data = kwargs.get('data', None)
            msg = kwargs.get('msg', None)
            premsg = f'[send_http_request][{(url)}][{_type}]'
            if msg: 
                premsg = f'{msg}{premsg}'           

            _CLIENT = kwargs.get('client')
            res = None
            _msg_err = ""
            req = _CLIENT.build_request(_type, url, data=data, headers=headers)
            res = _CLIENT.send(req)
            if res:
                res.raise_for_status()                
                return res
            else: return ""
        except Exception as e:            
            _msg_err = repr(e)
            if res and res.status_code == 403:                
                raise ReExtractInfo(_msg_err)
            if res and res.status_code == 404:           
                res.raise_for_status()
            elif res and res.status_code == 503:
                raise StatusError503(repr(e))
            elif isinstance(e, ConnectError):
                if 'errno 61' in _msg_err.lower():                    
                    raise
                else:
                    raise Exception(_msg_err)   
            elif not res:
                raise TimeoutError(_msg_err)
            else:
                raise Exception(_msg_err) 
        finally:                
            logger.info(f"{premsg} {req}:{res}:{_msg_err}")

    def check_proxy(ip, port, queue_ok=None):
        try:
            
            cl = httpx.Client(proxies={"http://": f"http://atgarcia:ID4KrSc6mo6aiy8@{ip}:{port}"},timeout=10, follow_redirects=True)
            res = cl.get("https://checkip.dyndns.org")            
            print(f"{ip}:{port}:{res}")
            print(res.text)
            if res.status_code == 200:
                if queue_ok:
                    queue_ok.put((ip, port, res))
        except Exception as e:
            print(f"{ip}:{port}:{e}")
        finally:
            cl.close()

    def status_proxy(name):
        
        #dscacheutil -q host -a name proxy.torguard.org
        res = subprocess.run(f"dscacheutil -q host -a name {name}".split(' '), encoding='utf-8', capture_output=True).stdout
        IPS_SSL = re.findall(r"ip_address: (.+)", res)
        
        #IPS_ES_SSL = ["192.145.124.186", "192.145.124.234", "89.238.178.234", "192.145.124.242", "192.145.124.226", "192.145.124.238", "192.145.124.174", "89.238.178.206", "192.145.124.190"]
        
    # IPS_TORGUARD = ["82.129.66.196"]
        
        #PORTS = [6060,1337,1338,1339,1340,1341,1342,1343]
        PORTS_SSL = [489, 23, 7070, 465, 993, 282, 778, 592]

        queue_ok = Queue()

        futures = []
        
        with ThreadPoolExecutor(max_workers=8) as ex:
            for ip in IPS_SSL: 
                for port in PORTS_SSL:            
                    futures.append(ex.submit(check_proxy, ip, port, queue_ok))
            
        
        list_res = list(queue_ok.queue)
        
        list_ok = list(set([res[0] for res in list_res]))
        
        print(list_ok)
        
        queue_rtt = Queue() 
        
        def _get_rtt(ip):
            res = subprocess.run(["ping","-c","10","-q","-S","192.168.1.128", ip], encoding='utf-8', capture_output=True).stdout
            mobj = re.findall(r'= [^\/]+\/([^\/]+)\/', res)
            if mobj: _tavg = float(mobj[0])
            print(f"{ip}:{_tavg}")
            queue_rtt.put({'ip': ip, 'time': _tavg})
            
        futures = []
        
        with ThreadPoolExecutor(max_workers=8) as ex: 
            for ipl in list_ok:
                futures.append(ex.submit(_get_rtt, ipl))
            
        list_ord = list(queue_rtt.queue)

        def myFunc(e):
            return(e['time'])
        
        list_ord.sort(key=myFunc)
        
        with open(Path(Path.home(),"Projects/common/ipproxies.json"), "w") as f:
            f.write(json.dumps(list_ord))
        
        list_final = [el['ip'] for el in list_ord]
        
        print(json.dumps(list_final))
        return(list_ord)

def parse_ffmpeg_time_string(time_string):
    time = 0
    reg1 = re.match(r"((?P<H>\d\d?):)?((?P<M>\d\d?):)?(?P<S>\d\d?)(\.(?P<f>\d{1,3}))?", time_string)
    reg2 = re.match(r"\d+(?P<U>s|ms|us)", time_string)
    if reg1:
        if reg1.group('H') is not None:
            time += 3600 * int(reg1.group('H'))
        if reg1.group('M') is not None:
            time += 60 * int(reg1.group('M'))
        time += int(reg1.group('S'))
        if reg1.group('f') is not None:
            time += int(reg1.group('f')) / 1_000
    elif reg2:
        time = int(reg2.group('U'))
        if reg2.group('U') == 'ms':
            time /= 1_000
        elif reg2.group('U') == 'us':
            time /= 1_000_000
    return time

def compute_prefix(match):
    res = int(match.group('E'))
    if match.group('f') is not None:
        res += int(match.group('f'))
    if match.group('U') is not None:
        if match.group('U') == 'g':
            res *= 1_000_000_000
        elif match.group('U') == 'm':
            res *= 1_000_000
        elif match.group('U') == 'k':
            res *= 1_000
    return res


def _open_vpn(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            _proc = subprocess.run(['pkill', 'TorGuardDesktopQt'])
            relaunch = (_proc.returncode == 0)
            cmd = 'sudo openvpn --config /Users/antoniotorres/.config/openvpn/openvpnbrit.conf'
            _openvpn = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            _openvpn.poll()
            cont = 0
            while (_openvpn.returncode == None):
                _openvpn.poll()
                time.sleep(1)
                cont += 1
                if cont == 15: 
                    raise ExtractorError("openvpn couldnt get started")
            _ip = try_get(self._download_json("https://api.ipify.org?format=json", None), lambda x: x.get('ip'))
            self.to_screen(f"openvpn: ok, IP origen; {_ip}")   
            return func(self, *args, **kwargs)
        finally:
            subprocess.run(['sudo', 'pkill', 'openvpn'])
            if relaunch:
                subprocess.run(['open', '/Applications/Torguard.app'])
    return wrapper


@long_operation_in_process
def get_videos_cached(*args, **kwargs):        
        
        logger = logging.getLogger('test')

        """
        In local storage, files are saved wihtin the file files.cached.json in 5 groups each in different volumnes.
        If any of the volumes can't be accesed in real time, the local storage info of that volume will be used.    
        """

        queue = kwargs.get('queue')

        local_storage = LocalStorage()       

        #logger.info(f"[videos_cached] start scanning - dlnocaching [{self.args.nodlcaching}]")
        
        videos_cached = {}        
        last_time_sync = {}        
        
        try:            
            
            with local_storage.lock:
            
                local_storage.load_info()
                
                list_folders_to_scan = {}
                videos_cached = {}            
                last_time_sync = local_storage._last_time_sync
                            
                 
                for _vol,_folder in local_storage.config_folders.items():
                    if not _folder.exists(): #comm failure
                        logger.error(f"Fail to connect to [{_vol}], will use last info")
                        videos_cached.update(local_storage._data_from_file.get(_vol))
                    else:
                        list_folders_to_scan.update({_folder: _vol})

                _repeated = []
                _dont_exist = []

                launch_time = datetime.now()
                    
                for folder in list_folders_to_scan:
                    
                    try:
                        
                        files = [file for file in folder.rglob('*') 
                                if file.is_file() and not file.stem.startswith('.') and (file.suffix.lower() in ('.mp4', '.mkv', '.zip'))]
                        
                        for file in files:                        

                            _res = file.stem.split('_', 1)
                            if len(_res) == 2:
                                _id = _res[0]
                                _title = sanitize_filename(_res[1], restricted=True).upper()                                
                                _name = f"{_id}_{_title}"
                            else:
                                _name = sanitize_filename(file.stem, restricted=True).upper()

                            if not (_video_path_str:=videos_cached.get(_name)): 
                                
                                videos_cached.update({_name: str(file)})
                                
                            else:
                                _video_path = Path(_video_path_str)
                                if _video_path != file: 
                                    
                                    if not file.is_symlink() and not _video_path.is_symlink(): #only if both are hard files we have to do something, so lets report it in repeated files
                                        _repeated.append({'title':_name, 'indict': _video_path_str, 'file': str(file)})
                                    elif not file.is_symlink() and _video_path.is_symlink():
                                            _links = get_chain_links(_video_path)                                             
                                            if (_links[-1] == file):
                                                if len(_links) > 2:
                                                    logger.debug(f'[videos_cached] \nfile not symlink: {str(file)}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links])}')
                                                    for _link in _links[0:-1]:
                                                        _link.unlink()
                                                        _link.symlink_to(file)
                                                        _link._accessor.utime(_link, (int(launch_time().timestamp()), file.stat().st_mtime), follow_symlinks=False)
                                                
                                                videos_cached.update({_name: str(file)})
                                            else:
                                                logger.warning(f'[videos_cached] \n**file not symlink: {str(file)}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links])}')
                                                    
                                    elif file.is_symlink() and not _video_path.is_symlink():
                                        _links =  get_chain_links(file)
                                        if (_links[-1] == _video_path):
                                            if len(_links) > 2:
                                                logger.debug(f'[videos_cached] \nfile symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links])}\nvideopath not symlink: {str(_video_path)}')
                                                for _link in _links[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_video_path)
                                                    _link._accessor.utime(_link, (int(launch_time().timestamp()), _video_path.stat().st_mtime), follow_symlinks=False)
                                                
                                            videos_cached.update({_name: str(_video_path)})
                                            if not _video_path.exists(): _dont_exist.append({'title': _name, 'file_not_exist': str(_video_path), 'links': [str(_l) for _l in _links[0:-1]]})
                                        else:
                                            logger.warning(f'[videos_cached] \n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links])}\nvideopath not symlink: {str(_video_path)}')

                                    else:
                                        _links_file = get_chain_links(file) 
                                        _links_video_path = get_chain_links(_video_path)
                                        if ((_file:=_links_file[-1]) == _links_video_path[-1]):
                                            if len(_links_file) > 2:
                                                logger.debug(f'[videos_cached] \nfile symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}')                                                
                                                for _link in _links_file[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(_link, (int(launch_time().timestamp()), _file.stat().st_mtime), follow_symlinks=False)
                                            if len(_links_video_path) > 2:
                                                logger.debug(f'[videos_cached] \nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}')
                                                for _link in _links_video_path[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(_link, (int(launch_time().timestamp()), _file.stat().st_mtime), follow_symlinks=False)
                                            
                                            videos_cached.update({_name: str(_file)})
                                            if not _file.exists():  _dont_exist.append({'title': _name, 'file_not_exist': str(_file), 'links': [str(_l) for _l in (_links_file[0:-1] + _links_video_path[0:-1])]})
                                                
                                        else:
                                            logger.warning(f'[videos_cached] \n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}') 

                    except Exception as e:
                        logger.error(f"[videos_cached][{list_folders_to_scan[folder]}] {repr(e)}")

                    else:
                        last_time_sync.update({list_folders_to_scan[folder]: str(launch_time)})
                    
                
                logger.info(f"[videos_cached] Total videos cached: [{len(videos_cached)}]")
                

                try:
                    
                    local_storage.dump_info(videos_cached, last_time_sync)           
                
                    if _repeated:                                                
                        logger.warning("[videos_cached] Please check videos repeated in logs")
                        logger.debug(f"[videos_cached] videos repeated: \n {_repeated}")
                    
                    if _dont_exist:
                        logger.warning("[videos_cached] Please check videos dont exist in logs")
                        logger.debug(f"[videos_cached] videos dont exist: \n {_dont_exist}")

                except Exception as e:
                    logger.exception(f"[videos_cached] {repr(e)}")
                finally:
                    queue.put_nowait(videos_cached)
                    
        except Exception as e:
            logger.exception(f"[videos_cached] {repr(e)}")




def check_if_dl(info_dict, videos):

    if not (_id := info_dict.get('id') ) or not ( _title := info_dict.get('title')):
        return False

    _title = sanitize_filename(_title, restricted=True).upper()
    vid_name = f"{_id}_{_title}"

    return videos.get(vid_name)