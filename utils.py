#!/usr/bin/env python
import argparse
import asyncio
import contextvars
import functools
import json
import logging
import logging.config
import random
import re
import shutil
import signal
import subprocess
import time
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
import contextlib
from itertools import zip_longest

PATH_LOGS = Path(Path.home(), "Projects/common/logs")

try:
    import proxy
    _SUPPORT_PROXY = True
except Exception:
    _SUPPORT_PROXY = False


try:
    import aria2p
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
    from yt_dlp.utils import (
        js_to_json, 
        try_get, 
        sanitize_filename, 
        traverse_obj,
        get_domain
    )

    from yt_dlp.extractor.commonwebdriver import (
        limiter_15, 
        limiter_5, 
        limiter_1, 
        limiter_non,
        dec_on_exception, 
        dec_retry_error,
        CONFIG_EXTRACTORS,
        SeleniumInfoExtractor
    )
    
    _SUPPORT_YTDL = True
except Exception:
    _SUPPORT_YTDL = False

import threading

try:    
    from filelock import FileLock
    _SUPPORT_FILELOCK = True
except Exception:
    _SUPPORT_FILELOCK = False

class SignalHandler:
    
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print(signum)
        print("Exiting gracefully")
        self.KEEP_PROCESSING = False
 
class EMA(object):
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

class MyLogger(logging.LoggerAdapter):
    #para ser compatible con el logging de yt_dlp: yt_dlp usa debug para enviar los debug y
    #los info. Los debug llevan '[debug] ' antes.
    #se pasa un logger de logging al crear la instancia 
    # mylogger = MyLogger(logging.getLogger("name_ejemplo", {}))
    
    _debug_phr = [  'Falling back on generic information extractor','Extracting URL:',
                    'The information of all playlist entries will be held in memory', 'Looking for video embeds',
                    'Identified a HTML5 media', 'Identified a KWS Player', ' unable to extract', 'Looking for embeds',
                    'Looking for Brightcove embeds', 'Identified a html5 embed']
    
    _skip_phr = ['Downloading', 'Extracting information', 'Checking', 'Logging']
    
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
        mobj = get_values_regex([r'^(\[[^\]]+\])'], msg) or ""
        mobj2 = msg.split(': ')[-1]

        if self.quiet:
            self.log(logging.DEBUG, msg, *args, **kwargs)
        elif self.verbose and not self.superverbose:
            if (mobj in ('[redirect]', '[download]', '[debug+]', '[info]')) or (mobj in ('[debug]') and any(_ in msg for _ in self._debug_phr)) or any(_ in mobj2 for _ in self._skip_phr):
                self.log(logging.DEBUG, msg[len(mobj):].strip(), *args, **kwargs)
            else:
                self.log(logging.INFO, msg, *args, **kwargs)            
        elif self.superverbose:
            self.log(logging.INFO, msg, *args, **kwargs)
        else:    
            if mobj in ('[redirect]', '[debug]', '[info]', '[download]', '[debug+]') or any(_ in mobj2 for _ in self._skip_phr):
                self.log(logging.DEBUG, msg[len(mobj):].strip(), *args, **kwargs)
            else:                
                self.log(logging.INFO, msg, *args, **kwargs)

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

def init_argparser():
    
 
    UA_LIST = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:104.0) Gecko/20100101 Firefox/104.0"]

    parser = argparse.ArgumentParser(description="Async downloader videos / playlist videos HLS / HTTP")
    parser.add_argument("-w", help="Number of DL workers", default="5", type=int)
    parser.add_argument("--winit", help="Number of init workers, default is same number for DL workers", default="0", type=int)
    parser.add_argument("-p", "--parts", help="Number of workers for each DL", default="16", type=int)
    parser.add_argument("--format", help="Format preferred of the video in youtube-dl format", default="bv*+ba/b", type=str)
    parser.add_argument("--sort", help="Formats sort preferred", default="ext:mp4:mp4a", type=str)
    parser.add_argument("--index", help="index of a video in a playlist", default=None, type=int)
    parser.add_argument("--file", help="jsonfiles", action="append", dest="collection_files", default=[])
    parser.add_argument("--nocheckcert", help="nocheckcertificate", action="store_true", default=False)
    parser.add_argument("--ytdlopts", help="init dict de conf", default="", type=str)
    parser.add_argument("--proxy", default=None, type=str)
    parser.add_argument("--useragent", default=UA_LIST[0], type=str)
    parser.add_argument("--first", default=None, type=int)
    parser.add_argument("--last", default=None, type=int)
    parser.add_argument("--nodl", help="not download", action="store_true", default=False)   
    parser.add_argument("--headers", default="", type=str)  
    parser.add_argument("-u", action="append", dest="collection", default=[])   
    parser.add_argument("--nodlcaching", help="dont get new cache videos dl, use previous", action="store_true", default=False)
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
            
    return args





if _SUPPORT_PROXY:
    
    logger = logging.getLogger("http_proxy")
    
    def long_operation_in_thread(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            stop_event = kwargs.get('event', threading.Event())
            _kwargs = {k:v for k,v in kwargs.items() if k != 'event'}
            thread = threading.Thread(target=func, args=(stop_event, *args), kwargs=_kwargs, daemon=True)
            thread.start()
            return stop_event
        return wrapper  

    @long_operation_in_thread
    def run_proxy_http(stop_event, log_level="DEBUG"):
        with proxy.Proxy(log_level=log_level, ca_cert_file='/Users/antoniotorres/Projects/proxy.py/venv/lib/python3.9/site-packages/certifi/cacert.pem',
                         plugins=[b'proxy.plugin.cache.CacheResponsesPlugin', b'proxy.plugin.ProxyPoolByHostPlugin']) as p:
            try:
                logger.info(p.flags)
                while not stop_event.is_set():
                    time.sleep(1)
            except KeyboardInterrupt:
                raise
            
            

if _SUPPORT_ARIA2P:
    
    def init_aria2c(args):
        
        logger = logging.getLogger("asyncDL")
        res = subprocess.run(["ps", "-u", "501", "-x", "-o" , "pid,tty,command"], encoding='utf-8', capture_output=True).stdout
        mobj = re.findall(r"aria2c.+--rpc-listen-port ([^ ]+).+", res)
        if mobj:
            if str(args.rpcport) in mobj:
                mobj.sort()
                args.rpcport = int(mobj[-1]) + 100
                
        subprocess.run(["aria2c","--rpc-listen-port",f"{args.rpcport}", "--enable-rpc","--daemon"])
        logger.info(f"aria2c daemon running on port: {args.rpcport} ")
        cl = aria2p.API(aria2p.Client(port=args.rpcport))
        opts = cl.get_global_options()
        logger.debug(f"aria2c options:\n{opts._struct}")
        del opts
        del cl
        

def grouper(iterable, n, *, incomplete='fill', fillvalue=None):
    "Collect data into non-overlapping fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, fillvalue='x') --> ABC DEF Gxx
    # grouper('ABCDEFG', 3, incomplete='strict') --> ABC DEF ValueError
    # grouper('ABCDEFG', 3, incomplete='ignore') --> ABC DEF
    args = [iter(iterable)] * n
    if incomplete == 'fill':
        return zip_longest(*args, fillvalue=fillvalue)
    if incomplete == 'strict':
        return zip(*args, strict=True)
    if incomplete == 'ignore':
        return zip(*args)
    else:
        raise ValueError('Expected fill, strict, or ignore')      
        
    
def init_proxies(n=10, num_el_set=4):
        
    #SP
    IPS_SSL = ['89.238.178.206', '192.145.124.242', '89.238.178.234', '192.145.124.234', '192.145.124.230', 
               '192.145.124.174', '192.145.124.190', '192.145.124.186', '192.145.124.238', '192.145.124.226'] 
    #FR
    IPS_SSL += ['93.177.75.90', '93.177.75.18', '93.177.75.26', '93.177.75.122', '93.177.75.138', '93.177.75.2', 
                '93.177.75.146', '93.177.75.42', '93.177.75.50', '93.177.75.154', '93.177.75.34', '93.177.75.98', 
                '93.177.75.202', '93.177.75.218', '93.177.75.106', '37.120.158.138', '93.177.75.10', '93.177.75.210', 
                '93.177.75.130', '93.177.75.162', '93.177.75.82', '93.177.75.74', '93.177.75.58', '93.177.75.66', '93.177.75.114']    
    #UK
    IPS_SSL += ['146.70.83.170', '146.70.95.34', '37.120.198.162', '146.70.95.18', '146.70.83.130', '146.70.83.162', 
                '146.70.83.178', '146.70.83.210', '146.70.83.250', '146.70.95.58', '146.70.95.66', '146.70.83.202', 
                '146.70.83.194', '146.70.83.154', '146.70.83.242', '146.70.83.186', '146.70.95.50', '146.70.83.146', 
                '146.70.83.234', '146.70.95.42', '146.70.83.218', '146.70.83.226', '185.253.98.42']
    #DE
    IPS_SSL += ['93.177.73.210', '93.177.73.234', '93.177.73.130', '93.177.73.114', '93.177.73.154', '93.177.73.98', '93.177.73.202', 
                '93.177.73.138', '93.177.73.90', '93.177.73.218', '93.177.73.74', '93.177.73.146', '93.177.73.66', '93.177.73.194', 
                '93.177.73.106', '93.177.73.122', '93.177.73.226', '93.177.73.82']
    
    
    logger = logging.getLogger("asyncDL")
    
    _ips = random.sample(IPS_SSL, n * num_el_set)
    
    FINAL_IPS = list(grouper(_ips, num_el_set))
        
    cmd_gost_simple = [f"gost -L=:{1235 + 10*i} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[0]}:7070" for i, ip in enumerate(FINAL_IPS)]
    cmd_gost_ip0 = [f"gost -L=:{1235 + 10*i + 1} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[1]}:7070" for i, ip in enumerate(FINAL_IPS)]
    cmd_gost_ip1 = [f"gost -L=:{1235 + 10*i + 2} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[2]}:7070" for i, ip in enumerate(FINAL_IPS)]
    cmd_gost_ip2 = [f"gost -L=:{1235 + 10*i + 3} -F=http+tls://atgarcia:ID4KrSc6mo6aiy8@{ip[3]}:7070" for i, ip, in enumerate(FINAL_IPS)]
    #cmd_gost_group =  [f"gost -L=:{1235 + 10*i + 9} -F=':{1235 + 10*i + 9}?ip={','.join([f':{1235 + 10*i + j}' for j in range(1,num_el_set)])}&strategy=round&max_fails=10&fail_timeout=1s'" for i in range(n)]
    cmd_gost_group =  [f"gost -L=:{1235 + 10*i + 9} -F=:8899" for i in range(n)] 
    
    cmd_gost =cmd_gost_simple + cmd_gost_ip0 +  cmd_gost_ip1 +  cmd_gost_ip2 + cmd_gost_group
    
    logger.debug(f"[init_proxies] {cmd_gost}")

    proc_gost = [subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True) for cmd in cmd_gost]
    
    return proc_gost
        


if _SUPPORT_YTDL:

    class AsyncYTDL(YoutubeDL):
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
        
        async def async_extract_info(self, executor, url, **kwargs):
            return await async_ex_in_executor(executor, self.extract_info, url, **kwargs)
            
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
        if args.proxy:        
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
            "sem": {}
                    
        }
        
        if args.use_cookies:
            ytdl_opts.update(
                {
                    "cookiesfrombrowser": ('firefox', '/Users/antoniotorres/Library/Application Support/Firefox/Profiles/c3nsqmmt.default-1637669918519', None)
                })
        
        if args.ytdlopts: 
            ytdl_opts.update(json.loads(js_to_json(args.ytdlopts)))
            
        ytdl = YoutubeDL(ytdl_opts)
        
        logger.info(f"ytdl opts:\n{ytdl.params}")   
        
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
        
        config_folders = {'local': Path(Path.home(), "testing"), 'pandaext4': Path("/Volumes/Pandaext4/videos"), 'datostoni': Path("/Volumes/DatosToni/videos"), 'wd1b': Path("/Volumes/WD1B/videos"), 'wd5': Path("/Volumes/WD5/videos")}

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
                        "wd5": Path("/Volumes/WD5/videos")}
        
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

            if videos_cached:
                self._data_for_scan = videos_cached.copy()
            if last_time_sync:
                self._last_time_sync = last_time_sync.copy()
            
            _temp = {"last_time_sync": {}, "local": {}, "wd5": {}, "wd1b": {}, "pandaext4": {}, "datostoni": {}}
            
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

def perform_long_operation(_func, *args, **kwargs):

    stop_event = kwargs.get('event', threading.Event())
    _kwargs = {k:v for k,v in kwargs.items() if k != 'event'}
    thread = threading.Thread(target=_func, args=(stop_event, *args), kwargs=_kwargs, daemon=True)
    thread.start()
    return(thread, stop_event)

async def async_ex_in_executor(executor, func, /, *args, **kwargs):
    loop = kwargs.get('loop', asyncio.get_running_loop())
    ctx = contextvars.copy_context()
    _kwargs = {k:v for k,v in kwargs.items() if k != 'loop'}
    func_call = functools.partial(ctx.run, func, *args, **_kwargs)        
    return await loop.run_in_executor(executor, func_call)
    
async def async_ex_in_thread(prefix, func, /, *args, **kwargs):
        
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    ex = ThreadPoolExecutor(thread_name_prefix=prefix)    
    #return await asyncio.to_thread(func_call)
    return await loop.run_in_executor(ex, func_call)
    

@contextlib.asynccontextmanager
async def async_lock(executor, lock):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, lock.acquire)
    try:
        yield  # the lock is held
    finally:
        lock.release()

async def async_wait_time(n):
   
    _started = time.monotonic()
    while True:
        if (_t:=(time.monotonic() - _started)) >= n:
            return _t
        else:
            await asyncio.sleep(0)
            
def wait_time(n):
    _started = time.monotonic()
    while True:
        if (_t:=(time.monotonic() - _started)) >= n:
            return _t
        else:
            time.sleep(n/2)

def none_to_cero(item):
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
    
    def _log(msg):
        logger.debug(msg) if logger else print(msg)
        
    term = (subprocess.run(["tty"], encoding='utf-8', capture_output=True).stdout).splitlines()[0].replace("/dev/", "")
    res = subprocess.run(["ps", "-u", "501", "-x", "-o" , "pid,tty,command"], encoding='utf-8', capture_output=True).stdout
    if rpcport: _aria2cstr = f"aria2c.+--rpc-listen-port {rpcport}.+"
    else: _aria2cstr = f"aria2cDUMMY"
    mobj = re.findall(rf'(\d+)\s+(?:\?\?|{term})\s+((?:.+browsermob-proxy --port.+|{_aria2cstr}|geckodriver.+|java -Dapp.name=browsermob-proxy.+|/Applications/Firefox.app/Contents/MacOS/firefox-bin.+))', res)
    #mobj = re.findall(rf'(\d+)\s+(?:\?\?|{term})\s+((?:.+browsermob-proxy --port.+|{_aria2cstr}|geckodriver.+|java -Dapp.name=browsermob-proxy.+))', res)
    mobj2 = re.findall(rf'\d+\s+(?:\?\?|{term})\s+/Applications/Firefox.app/Contents/MacOS/firefox-bin.+--profile (/var/folders/[^\ ]+) ', res)
    if mobj:
        proc_to_kill = list(set(mobj))                    
        results = [subprocess.run(["kill","-9",f"{process[0]}"], encoding='utf-8', capture_output=True) for process in proc_to_kill]
            
        _debugstr  = [f"pid: {proc[0]}\n\tcommand: {proc[1]}\n\tres: {res}" for proc, res in zip(proc_to_kill, results)]
            
        _log("[kill_processes]\n" + '\n'.join(_debugstr))
            
    
    else: 
        _log("[kill_processes] No processes found to kill") 
        
    if mobj2:
        
        for el in mobj2:
            shutil.rmtree(el, ignore_errors=True)
            
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
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML0-', autoscroll=True, auto_refresh=True)]
            ], element_justification='l', expand_x=True, expand_y=True)
            
            col_1 = sg.Column([
                                [sg.Text("NOW DOWNLOADING/CREATING FILE", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(80, 25), font='Any 10', write_only=True, key='-ML1-', autoscroll=True, auto_refresh=True)]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            col_2 = sg.Column([
                                [sg.Text("DOWNLOADED/STOPPED/ERRORS", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML2-', autoscroll=True, auto_refresh=True)]
            ], element_justification='r', expand_x=True, expand_y=True)
            
            layout_root = [ [col_0, col_1, col_2] ]
            
            window_root = sg.Window('async_downloader', layout_root, alpha_channel=0.99, location=(0, 0), finalize=True, resizable=True)
            window_root.set_min_size(window_root.size)
            
            window_root['-ML0-'].expand(True, True, True)
            window_root['-ML1-'].expand(True, True, True)
            window_root['-ML2-'].expand(True, True, True)
            
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
                                    [sg.Checkbox('PasRes', key='-PASRES-', default=True, enable_events=True), sg.Checkbox('WkInit', key='-WKINIT-', default=True, enable_events=True), sg.Button('+PasRes'), sg.Button('-PasRes'), sg.Button('DLStatus', key='-DL-STATUS'), sg.Button('Info'), sg.Button('ToFile'), sg.Button('+runwk', key='IncWorkerRun'), sg.Button('#vidwk', key='NumVideoWorkers'), sg.Button('TimePasRes'), sg.Button('Pause'), sg.Button('Resume'), sg.Button('Reset'), sg.Button('Stop'), sg.Button('Exit')]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            layout_pygui = [ [col_pygui] ]

            window_console = sg.Window('Console', layout_pygui, alpha_channel=0.99, location=(0, 500), finalize=True, resizable=True)
            window_console.set_min_size(window_console.size)
            window_console['-ML-'].expand(True, True, True)
            
            window_console.bring_to_front()
            
            return window_console
        
        except Exception as e:
            logger.exception(f'[init_gui] error {repr(e)}')
            
    def init_gui_result():
        
        try:
            
            logger = logging.getLogger("asyncDL")
            
            sg.theme("SystemDefaultForReal")
            
            col = sg.Column([
                                [sg.Text("RESULTS", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(160, 50), font='Any 10', write_only=True, key='-MLRES-', reroute_cprint=True, autoscroll=True, auto_refresh=True)]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            layout = [ [col] ]
            
            window = sg.Window('Console', layout, alpha_channel=0.99, location=(0, 500), finalize=True, resizable=True)
            window.set_min_size(window.size)
            window['-MLRES-'].expand(True, True, True)
            
            window.bring_to_front()
            
            return window
        
        except Exception as e:
            logger.exception(f'[init_gui_result] error {repr(e)}')
            

    def init_gui():
        
        try:
            
            logger = logging.getLogger("asyncDL")
            
            sg.theme("SystemDefaultForReal")
            
            col_0 = sg.Column([
                                [sg.Text("WAITING TO DL", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML0-', autoscroll=True, auto_refresh=True)]
            ], element_justification='l', expand_x=True, expand_y=True)
            
            col_1 = sg.Column([
                                [sg.Text("NOW DOWNLOADING/CREATING FILE", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(80, 25), font='Any 10', write_only=True, key='-ML1-', autoscroll=True, auto_refresh=True)]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            col_2 = sg.Column([
                                [sg.Text("DOWNLOADED/STOPPED/ERRORS", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML2-', autoscroll=True, auto_refresh=True)]
            ], element_justification='r', expand_x=True, expand_y=True)
            
            
            col_pygui = sg.Column([
                                    [sg.Text('Select DL', font='Any 14')],
                                    [sg.Input(key='-IN-', font='Any 10', focus=True)],
                                    [sg.Multiline(size=(50, 12), font='Any 10', write_only=True, key='-ML-', reroute_cprint=True, auto_refresh=True, autoscroll=True)],
                                    [sg.Checkbox('PasRes', key='-PASRES-', default=True, enable_events=True), sg.Checkbox('WkInit', key='-WKINIT-', default=True, enable_events=True), sg.Button('+PasRes'), sg.Button('-PasRes'), sg.Button('DLStatus', key='-DL-STATUS'), sg.Button('Info'), sg.Button('ToFile'), sg.Button('+runwk', key='IncWorkerRun'), sg.Button('#vidwk', key='NumVideoWorkers'), sg.Button('TimePasRes'), sg.Button('Pause'), sg.Button('Resume'), sg.Button('Reset'), sg.Button('Stop'), sg.Button('Exit')]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            layout_single = [   [col_0, col_1, col_2], 
                                [col_pygui] ]


            window_single = sg.Window('async_downloader', layout_single,  alpha_channel=0.99, location=(0, 0), finalize=True, resizable=True)
            window_single.set_min_size(window_single.size)
            
            window_single['-ML0-'].expand(True, True, True)
            window_single['-ML1-'].expand(True, True, True)
            window_single['-ML2-'].expand(True, True, True)
            window_single['-ML-'].expand(True, True, True)
            
            return window_single
        
        except Exception as e:
            logger.exception(f'[init_gui] error {repr(e)}')
    

    def _init_gui():
    
        try:
            
            logger = logging.getLogger("asyncDL")
            
            sg.theme("SystemDefaultForReal")
            
            col_0 = sg.Column([
                                [sg.Text("WAITING TO DL", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML0-', autoscroll=True, auto_refresh=True)]
            ], element_justification='l', expand_x=True, expand_y=True)
            
            col_1 = sg.Column([
                                [sg.Text("NOW DOWNLOADING/CREATING FILE", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(80, 25), font='Any 10', write_only=True, key='-ML1-', autoscroll=True, auto_refresh=True)]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            col_2 = sg.Column([
                                [sg.Text("DOWNLOADED/STOPPED/ERRORS", font='Any 14')], 
                                [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML2-', autoscroll=True, auto_refresh=True)]
            ], element_justification='r', expand_x=True, expand_y=True)
            
            layout_root = [ [col_0, col_1, col_2] ]
            
            window_root = sg.Window('async_downloader', layout_root, location=(0, 0), finalize=True, resizable=True)
            window_root.set_min_size(window_root.size)
            
            window_root['-ML0-'].expand(True, True, True)
            window_root['-ML1-'].expand(True, True, True)
            window_root['-ML2-'].expand(True, True, True)
            
            col_pygui = sg.Column([
                                    [sg.Text('Select DL', font='Any 14')],
                                    [sg.Input(key='-IN-', font='Any 10', focus=True)],
                                    [sg.Multiline(size=(50, 12), font='Any 10', write_only=True, key='-ML-', reroute_cprint=True, auto_refresh=True, autoscroll=True)],
                                    [sg.Checkbox('PasRes', key='-PASRES-', default=False, enable_events=True), sg.Checkbox('WkInit', key='-WKINIT-', default=True, enable_events=True), sg.Button('DLStatus', key='-DL-STATUS'), sg.Button('Info'), sg.Button('ToFile'), sg.Button('Pause'), sg.Button('Resume'), sg.Button('Reset'), sg.Button('Stop'), sg.Button('Exit')]
            ], element_justification='c', expand_x=True, expand_y=True)
            
            layout_pygui = [ [col_pygui] ]

            window_console = sg.Window('Console', layout_pygui, location=(0, 500), finalize=True, resizable=True)
            window_console.set_min_size(window_console.size)
            window_console['-ML-'].expand(True, True, True)
            
            window_console.bring_to_front()
            
            return(window_root, window_console)
        
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