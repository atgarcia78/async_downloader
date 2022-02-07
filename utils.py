#!/usr/bin/env python
from concurrent.futures.thread import ThreadPoolExecutor
import logging
import logging.config
import json
from pathlib import Path
from yt_dlp import YoutubeDL
from yt_dlp.utils import (
    js_to_json)
import random
import httpx
from pathlib import Path
import re
import argparse

from queue import Queue
import subprocess
import shutil

import aria2p
import time
import asyncio
import contextvars
import functools
import PySimpleGUI as sg
import collections
 
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

async def async_ex_in_executor(executor, func, /, *args, **kwargs):
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)        
    return await loop.run_in_executor(executor, func_call)
    
async def async_ex_in_thread(prefix, func, /, *args, **kwargs):
        
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    ex = ThreadPoolExecutor(thread_name_prefix=prefix)    
    return await loop.run_in_executor(ex, func_call)
    #return await asyncio.to_thread(func_call)

async def async_wait_time(n):
   
    _started = time.monotonic()
    while True:
        if (_t:=(time.monotonic() - _started)) >= n:
            return _t
        else:
            await asyncio.sleep(0)

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
    mobj = re.findall(rf'(\d+)\s+(?:\?\?|{term})\s+((?:.+browsermob-proxy --port.+|{_aria2cstr}|geckodriver.+|java -Dapp.name=browsermob-proxy.+|/Applications/Firefox Nightly.app/Contents/MacOS/firefox-bin.+))', res)
    mobj2 = re.findall(r'/Applications/Firefox Nightly.app/Contents/MacOS/firefox-bin.+--profile (/var/folders/[^\ ]+) ', res)
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
            
def get_extractor(url, ytdl):
    
    ies = ytdl._ies   
    for ie_key, ie in ies.items():
        if ie.suitable(url) and (ie_key != 'Generic'):
            return (ie_key, ie)                
    return('Generic', ies['Generic'])
    
def is_playlist_extractor(url, ytdl):    
        
    ie_key, ie = get_extractor(url, ytdl)
    
    if ie_key == 'Generic':
        return(False, 'Generic')   
        
    ie_name = _iename.lower() if type(_iename:=getattr(ie, 'IE_NAME', '')) is str else ""
    
    ie_tests = str(getattr(ie, '_TESTS', ''))
    
    _is_pl = any("playlist" in _ for _ in [ie_key.lower(), ie_name, ie_tests])
    
    return(_is_pl, ie_key)

def foldersize(folder):
    #devuelve en bytes size folder
    return sum(file.stat().st_size for file in Path(folder).rglob('*') if file.is_file())

def folderfiles(folder):
    count = 0
    for file in Path(folder).rglob('*'):
        if file.is_file(): count += 1
        
    return count

def variadic(x, allowed_types=(str, bytes, dict)):
    return x if isinstance(x, collections.abc.Iterable) and not isinstance(x, allowed_types) else (x,)

def try_get(src, getter, expected_type=None):
    for get in variadic(getter):
        try:
            v = get(src)
        except (AttributeError, KeyError, TypeError, IndexError):
            pass
        else:
            if expected_type is None or isinstance(v, expected_type):
                return v 

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

def init_logging(file_path=None):

    if not file_path:
        config_file = Path(Path.home(), "Projects/common/logging.json")
    else:
        config_file = Path(file_path)
    
    with open(config_file) as f:
        config = json.loads(f.read())
    
    config['handlers']['info_file_handler']['filename'] = config['handlers']['info_file_handler']['filename'].format(home = str(Path.home()))
    
    logging.config.dictConfig(config)   

def init_argparser():
    
 
    UA_LIST = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:98.0) Gecko/20100101 Firefox/98.0"]


    parser = argparse.ArgumentParser(description="Async downloader videos / playlist videos HLS / HTTP")
    parser.add_argument("-w", help="Number of DL workers", default="10", type=int)
    parser.add_argument("--winit", help="Number of init workers, default is same number for DL workers", default="0", type=int)
    parser.add_argument("-p", "--parts", help="Number of workers for each DL", default="16", type=int)
    parser.add_argument("--format", help="Format preferred of the video in youtube-dl format", default="bv*+ba/b", type=str)
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
    parser.add_argument("-q", "--quiet", help="quiet", action="store_true", default=False)
    parser.add_argument("--aria2c", help="use of external aria2c running in port [PORT]. By default PORT=6800", default=-1, nargs='?', type=int)
    parser.add_argument("--notaria2c", help="force to not use aria2c", action="store_true", default=False)
    parser.add_argument("--nosymlinks", action="store_true", default=False)
    
    
    
    args = parser.parse_args()
    if args.winit == 0:
        args.winit = args.w
    if args.aria2c != -1:
        args.rpcport = args.aria2c if args.aria2c else 6800
        args.aria2c = True
    else: 
        args.rpcport = None
        args.aria2c = False
        
    if args.notaria2c:
        args.aria2c = False 
        args.rpcport = None
        
    if args.path and len(args.path.split("/")) == 1:
        _path = Path(Path.home(),"testing", args.path)
        args.path = str(_path)
    
    
    return args

def init_aria2c(args):
    
    logger = logging.getLogger("asyncDL")
    subprocess.run(["aria2c","--rpc-listen-port",f"{args.rpcport}", "--enable-rpc","--daemon"])
    logger.info(f"aria2c daemon running on port: {args.rpcport} ")
    cl = aria2p.API(aria2p.Client(port=args.rpcport))
    opts = cl.get_global_options()
    logger.debug(f"aria2c options:\n{opts._struct}")
    del opts
    del cl
    
    
INFO = 20
DEBUG = 10
class MyLogger(logging.LoggerAdapter):
    #para ser compatible con el logging de yt_dlp: yt_dlp iusea debug para enviar los debig y
    #los info. Los debug llevan '[debug] ' antes.
    #se pasa un logger de logging al crear la instancia 
    # mylogger = MyLogger(logging.getLogger("name_ejemplo", {}))
    
    def __init__(self, logger, quiet=False):
        super().__init__(logger, {})
        self.quiet = quiet
    
    def debug(self, msg, *args, **kwargs):
        if self.quiet:
            self.log(DEBUG, msg, *args, **kwargs)
        else:    
            mobj = get_values_regex([r'^(\[[^\]]+\])'], msg)
            if mobj in ('[debug]', '[download]'):
                self.log(DEBUG, msg[len(mobj):].strip(), *args, **kwargs)
            else: self.log(INFO, msg, *args, **kwargs)
        
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
                               
    ytdl_opts = {
        "proxy" : proxy,        
        "logger" : MyLogger(logger, args.quiet),
        "verbose": args.verbose,
        "quiet": args.quiet,
        "format" : args.format,
        "nocheckcertificate" : args.nocheckcert,
        "subtitleslangs": ['en','es'],
        "continuedl": True,
        "updatetime": False,
        "ignoreerrors": False,        
        "extract_flat": "in_playlist",        
        "no_color" : True,
        "usenetrc": True,
        "skip_download": True,        
        "writesubtitles": True,        
        "restrictfilenames": True,
        "user_agent": args.useragent,
        "winit": args.winit
                  
    }
    
    if args.ytdlopts: ytdl_opts.update(json.loads(js_to_json(args.ytdlopts)))
        
    ytdl = YoutubeDL(ytdl_opts)
    
    logger.info(f"ytdl opts:\n{ytdl.params}")   
    
    return ytdl

def init_gui():
   
    sg.theme("SystemDefaultForReal")
    
    col_0 = sg.Column([
                        [sg.Text("WAITING TO DL", font='Any 14')], 
                        [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML0-', auto_refresh=True)]
    ], element_justification='l', expand_x=True, expand_y=True)
    
    col_1 = sg.Column([
                        [sg.Text("NOW DOWNLOADING/CREATING FILE", font='Any 14')], 
                        [sg.Multiline(default_text = "Waiting for info", size=(80, 25), font='Any 10', write_only=True, key='-ML1-', auto_refresh=True)]
    ], element_justification='c', expand_x=True, expand_y=True)
    
    col_2 = sg.Column([
                        [sg.Text("DOWNLOADED/STOPPED/ERRORS", font='Any 14')], 
                        [sg.Multiline(default_text = "Waiting for info", size=(50, 25), font='Any 10', write_only=True, key='-ML2-', auto_refresh=True)]
    ], element_justification='r', expand_x=True, expand_y=True)
    
    layout_root = [ [col_0, col_1, col_2] ]
    
    window_root = sg.Window('async_downloader', layout_root, location=(0, 0), finalize=True, resizable=True, use_default_focus=False)
    window_root.set_min_size(window_root.size)
    
    window_root['-ML0-'].expand(True, True, True)
    window_root['-ML1-'].expand(True, True, True)
    window_root['-ML2-'].expand(True, True, True)
    
    layout_pygui = [  [sg.Text('Select DL')],
                [sg.Input(key='-IN-', focus=True)],
                [sg.Multiline(size=(30, 8), write_only=True, key='-ML-', reroute_cprint=True)],
                [sg.Button('Pause'), sg.Button('Resume'), sg.Button('Reset'), sg.Button('Stop'), sg.Button('Exit')] ]

    window_pygui = sg.Window('Console', layout_pygui, location=(0, 350), finalize=True, use_default_focus=True)
    
    window_pygui.bring_to_front()
    
    return(window_root, window_pygui)


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
    
def check_proxy(ip, port, queue_ok=None):
    try:
        cl = httpx.Client(proxies=f"http://atgarcia:ID4KrSc6mo6aiy8@{ip}:{port}",timeout=10, follow_redirects=True)
        res = cl.get("https://torguard.net/whats-my-ip.php")            
        print(f"{ip}:{port}:{res}")
        if res.status_code == 200:
            if queue_ok:
                queue_ok.put((ip, port, res))
    except Exception as e:
        print(f"{ip}:{port}:{e}")
    finally:
        cl.close()

def status_proxy():
    
    #dscacheutil -q host -a name proxy.torguard.org

    
    IPS_ES_SSL = ["192.145.124.186", "192.145.124.234", "89.238.178.234", "192.145.124.242", "192.145.124.226", "192.145.124.238", "192.145.124.174", "89.238.178.206", "192.145.124.190"]
    
   # IPS_TORGUARD = ["82.129.66.196"]
    
    PORTS = [6060,1337,1338,1339,1340,1341,1342,1343]
    PORTS_SSL = [489, 23, 7070, 465, 993, 282, 778, 592]

    queue_ok = Queue()
    

    
    futures = []
    
    
    
    with ThreadPoolExecutor(max_workers=8) as ex:
        for ip in IPS_ES_SSL: 
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

 