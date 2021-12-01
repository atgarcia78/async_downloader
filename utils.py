#!/usr/bin/env python
from concurrent.futures.thread import ThreadPoolExecutor
import logging
import logging.config
import json
from pathlib import Path
from yt_dlp import YoutubeDL
from yt_dlp.utils import (
    std_headers, 
    )
import random
import httpx
from pathlib import Path
import re
import argparse
import tkinter as tk

import demjson
from queue import Queue
import subprocess
import asyncio
import shutil

import time
from tqdm import tqdm
import aria2p
 
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

def none_to_cero(item):
    return(item if item else 0)

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
            
        logger.debug("[kill_processes]\n" + '\n'.join(_debugstr))
            
    
    else: 
        logger.debug("[kill_processes] No processes found to kill") if logger else print("[kill_processes] No processes found to kill")
        
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

def get_el_list(_list: list, _index: int):
   return _list[_index] if _index < len(_list) else None 

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



def get_ip_proxy():
    with open(Path(Path.home(),"Projects/common/ipproxies.json"), "r") as f:
        return(random.choice(json.load(f)))
    
def check_proxy(ip, port, queue_ok=None):
    try:
        cl = httpx.Client(proxies=f"http://atgarcia:ID4KrSc6mo6aiy8@{ip}:{port}",timeout=10)
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
    IPS_TORGUARD = ["88.202.177.231", "68.71.244.82", "88.202.177.233", "68.71.244.70", "68.71.244.102", "68.71.244.94", "37.120.153.242", "68.71.244.22", "88.202.177.230", "89.238.177.198", "37.120.244.230", "88.202.177.239", "68.71.244.30", "37.120.244.194", "46.23.78.25", "185.212.171.114", "68.71.244.50", "88.202.177.240", "68.71.244.18", "68.71.244.62", "37.120.244.198", "68.71.244.34", "68.71.244.78", "194.59.250.250", "68.71.244.42", "194.59.250.242", "194.59.250.226", "37.120.244.214", "37.120.244.202", "194.59.250.202", "88.202.177.243", "68.71.244.6", "88.202.177.235", "37.120.141.122", "89.238.177.194", "68.71.244.66", "88.202.177.237", "68.71.244.26", "185.212.171.118", "68.71.244.10", "68.71.244.98", "37.120.244.206", "88.202.177.238", "37.120.153.234", "68.71.244.90", "37.120.244.226", "68.71.244.54", "194.59.250.210", "185.156.172.198", "37.120.244.222", "68.71.244.46", "68.71.244.38", "37.120.141.114", "37.120.244.210", "185.156.172.154", "88.202.177.242", "37.120.244.218", "194.59.250.234", "89.238.177.202", "88.202.177.232", "88.202.177.241", "88.202.177.234", "68.71.244.58", "46.23.78.24", "194.59.250.218", "68.71.244.14", "194.59.250.194", "2.58.44.226"]
    
    IPS_ES_SSL = ["192.145.124.186", "192.145.124.130", "192.145.124.234", "89.238.178.234", "192.145.124.242", "192.145.124.226", "192.145.124.238", "192.145.124.174", "89.238.178.206", "192.145.124.190"]
    
   # IPS_TORGUARD = ["82.129.66.196"]
    
    PORTS = [6060,1337,1338,1339,1340,1341,1342,1343]
    PORTS_SSL = [489, 23, 7070, 465, 993, 282, 778, 592]

    queue_ok = Queue()
    

    
    futures = []
    
    
    
    with ThreadPoolExecutor(max_workers=8) as ex:
        for proxy in IPS_ES_SSL: 
            for port in PORTS:            
                futures.append(ex.submit(check_proxy, proxy, port, queue_ok))
        
    
    list_res = list(queue_ok.queue)
    
    list_ok = [res[0] for res in list_res]
    
    queue_rtt = Queue() 
    
    def _get_rtt(ip, port):
        res = subprocess.run(["ping","-c","5","-q","-S","192.168.1.128", ip], encoding='utf-8', capture_output=True).stdout
        mobj = re.findall(r'= [^\/]+\/([^\/]+)\/', res)
        if mobj: _tavg = float(mobj[0])
        print(f"{ipl}:{_tavg}")
        queue_rtt.put({'ip': f'{ip}:{port}', 'time': _tavg})
         
    futures = []
    
    with ThreadPoolExecutor(max_workers=8) as ex: 
        for ipl in list_ok:
            futures.append(ex.submit(_get_rtt, ipl, port))
        
    list_ord = list(queue_rtt.queue)

    def myFunc(e):
        return(e['time'])
    
    list_ord.sort(key=myFunc)
    
    with open(Path(Path.home(),"Projects/common/ipproxies.json"), "w") as f:
        f.write(json.dumps(list_ord))
    
    print(list_ord)
    return(list_ord)

 
    
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
    
 
    UA_LIST = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:96.0) Gecko/20100101 Firefox/96.0"]


    parser = argparse.ArgumentParser(description="Async downloader videos / playlist videos HLS / HTTP")
    parser.add_argument("-w", help="Number of DLs", default="10", type=int)
    parser.add_argument("--winit", help="Number of init workers", default="0", type=int)
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
    parser.add_argument("--byfilesize", help="order list of videos to dl by filesize", action="store_true")  
    parser.add_argument("--nodlcaching", help="dont get new cache videos dl, use previous", action="store_true", default=False)
    parser.add_argument("--path", default=None, type=str)    
    parser.add_argument("--caplinks", action="store_true", default=False)    
    parser.add_argument("-v", "--verbose", help="verbose", action="store_true", default=False)
    parser.add_argument("-q", "--quiet", help="quiet", action="store_true", default=False)
    parser.add_argument("--aria2c", help="use of external aria2c running in port [PORT]. By default PORT=6800", default=-1, nargs='?', type=int)
    parser.add_argument("--notaria2c", help="force to not use aria2c", action="store_true", default=False)
    parser.add_argument("--nosymlinks", action="store_true", default=False)
    
    
    
    args = parser.parse_args()
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
    cl = aria2p.API(aria2p.Client(port=args.rpcport))
    opts = cl.get_global_options()
    opts_dict = {
                'check-certificate': not args.nocheckcert,
                'connect-timeout': '10',
                'timeout': '10',
                'max-tries': '2',
                'user-agent': std_headers['User-Agent']}
    for key,value in opts_dict.items():
            opts.set(key, value)
    
    dict_opts = opts._struct
    del dict_opts['dir']
    logger.info(f"aria2c options:\n{dict_opts}")
    del cl
    
    

def init_ytdl(args):


    logger = logging.getLogger("asyncDL")

    ytdl_opts = {        
        "continuedl": True,
        "updatetime": False,
        "ignoreerrors": False,
        "verbose": args.verbose,
        "quiet": args.quiet,
        "extract_flat": "in_playlist",        
        "format" : args.format,
        "no_color" : True,
        "usenetrc": True,
        "skip_download": True,        
        "logger" : logger,        
        "nocheckcertificate" : args.nocheckcert,
        "writesubtitles": True,
        "subtitleslangs": ['en','es'],
        "restrictfilenames": True,
        "winit" : args.winit if args.winit > 0 else args.w,
          
    }

    if args.proxy:
        proxy = None
        sch = args.proxy.split("://")
        if len(sch) == 2:
            if sch[0] != 'http':
                logger.error("Proxy is not valid, should be http")
            else: proxy = args.proxy
        else:
            proxy = f"http://{args.proxy}"
        
        if proxy:
            ytdl_opts['proxy'] = proxy

    if args.ytdlopts: ytdl_opts.update(demjson.decode(args.ytdlopts))
    
    
    # ytdl = YoutubeDL(ytdl_opts, auto_init=False)
    # ytdl.add_default_info_extractors()
    
    ytdl = YoutubeDL(ytdl_opts)
    
    logger.info(f"ytdl opts:\n{ytdl.params}")
   

    std_headers["User-Agent"] = args.useragent
    std_headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8" 
    std_headers["Connection"] = "keep-alive"
    std_headers["Accept-Language"] = "en,es-ES;q=0.5"
    std_headers["Accept-Encoding"] = "gzip, deflate"
    if args.headers:
        std_headers.update(demjson.decode(args.headers))
       
        
    logger.debug(f"std-headers:\n{std_headers}")
    return ytdl

def init_tk():
    window = tk.Tk()
    window.title("async_downloader")
    
    
    frame0 = tk.Frame(master=window, width=25, height=50, bg="white")  
    frame0.pack(fill=tk.BOTH, side=tk.LEFT, expand=True) 
    frame1 = tk.Frame(master=window, width=50, bg="white")
    frame1.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)  
    frame2 = tk.Frame(master=window, width=25, bg="white")
    frame2.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
        
    label0 = tk.Label(master=frame0, text="WAITING TO DL", bg="blue")
    label0.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text0 = tk.Text(master=frame0, font=("Source Code Pro", 10))
    text0.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)    
   
    
    label1 = tk.Label(master=frame1, text="NOW DOWNLOADING/CREATING FILE", bg="blue")
    label1.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text1 = tk.Text(master=frame1, font=("Source Code Pro", 10))
    text1.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
        
    label2 = tk.Label(master=frame2, text="DOWNLOADED/ERRROS", bg="blue")
    label2.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text2 = tk.Text(master=frame2, font=("Source Code Pro", 10))
    text2.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)   
    
    text0.insert(tk.END, "Waiting for info")     
    text1.insert(tk.END, "Waiting for info") 
    text2.insert(tk.END, "Waiting for info")
    
    res = [window, text0, text1, text2]
    return(res)    


    
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
    
