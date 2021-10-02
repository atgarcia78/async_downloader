#!/usr/bin/env python
from concurrent.futures.thread import ThreadPoolExecutor
from distutils.log import ERROR
import logging
import logging.config
import json
from multiprocessing.pool import INIT
from pathlib import Path
from h11 import DONE
from yt_dlp import YoutubeDL
from yt_dlp.utils import std_headers
from yt_dlp.extractor import gen_extractors
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


def kill_processes(logger):
    res = subprocess.run(["ps","-o","pid","-o","comm"], encoding='utf-8', capture_output=True).stdout
    mobj = re.findall(r'(\d+) ((?:aria2c|browsermob|geckodriver|java|/Applications/Firefox Nightly))', res)
    if mobj:
        for process in mobj:                    
            res = subprocess.run(["kill","-9",process[0]], encoding='utf-8', capture_output=True)
            if res.returncode != 0: logger.debug(f"cant kill {process[0]} : {process[1]} : {res.stderr}")
            else: logger.debug(f"killed {process[0]} : {process[1]}")

async def wait_time(n):
    _timer = httpx._utils.Timer()
    await _timer.async_start()
    while True:
        _t = await _timer.async_elapsed()
        if _t > n: break
        else: await asyncio.sleep(0)

def get_extractor(url, ytdl):
    
    extractor = None
    ies = ytdl._ies
    for ie_key, ie in ies.items():
        if ie.suitable(url):
            extractor = ie_key
            break
    return extractor

def is_playlist(url, ytdl):    
        
    ies = ytdl._ies 
    for ie_key, ie in ies.items():
        if ie.suitable(url):
            extractor = ie_key
            name = getattr(ie, 'IE_NAME', '')
            return ("playlist" in extractor.lower() or "playlist" in name.lower(), extractor)
       
    # matching_ies = [(_name, ie) for ie_key, ie in ies.items() if ie.suitable(url) and (_name:=(getattr(ie,'IE_NAME','') or ie_key).lower()) != 'generic']
    
    # if matching_ies:
    #     ie_name, ie = matching_ies[0]
    #     if 'playlist' in ie_name: return (True, ie.ie_key())
    #     for tc in ie.get_testcases():
    #         if tc.get('playlist'):
    #             return (True, ie.ie_key())
    # return (False, "")

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
    

def naturalsize(value, binary=False, gnu=False, format="%.2f"):
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
        "binary": ("KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"),
        "gnu": "KMGTPEZY",
    }
    
    if gnu:
        suffix = SUFFIXES["gnu"]
    elif binary:
        suffix = SUFFIXES["binary"]
    else:
        suffix = SUFFIXES["decimal"]

    base = 1024 if (gnu or binary) else 1000
    bytes = float(value)
    abs_bytes = abs(bytes)

    if abs_bytes == 1 and not gnu:
        return "%d Byte" % bytes
    elif abs_bytes < base and not gnu:
        return "%d Bytes" % bytes
    elif abs_bytes < base and gnu:
        return "%dB" % bytes

    for i, s in enumerate(suffix):
        unit = base ** (i + 2)
        if abs_bytes < unit and not gnu:
            return (format + " %s") % ((base * bytes / unit), s)
        elif abs_bytes < unit and gnu:
            return (format + "%s") % ((base * bytes / unit), s)
    if gnu:
        return (format + "%s") % ((base * bytes / unit), s)
    return (format + " %s") % ((base * bytes / unit), s)



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
    
   # IPS_TORGUARD = ["82.129.66.196"]
    
    PORTS = [6060,1337,1338,1339,1340,1341,1342,1343]

    queue_ok = Queue()
    

    
    futures = []
    
    _port = random.choice(PORTS)
    
    with ThreadPoolExecutor(max_workers=8) as ex:
        for proxy in IPS_TORGUARD:            
            futures.append(ex.submit(check_proxy, proxy, _port, queue_ok))
        
    
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
            futures.append(ex.submit(_get_rtt, ipl, _port))
        
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
    
    # UA_LIST = ["Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0", "Mozilla/5.0 (Android 11; Mobile; rv:88.0) Gecko/88.0 Firefox/88.0", "Mozilla/5.0 (iPad; CPU OS 10_15_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/24.1 Mobile/15E148 Safari/605.1.15", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:85.0) Gecko/20100101 Firefox/85.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:79.0) Gecko/20100101 Firefox/79.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:88.0) Gecko/20100101 Firefox/88.0"]
    #UA_LIST = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:85.0) Gecko/20100101 Firefox/85.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:79.0) Gecko/20100101 Firefox/79.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:88.0) Gecko/20100101 Firefox/88.0"]
    
    UA_LIST = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:94.0) Gecko/20100101 Firefox/94.0"]


    parser = argparse.ArgumentParser(description="Async downloader videos / playlist videos HLS / HTTP")
    parser.add_argument("-w", help="Number of workers", default="10", type=int)
    parser.add_argument("--winit", help="Number of init workers", default="0", type=int)
    parser.add_argument("-p", help="Number of parts", default="16", type=int)
    parser.add_argument("--format", help="Format preferred of the video in youtube-dl format", default="bestvideo+bestaudio/best", type=str)
    parser.add_argument("--index", help="index of a video in a playlist", default=None, type=int)
    parser.add_argument("--file", help="jsonfiles", action="append", dest="collection_files", default=[])
    parser.add_argument("--nocheckcert", help="nocheckcertificate", action="store_true")
    parser.add_argument("--ytdlopts", help="init dict de conf", type=str)
    parser.add_argument("--proxy", default=None, type=str)
    parser.add_argument("--useragent", default=random.choice(UA_LIST), type=str)
    parser.add_argument("--first", default=None, type=int)
    parser.add_argument("--last", default=None, type=int)
    parser.add_argument("--nodl", help="not download", action="store_true")   
    parser.add_argument("--headers", default="", type=str)  
    parser.add_argument("-u", action="append", dest="collection", default=[])
    parser.add_argument("--byfilesize", help="order list of videos to dl by filesize", action="store_true")  
    parser.add_argument("--lastres", help="use last result for get videos list", action="store_true")
    parser.add_argument("--nodlcaching", help="dont get new cache videos dl, use previous", action="store_true")
    parser.add_argument("--path", default=None, type=str)    
    parser.add_argument("--caplinks", action="store_true")    
    parser.add_argument("--aria2c", action="store_true")
    parser.add_argument("-v", "--verbose", help="verbose", action="store_true")
    
    
    return parser.parse_args()


def init_ytdl(args):


    logger = logging.getLogger("youtube_dl")

    ytdl_opts = {        
        "continue_dl": True,
        "updatetime": False,
        "ignoreerrors": False,
        "verbose": args.verbose,
        "quiet": False,
        "extract_flat": "in_playlist",        
        "format" : args.format,
        "usenetrc": True,
        "skip_download": True,        
        "logger" : logger,        
        "nocheckcertificate" : args.nocheckcert,
        "writesubtitles": True,
        "subtitleslangs": ['en','es'],
        "restrictfilenames": True,
        "winit" : args.winit if args.winit > 0 else args.w,
          
    }

    if args.proxy: ytdl_opts['proxy'] = args.proxy
    if args.ytdlopts: ytdl_opts.update(demjson.decode(args.ytdlopts))
    logger.debug(f"ytdl opts: \{ytdl_opts}")
    
    # ytdl = YoutubeDL(ytdl_opts, auto_init=False)
    # ytdl.add_default_info_extractors()
    
    ytdl = YoutubeDL(ytdl_opts)

    std_headers["User-Agent"] = args.useragent
    std_headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
   
    std_headers["Connection"] = "keep-alive"
    std_headers["Accept-Language"] = "en-US;q=0.7,en;q=0.3"
    std_headers["Accept-Encoding"] = "gzip, deflate"
    if args.headers:
        std_headers.update(demjson.decode(args.headers))
       
        
    logger.debug(f"std-headers: {std_headers}")
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

def init_tk_afiles(n_files):
    window = tk.Tk()
    window.title("async_files")
    
    frame0 = tk.Frame(master=window, width=300, height=25*n_files, bg="white")
  
    frame0.pack(fill=tk.BOTH, side=tk.LEFT, expand=True) 
    
    frame1 = tk.Frame(master=window, width=300, bg="white")
  
    frame1.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)    
    
    frame2 = tk.Frame(master=window, width=300, bg="white")
   
    frame2.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
    label0 = tk.Label(master=frame0, text="WAITING TO ENTER IN POOL", bg="blue")
    label0.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text0 = tk.Text(master=frame0, font=("Source Code Pro", 9))
    text0.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
    label1 = tk.Label(master=frame1, text="NOW RUNNING", bg="blue")
    label1.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text1 = tk.Text(master=frame1, font=("Source Code Pro", 9))
    text1.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
    label2 = tk.Label(master=frame2, text="DONE/ERRORS", bg="blue")
    label2.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text2 = tk.Text(master=frame2, font=("Source Code Pro", 9))
    text2.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)   
    
    text0.insert(tk.END, "Waiting for info") 
    text1.insert(tk.END, "Waiting for info") 
    text2.insert(tk.END, "Waiting for info") 
    
    
           
    return(window, text0, text1, text2)
    


    
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
    
