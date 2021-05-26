#!/usr/bin/env python
import logging
import logging.config
import json
from pathlib import Path
import asyncio
from datetime import datetime
from youtube_dl import YoutubeDL
from youtube_dl.utils import (
    std_headers,
    determine_protocol
)
import random
import httpx
from pathlib import Path
import re
import argparse
import tkinter as tk



def shorter_str(msg, nchars):
    if len(msg) < nchars: return msg
    else: return msg[:nchars] 

def foldersize(folder):
    #devuelve en bytes size folder
    return sum(file.stat().st_size for file in Path(folder).rglob('*') if file.is_file())

def folderfiles(folder):
    count = 0
    for file in Path(folder).rglob('*'):
        if file.is_file(): count += 1
        
    return count

"""Bits and bytes related humanization."""

SUFFIXES = {
    "decimal": ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"),
    "binary": ("KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"),
    "gnu": "KMGTPEZY",
}

def naturalsize(value, binary=False, gnu=False, format="%.4f"):
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

def get_value_regex(value, str_reg, str_content, not_found):
    mobj = re.search(str_reg, str_content)
    if not mobj:
        return not_found
    else:
        res = mobj.group(value)
        if res:
            return res
        else:
            return not_found




def generate_random_ua():
    cl = httpx.Client()
    list_el = []
    for i in range(25):
        list_el += re.find(cl.get("https://generate-name.net/user-agent").text)

IPS_TORGUARD = ["88.202.177.243","96.44.144.122","194.59.250.210","88.202.177.234","68.71.244.42","194.59.250.226","173.254.222.146","68.71.244.98","88.202.177.241","185.212.171.118","98.143.158.50","88.202.177.242","173.44.37.82","194.59.250.242","194.59.250.202","68.71.244.66","173.44.37.114","2.58.44.226","88.202.177.240","96.44.148.66","37.120.153.242","185.156.172.154","68.71.244.30","46.23.78.24","88.202.177.238","88.202.177.239","68.71.244.38","194.59.250.218", "88.202.177.230"]

def get_ip_proxy():
    with open(Path(Path.home(),"Projects/common/ipproxies.json"), "r") as f:
        return(random.choice(json.load(f)))

def status_proxy():

    from scapy.all import sr,IP,ICMP
    list_ok = []
    for proxy in IPS_TORGUARD:
        try:
            cl = httpx.Client(proxies=f"http://atgarcia:ID4KrSc6mo6aiy8@{proxy}:6060")
            res = cl.get("https://torguard.net/whats-my-ip.php")            
            print(f"{proxy}:{res}")
            if res.status_code == 200:
                list_ok.append(proxy)
        except Exception as e:
            pass
            print(f"{proxy}:{e}")

    
        
    list_ord = []
    for ipl in list_ok:
        ans, unans = sr([IP(dst=ipl)/ICMP()/b"Sent" for i in range(5)])
        t = [(a[1].time - a[0].sent_time)*1000 for a in ans]
        t.sort()
        tmed = sum(t[1:-1])/3
        print(f"{ipl}:{t}:{tmed}")
        list_ord.append({'ip': ipl, 'time': tmed})

    def myFunc(e):
        return(e['time'])
    
    list_ord.sort(key=myFunc)
    
    with open(Path(Path.home(),"Projects/common/ipproxies.json"), "w") as f:
        f.write(json.dumps(list_ord))
    

    return(list_ord)

# def init_ffprofiles_file():
#     with open(Path(Path.home(), "testing/firefoxprofiles.json"), "r") as f:
#         ffprofiles_dict = json.loads(f.read())
        
#     for prof in ffprofiles_dict['profiles']:
#         prof['count'] = 0
    
#     with open(Path(Path.home(), "testing/firefoxprofiles.json"), "w") as f:
#         json.dump(ffprofiles_dict, f)


 
    
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

    parser = argparse.ArgumentParser(description="Async downloader videos / playlist videos HLS / HTTP")
    parser.add_argument("-w", help="Number of workers", default="10", type=int)
    parser.add_argument("-p", help="Number of parts", default="16", type=int)
    parser.add_argument("--format", help="Format preferred of the video in youtube-dl format", default="bestvideo+bestaudio/best", type=str)
    parser.add_argument("--playlist", help="URL should be trreated as a playlist", action="store_true") 
    parser.add_argument("--index", help="index of a video in a playlist", default=None, type=int)
    parser.add_argument("--file", help="jsonfiles", action="append", dest="collection_files", default=[])
    parser.add_argument("--nocheckcert", help="nocheckcertificate", action="store_true")
    parser.add_argument("--ytdlopts", help="init dict de conf", type=str)
    parser.add_argument("--proxy", default=None, type=str)
    parser.add_argument("--useragent", default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.5; rv:88.0) Gecko/20100101 Firefox/88.0", type=str)
    parser.add_argument("--first", default=None, type=int)
    parser.add_argument("--last", default=None, type=int)
    parser.add_argument("--nodl", help="not download", action="store_true")
    parser.add_argument("--nomult", help="init not concurrent", action="store_true")
    parser.add_argument("--referer", default=None, type=str)
    parser.add_argument("--listvideos", help="get list videos. no dl", action="store_true")
    
    #parser.add_argument("target", help="Source(s) to download the video(s), either from URLs of JSON YTDL file (with --file option)")
    parser.add_argument("-u", action="append", dest="collection", default=[])
    parser.add_argument("--byfilesize", help="order list of videos to dl by filesize", action="store_true")
    
    parser.add_argument("--minsize", default=None, type=str)
    parser.add_argument("--maxsize", default=None, type=str)
    parser.add_argument("--name", action="append", dest="col_names", default=[])
    parser.add_argument("--lastres", help="use last result for get videos list", action="store_true")
    parser.add_argument("--nodlcaching", help="dont get new cache videos dl, use previous", action="store_true")
    parser.add_argument("--path", default=None, type=str)
    
    
    

    return parser.parse_args()


def init_ytdl(dict_opts, uagent, referer):


    logger = logging.getLogger("youtube_dl")

    ytdl_opts = {
        #"debug_printtraffic": True,
        "continue_dl": True,
        "updatetime": False,
        "ignoreerrors": True,
        "verbose": True,
        "quiet": False,
        "extract_flat": "in_playlist",
        #"outtmpl": outtmpl,
        "format" : "best",
        "usenetrc": True,
        "skip_download": True,
        #"forcejson": True,
        #"dump_single_json" : True,
        "logger" : logger,
        #"proxy" : "192.168.1.139:5555",
        "nocheckcertificate" : False   
    }

    ytdl_opts.update(dict_opts)
    ytdl = YoutubeDL(ytdl_opts, auto_init=False)
    ytdl.add_default_info_extractors()

    std_headers["User-Agent"] = uagent
    if referer:
        std_headers["Referer"] = referer
        std_headers["Origin"] = referer
        std_headers["Accept"] = "*/*"
    
    logger.debug(f"std-headers: {std_headers}")
    return ytdl

def init_tk(n_dl):
    window = tk.Tk()
    window.title("async_downloader")
    #window.geometry('{}x{}'.format(500, 25*n_dl))
    
    frame0 = tk.Frame(master=window, width=300, height=25*n_dl, bg="white")
  
    frame0.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)    
    
    
    
    frame4 = tk.Frame(master=window, width=300, bg="white")
   
    frame4.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
    frame1 = tk.Frame(master=window, width=300, bg="white")
  
    frame1.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)    
    
    frame2 = tk.Frame(master=window, width=300, bg="white")
   
    frame2.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
    
    
    
    
    label0 = tk.Label(master=frame0, text="WAITING TO DL", bg="blue")
    label0.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text0 = tk.Text(master=frame0, font=("Source Code Pro", 9))
    text0.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
    label4 = tk.Label(master=frame4, text="WAITING TO CREATE FILE", bg="blue")
    label4.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text4 = tk.Text(master=frame4, font=("Source Code Pro", 9))
    text4.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
    label1 = tk.Label(master=frame1, text="NOW DOWNLOADING/CREATING FILE", bg="blue")
    label1.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text1 = tk.Text(master=frame1, font=("Source Code Pro", 9))
    text1.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    
    label2 = tk.Label(master=frame2, text="DOWNLOADED/ERRROS", bg="blue")
    label2.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text2 = tk.Text(master=frame2, font=("Source Code Pro", 9))
    text2.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)   
    
    text0.insert(tk.END, "Waiting for info") 
    text4.insert(tk.END, "Waiting for info")
    text1.insert(tk.END, "Waiting for info") 
    text2.insert(tk.END, "Waiting for info")
    
    
    window2 = tk.Tk()
    window2.title("async_downloader")  
    frame3 = tk.Frame(master=window2, width=25, height=25, bg="white")
    frame3.pack(fill=tk.BOTH, side=tk.TOP, expand=True)
    label3 = tk.Label(master=frame3, text="Total bytes to DL", bg="blue")
    label3.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    text3 = tk.Text(master=frame3, font=("Source Code Pro", 9))
    text3.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    # frame3 = tk.Frame(master=window2, width=300, height=25, bg="white")
    # frame3.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    # label3 = tk.Label(master=frame3, text="NUMBER OF TASKS", bg="blue")
    # label3.pack(fill=tk.BOTH, side=tk.TOP, expand=False)
    # text3 = tk.Text(master=frame3, font=("Source Code Pro", 9))
    # text3.pack(fill=tk.BOTH, side=tk.LEFT, expand=True)
    text3.insert(tk.END, "Waiting for info")
    #text3 = None
    res = [window, text0, text4, text1, text2, window2, text3]       
    #return(window, text0, text1, text2, window2, text3)
    
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
    


       
    

def get_info_dl(info_dict):
    if info_dict.get("_type") == "playlist":
        f_info_dict = info_dict['entries'][0]
    else: f_info_dict = info_dict
    if f_info_dict.get('requested_formats'):
        protocol = determine_protocol(f_info_dict['requested_formats'][0])
        container = f_info_dict['requested_formats'][0].get('container')
        #if container and "dash" in container:
        #    protocol = "http_dash_segments"
        return(protocol, f_info_dict)
    else:
        protocol = determine_protocol(f_info_dict)
        container = f_info_dict.get('container')
        #if container and "dash" in container:
        #    protocol = "http_dash_segments"
        return (protocol, f_info_dict)
    
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
    
def int_or_none(res):
    if res: return int(res)
    else: return None