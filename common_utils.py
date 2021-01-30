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
import ssl
import argparse


SSL_PROTOCOLS = (asyncio.sslproto.SSLProtocol,)
try:
    import uvloop.loop
except ImportError:
    pass
else:
    SSL_PROTOCOLS = (*SSL_PROTOCOLS, uvloop.loop.SSLProtocol)

def ignore_aiohttp_ssl_error(loop):
    """Ignore aiohttp #3535 / cpython #13548 issue with SSL data after close

    There is an issue in Python 3.7 up to 3.7.3 that over-reports a
    ssl.SSLError fatal error (ssl.SSLError: [SSL: KRB5_S_INIT] application data
    after close notify (_ssl.c:2609)) after we are already done with the
    connection. See GitHub issues aio-libs/aiohttp#3535 and
    python/cpython#13548.

    Given a loop, this sets up an exception handler that ignores this specific
    exception, but passes everything else on to the previous exception handler
    this one replaces.

    Checks for fixed Python versions, disabling itself when running on 3.7.4+
    or 3.8.

    """
    

    orig_handler = loop.get_exception_handler()

    def ignore_ssl_error(loop, context):
        if context.get("message") in {
            "SSL error in data received",
            "Fatal error on transport",
            "application data after close notify"
        }:
            # validate we have the right exception, transport and protocol
            exception = context.get('exception')
            protocol = context.get('protocol')
            if (
                isinstance(exception, ssl.SSLError)
                and exception.reason in { 'APPLICATION_DATA_AFTER_CLOSE_NOTIFY', 'KRB5_S_INIT' }
                and isinstance(protocol, SSL_PROTOCOLS)
            ):
                if loop.get_debug():
                    asyncio.log.logger.debug(f"Ignoring asyncio  {exception.reason} error")
                return
        if orig_handler is not None:
            orig_handler(loop, context)
        else:
            loop.default_exception_handler(context)

    loop.set_exception_handler(ignore_ssl_error)


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

IPS_TORGUARD = ["88.202.177.243","96.44.144.122","194.59.250.210","88.202.177.234","68.71.244.42","194.59.250.226","173.254.222.146","68.71.244.98","88.202.177.241","185.212.171.118","98.143.158.50","88.202.177.242","173.44.37.82","194.59.250.242","194.59.250.202","68.71.244.66","173.44.37.114","2.58.44.226","88.202.177.240","96.44.148.66","37.120.153.242","185.156.172.154","68.71.244.30","46.23.78.24","88.202.177.238","88.202.177.239","68.71.244.38","194.59.250.218", "88.202.177.230"]

def get_ip_proxy():
    with open(Path(Path.home(),"testing/ipproxies.json"), "r") as f:
        return(random.choice(json.load(f)))

def status_proxy():

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

    with open(Path(Path.home(),"testing/ipproxies.json"), "w") as f:
        f.write(json.dumps(list_ok))
    list_ok = []
    for proxy in IPS_TORGUARD:
        try:
            cl = httpx.Client(proxies=f"http://atgarcia:ID4KrSc6mo6aiy8@{proxy}:6060")
            res = cl.get("https://torguard.net/whats-my-ip.php")            
            #print(f"{proxy}:{res}")
            if res.status_code == 200:
                list_ok.append(proxy)
        except Exception as e:
            pass
            #print(f"{proxy}:{e}")

    with open(Path(Path.home(),"testing/ipproxies.json"), "w") as f:
        f.write(json.dumps(list_ok))

    return(list_ok)

def init_logging(file_path=None):

    if not file_path:
        config_file = Path(Path.home(), "testing/logging.json")
    else:
        config_file = Path(file_path)
    
    with open(config_file) as f:
        config = json.loads(f.read())
    
    config['handlers']['info_file_handler']['filename'] = config['handlers']['info_file_handler']['filename'].format(home = str(Path.home()))
    config['handlers']['error_file_handler']['filename'] = config['handlers']['error_file_handler']['filename'].format(home = str(Path.home()))

    
    logging.config.dictConfig(config)   

def init_argparser():

    parser = argparse.ArgumentParser(description="Descargar playlist de videos no fragmentados")
    parser.add_argument("-w", help="Number of workers", default="10", type=int)
    parser.add_argument("-p", help="Number of parts", default="16", type=int)
    #parser.add_argument("-v", help="verbose", action="store_true")
    parser.add_argument("--format", help="Format preferred of the video in youtube-dl format", default="bestvideo+bestaudio/best", type=str)
    parser.add_argument("--playlist", help="URL should be trreated as a playlist", action="store_true") 
    parser.add_argument("--index", help="index of a video in a playlist", default="-1", type=int)
    parser.add_argument("--file", help="jsonfile", action="store_true")
    parser.add_argument("--nocheckcert", help="nocheckcertificate", action="store_true")
    parser.add_argument("--ytdlopts", help="init dict de conf", type=str)
    parser.add_argument("--proxy", default=None, type=str)
    parser.add_argument("target", help="Source(s) to download the video(s), either from URLs of JSON YTDL file (with --file option)")

    return parser.parse_args()


class TaskPool(object):

    def __init__(self, num_workers):

        self.logger = logging.getLogger("Taskpool")
        self.tasks = asyncio.Queue()
        self.n_workers = num_workers
        self.workers = []
        
    

    async def worker(self, i):
        while True:
            
            if self.tasks.empty():
                self.logger.info(f"Taskpool[{i}]:worker finds task queue empty, says bye")
                break
                
            future, task, label = await self.tasks.get()
            self.logger.info(f"Taskpool[{i}]:{label}:task created and waiting for it")
            result = await asyncio.wait_for(task, None)
            future.set_result(result)

                
    #proporciona en self.task tupla con objeto future y función 'task' que el worker transf en task
    def submit(self, task, label):
        future = asyncio.Future()
        self.tasks.put_nowait((future, task, label))
        return future

    async def join(self):
        for index in range(self.n_workers):
            worker = asyncio.create_task(self.worker(index))
            self.workers.append(worker)
        await asyncio.wait(self.workers, return_when=asyncio.ALL_COMPLETED)

def init_ytdl(dict_opts):

    fecha = (datetime.now()).strftime("%Y%m%d")
    dlpath = Path(Path.home(), "testing", fecha)
    dlpath.mkdir(parents=True, exist_ok=True)

    outtmpl = f"{str(dlpath)}/{fecha}_%(id)5s_%(title)s.%(ext)s"

    logger = logging.getLogger("_ytdl_")

    ytdl_opts = {
        #"debug_printtraffic": True,
        "continue_dl": True,
        "updatetime": False,
        "ignoreerrors": True,
        "verbose": True,
        "quiet": False,
        "extract_flat": "in_playlist",
        "outtmpl": outtmpl,
        "format" : "bestvideo+bestaudio/best",
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

    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:84.0) Gecko/20100101 Firefox/84.0"
    #user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36"
    std_headers["User-Agent"] = user_agent

    return ytdl

def get_protocol(info_dict):
    if info_dict.get('requested_formats'):
        return(determine_protocol(info_dict['requested_formats'][0]))
    else:
        return determine_protocol(info_dict)