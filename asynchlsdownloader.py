import asyncio
import binascii
import datetime
import json
import logging
import random
import sys
import time
import traceback
from concurrent.futures import CancelledError, ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from shutil import rmtree
from urllib.parse import urlparse
import threading

import aiofiles
import aiofiles.os as os
import httpx
import m3u8
from Cryptodome.Cipher import AES

from utils import (CONF_HLS_SPEED_PER_WORKER, CONF_PROXIES_BASE_PORT,
                   CONF_PROXIES_MAX_N_GR_HOST, CONF_PROXIES_N_GR_VIDEO,
                   CONFIG_EXTRACTORS, EMA, CONF_HLS_MAX_SPEED_PER_DL, ProxyYTDL, StatusStop, _for_print,
                   _for_print_entry, async_ex_in_executor, async_wait_time, wait_time, dec_retry_error,
                   get_domain, get_format_id, int_or_none, limiter_15, limiter_non,
                   naturalsize, print_norm_time, smuggle_url, sync_to_async,
                   try_get, unsmuggle_url, traverse_obj)

logger = logging.getLogger("async_HLS_DL")


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


class AsyncHLSDLErrorFatal(Exception):
        

   def __init__(self, msg, exc_info=None):
        
        super().__init__(msg)

        self.exc_info = exc_info

class AsyncHLSDLError(Exception):   

   def __init__(self, msg, exc_info=None):
        
        super().__init__(msg)

        self.exc_info = exc_info
        
class AsyncHLSDLReset(Exception):
    
   def __init__(self, msg, exc_info=None):
        
        super().__init__(msg)

        self.exc_info = exc_info

class AsyncHLSDownloader():

    _CHUNK_SIZE = 102400
    _MAX_RETRIES = 5
    _MAX_RESETS = 10
    _MIN_TIME_RESETS = 15
    _CONFIG = CONFIG_EXTRACTORS.copy()
    _CLASSLOCK = threading.Lock()
       
    def __init__(self, enproxy, video_dict, vid_dl):

        try:
        
            self.info_dict = video_dict.copy()
            self.video_downloader = vid_dl
            self.enproxy = (enproxy != 0)
            self.n_workers = self.video_downloader.info_dl['n_workers'] 
            self.count = 0 #cuenta de los workers activos haciendo DL. Al comienzo serán igual a n_workers
            self.video_url = self.info_dict.get('url') #url del format
            self.webpage_url = self.info_dict.get('webpage_url') #url de la web
            self.id = self.info_dict['id']
            self.ytdl = self.video_downloader.info_dl['ytdl']
            self.verifycert = not self.ytdl.params.get('nocheckcertificate')
            self.timeout = httpx.Timeout(30, connect=30)
            self.limits = httpx.Limits(max_keepalive_connections=None, max_connections=None, keepalive_expiry=30)
            self.headers = self.info_dict.get('http_headers')
            
            self.base_download_path = Path(str(self.info_dict['download_path']))

            self.init_file = Path(self.base_download_path, f"init_file.{self.info_dict['format_id']}")

            _filename = self.info_dict.get('_filename') or self.info_dict.get('filename')
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True) 
            self.fragments_base_path = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + "ts")
            
            self.key_cache = dict()
            
            self.n_reset = 0

            self.ema_s = EMA(smoothing=0.01)
            self.ema_t = EMA(smoothing=0.01)

            self.throttle = 0
            self.down_size = 0
            self.down_temp = 0
            self.status = "init"
            self.error_message = ""
            
            self._qspeed = None
            self.ex_hlsdl = ThreadPoolExecutor(thread_name_prefix="ex_hlsdl")
            
            self._proxy = None

            if self.enproxy:

                self._qproxies = Queue()
                for el1, el2 in zip(random.sample(range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST), random.sample(range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST)):
                    self._qproxies.put_nowait((el1, el2))

                            
            self.init_client = httpx.Client(proxies=self._proxy, follow_redirects=True, headers=self.headers, limits=self.limits, timeout=self.timeout, verify=False)
            
            self.filesize = None
            
            self.init()
        
        except Exception as e:
            logger.exception(repr(e))
            self.init_client.close()
            
    def init(self):

        def getter(x):
        
            value, key_text = try_get(
                [(v,kt) for k,v in self._CONFIG.items() if any(x==(kt:=_) for _ in k)],
                lambda y: y[0]) or ("","") 
            
            self.auto_pasres = False
            if 'nakedsword' in x:
                self.auto_pasres = True
                self.n_workers = 5
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {_for_print_entry(self.info_dict)}")
                self.fromplns = self.info_dict.get('playlist_id', False)
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] fromplns [{self.fromplns}]")
                if self.fromplns:
                    if not self.video_downloader.info_dl['fromplns'].get(self.fromplns):
                        self.video_downloader.info_dl['fromplns'][self.fromplns] = [self.video_downloader]
                    else:
                        self.video_downloader.info_dl['fromplns'][self.fromplns].append(self.video_downloader)
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: added new dl to plns [{self.fromplns}], count [{self.video_downloader.info_dl['fromplns'][self.fromplns]}] members[{[dl.info_dict['playlist_index'] for dl in self.video_downloader.info_dl['fromplns'][self.fromplns]]}]")


            if value:
                self.special_extr = True
                return value['ratelimit'].ratelimit(key_text, delay=True)
            else:
                self.special_extr = False
                return limiter_non.ratelimit("transp", delay=True)
        
        

        try:
            self.fromplns = False           
            _extractor = try_get(self.info_dict.get('extractor_key').lower(), lambda x: x.lower())            
            self._limit = getter(_extractor)
            self.info_frag = []
            self.info_init_section = {}
            self.frags_to_dl = []
            self.n_dl_fragments = 0
            
            self.tbr = self.info_dict.get('tbr', 0) #for audio streams tbr is not present
            self.abr = self.info_dict.get('abr', 0)
            _br = self.tbr or self.abr

            part = 0
            uri_ant = ""
            byte_range = {}
            
            self.info_dict['fragments'] = self.get_info_fragments()
            self.info_dict['init_section'] = self.info_dict['fragments'][0].init_section
            
            logger.debug(f"fragments: \n{[str(f) for f in self.info_dict['fragments']]}")
            logger.debug(f"init_section: \n{self.info_dict['init_section']}")

            if (_frag:=self.info_dict['init_section']):
                _file_path =  Path(str(self.fragments_base_path) + ".Frag0")
                _url = _frag.absolute_uri
                if '&hash=' in _url and _url.endswith('&='): _url += '&='
                if _frag.key is not None and _frag.key.method == 'AES-128':
                    if _frag.key.absolute_uri not in self.key_cache:
                        self.key_cache.update({_frag.key.absolute_uri : httpx.get(_frag.key.absolute_uri, headers=self.headers).content})
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{self.key_cache[_frag.key.absolute_uri]}")
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{_frag.key.iv}")
                self.get_init_section(_url, _file_path, _frag.key)
                self.info_init_section.update({"frag": 0, "url": _url, "file": _file_path, "downloaded": True})
            

            
            if self.init_file.exists():
                with open(self.init_file, "r") as f:
                    init_data = json.loads(f.read())
                
                init_data = {int(k): v for k,v in init_data.items()}
            else: init_data = {}


            for i, fragment in enumerate(self.info_dict['fragments']):
                                    
                if fragment.byterange:
                    if fragment.uri == uri_ant:
                        part += 1
                    else:
                        part = 1
                        
                    uri_ant = fragment.uri
                    
                    _file_path =  Path(f"{str(self.fragments_base_path)}.Frag{i}.part.{part}")
                    splitted_byte_range = fragment.byterange.split('@')
                    sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                    byte_range = {
                        'start': sub_range_start,
                        'end': sub_range_start + int(splitted_byte_range[0]),
                    }

                else:
                    _file_path =    Path(f"{str(self.fragments_base_path)}.Frag{i}")
                    byte_range = {}

                est_size = int(_br * fragment.duration * 1000 / 8)

                _url = fragment.absolute_uri
                if '&hash=' in _url and _url.endswith('&='): _url += '&=' 
                
                if _file_path.exists():
                    size = _file_path.stat().st_size
                    hsize = init_data.get(i+1)
                    self.info_frag.append({"frag" : i+1, "url" : _url, "key": fragment.key, "file" : _file_path, "byterange" : byte_range, "downloaded" : True, "estsize" : est_size, "headersize" : hsize, "size": size, "n_retries": 0, "error" : ["AlreadyDL"]})                
                    self.n_dl_fragments += 1
                    self.down_size += size
                    if not hsize:
                        self.frags_to_dl.append(i+1)
                    
                else:
                    self.info_frag.append({"frag" : i+1, "url" : _url, "key": fragment.key, "file" : _file_path, "byterange" : byte_range, "downloaded" : False, "estsize" : est_size, "headersize": init_data.get(i+1), "size": None, "n_retries": 0, "error" : []})
                    self.frags_to_dl.append(i+1)
    
                    
                if fragment.key is not None and fragment.key.method == 'AES-128':
                    if fragment.key.absolute_uri not in self.key_cache:
                        self.key_cache.update({fragment.key.absolute_uri : httpx.get(fragment.key.absolute_uri, headers=self.headers).content})
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{self.key_cache[fragment.key.absolute_uri]}")
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{fragment.key.iv}")

            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: \nFrags DL: {self.fragsdl()}\nFrags not DL: {self.fragsnotdl()}")        
            
            self.n_total_fragments = len(self.info_dict['fragments'])
            
            self.totalduration = self.info_dict.get('duration')
            if not self.totalduration:
                self.calculate_duration() #get total duration
            self.filesize = self.info_dict.get('filesize') or self.info_dict.get('filesize_approx')
            if not self.filesize:
                self.calculate_filesize() #get filesize estimated
            
            self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 60

            if not self.filesize: _est_size = "NA"
            
            else: 
                if (self.filesize - self.down_size) < 250000000:
                    self.n_workers = min(self.n_workers, 8)
                elif 250000000 <= (self.filesize - self.down_size) < 500000000:
                    self.n_workers = min(self.n_workers, 16)
                elif 500000000 <= (self.filesize - self.down_size) < 1250000000:
                    self.n_workers = min(self.n_workers, 32)
                    self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 90
                else:
                    self.n_workers = min(self.n_workers, 64)
                    self._CONF_HLS_MIN_N_TO_CHECK_SPEED = 120

                
                _est_size = naturalsize(self.filesize)

            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: total duration {print_norm_time(self.totalduration)} -- estimated filesize {_est_size} -- already downloaded {naturalsize(self.down_size)} -- total fragments {self.n_total_fragments} -- fragments already dl {self.n_dl_fragments}")  
            
            if self.filename.exists() and self.filename.stat().st_size > 0:
                self.status = "done"
                
            elif not self.frags_to_dl:
                self.status = "init_manipulating"
        except Exception as e:
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][init] {repr(e)}")
            self.status = "error"

    def calculate_duration(self):
        self.totalduration = 0
        for fragment in self.info_dict['fragments']:
            self.totalduration += fragment.duration
            
    def calculate_filesize(self):
        _bitrate = self.tbr or self.abr
        self.filesize = int(self.totalduration * 1000 * _bitrate / 8)
          
    def get_info_fragments(self):
        
        try:
                    
            self.m3u8_obj = self.get_m3u8_obj()
            
            if not self.m3u8_obj or not self.m3u8_obj.segments: 
                raise AsyncHLSDLError("couldnt get m3u8 file")
            
            #self.cookies = self.init_client.cookies.jar.__dict__['_cookies']
            if self.m3u8_obj.keys:
                for _key in self.m3u8_obj.keys:
                    if _key and _key.method != 'AES-128': logger.warning(f"key AES method: {_key.method}")
            
            return self.m3u8_obj.segments
 
        except Exception as e:
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][get_info_fragments] - {repr(e)}")
            raise AsyncHLSDLErrorFatal(repr(e))
 
    def get_m3u8_obj(self):  
            
        m3u8_doc = try_get(self.init_client.get(self.video_url), 
                                lambda x: x.content.decode('utf-8', 'replace'))

        return(m3u8.loads(m3u8_doc, self.video_url))
        
    @dec_retry_error
    def get_init_section(self, uri, file, key):
        try:  
            cipher = None
            if key is not None and key.method == 'AES-128':
                iv = binascii.unhexlify(key.iv[2:])
                cipher = AES.new(self.key_cache[key.absolute_uri], AES.MODE_CBC, iv)
            res = self.init_client.get(uri)
            res.raise_for_status()
            _frag = res.content if not cipher else cipher.decrypt(res.content)
            with open(file, "wb") as f:
                f.write(_frag)            

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[get_init_section] {repr(e)} \n{'!!'.join(lines)}")
            raise
        
    def reset(self):

        self.ema_s.reset()
        self.ema_t.reset()

        count = 0
        
        while (count < 3):

            with self._limit:

                try:
                    
                    if self.video_downloader.stop_event.is_set():
                        break                    
                    
                    if self.fromplns:
                        _wurl = self.info_dict.get('webpage_url').split("/scene")[0]
                    else:
                        _wurl = self.info_dict.get('webpage_url')
                    
                    _webpage_url = smuggle_url(_wurl, {'indexdl': self.video_downloader.index}) if self.special_extr else _wurl

                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]:get video dict: {_webpage_url}")

                    _info = None

                    try:
                        if self.enproxy:
                            el1, el2 = self._qproxies.get()
                            _proxy =  f'http://127.0.0.1:{CONF_PROXIES_BASE_PORT + el1*100 + el2}'
                            self._proxy = {'http://': _proxy, 'https://': _proxy}
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: proxy [{_proxy}]")
                            ytdl_opts = self.ytdl.params.copy()
                            with ProxyYTDL(opts=ytdl_opts, proxy=_proxy) as proxy_ytdl:
                                _info = proxy_ytdl.sanitize_info(proxy_ytdl.extract_info(_webpage_url))
                        else:
                            if self.fromplns:
                                with AsyncHLSDownloader._CLASSLOCK:
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: {self.video_downloader.info_dl['urls_on_go'].get(_wurl)}")
                                    _waitqueue = True
                                    if not self.video_downloader.info_dl['urls_on_go'].get(_wurl):
                                        _waitqueue = False
                                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: url pl not on go")
                                        self.video_downloader.info_dl['urls_on_go'][_wurl] = {"event": threading.Event(), "checked": {}}                                
                                        
                                        try:
                                            _plinfo = self.ytdl.sanitize_info(self.ytdl.extract_info(_webpage_url, download=False))
                                        except Exception as e:
                                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: fail when geting plinfo")
                                            _plinfo = None
                                        
                                        if _plinfo:
                                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: info from video pl ok, out in the queue\n%no%{_for_print(_plinfo)}") 
                                            self.video_downloader.info_dl['queue_ch'].put_nowait({"id": _plinfo['id'], "info": _plinfo})
                                            self.video_downloader.info_dl['urls_on_go'][_wurl]["checked"].update({self.info_dict['playlist_index']: True})
                                            self.video_downloader.info_dl['urls_on_go'][_wurl]["event"].set()                                            
                                            _info = traverse_obj(_plinfo['entries'], self.info_dict['playlist_index']-1)

                                if _waitqueue:
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: waiitng for event that info is in the queeue")                                    
                                    res = wait_time(120, self.video_downloader.info_dl['urls_on_go'][_wurl]["event"])
                                    if not res:
                                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: info put on the queue, waiting for it")  
                                        _count = len(self.video_downloader.info_dl['queue_ch'].queue)
                                        while _count > 0:                                                
                                            try:
                                                _msg = self.video_downloader.info_dl['queue_ch'].get(timeout=30)
                                                if _msg.get('id') != self.info_dict['playlist_id']:
                                                    self.video_downloader.info_dl['queue_ch'].put_nowait(_msg)
                                                    _count -= 1
                                                    continue
                                                else:
                                                    _plinfo = _msg.get('info')
                                                    _info = traverse_obj(_plinfo['entries'], self.info_dict['playlist_index']-1)
                                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: {_for_print_entry(_info)}")
                                                    self.video_downloader.info_dl['urls_on_go'][_wurl]["checked"].update({self.info_dict['playlist_index']: True})
                                                    logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: {self.video_downloader.info_dl['urls_on_go'].get(_wurl)}")
                                                    if len(self.video_downloader.info_dl['urls_on_go'][_wurl]["checked"]) == self.info_dict['n_entries']:
                                                        self.video_downloader.info_dl['urls_on_go'][_wurl] = None
                                                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]: all scenes got info")
                                                    else:
                                                        self.video_downloader.info_dl['queue_ch'].put_nowait(_msg)

                                                    break


                                            except Exception as e:
                                                logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}] error queue get info {str(e)}")
                            else:
                                _info = _info = self.ytdl.sanitize_info(self.ytdl.extract_info(_webpage_url, download=False))
                                            
                        if _info:
                            info_reset = get_format_id(_info, self.info_dict['format_id'])
                        else:
                            info_reset = None
                             
                    except StatusStop as e:
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]  stop {repr(e)}")
                        return
                    except Exception as e:
                        raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}] fails no descriptor {e}")
                    
                    if self.video_downloader.stop_event.is_set():
                        return
                    if not info_reset:
                        raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]  fails no descriptor")         

                    try: 
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}] New info video OK")   
                        #logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}] New info video {_for_print(info_reset)}")
            
                        self.prep_reset(info_reset)
                        self.n_reset += 1
                        break
                    except Exception as e:
                        logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}] Exception occurred when reset: {repr(e)} {_for_print(info_reset)}")
                        raise AsyncHLSDLErrorFatal("RESET fails: preparation frags failed")

                except Exception as e:
                    logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}] outer Exception occurred when reset: {repr(e)}")
                    count += 1
                    if count == 3: raise AsyncHLSDLErrorFatal("Reset failed")
                
    def prep_reset(self, info_reset):
       

        self.headers = info_reset.get('http_headers')
        self.video_url = info_reset.get('url')
        self.init_client.close()
        self.init_client = httpx.Client(proxies=self._proxy, follow_redirects=True, headers=self.headers, limits=self.limits, timeout=self.timeout, verify=False)
        
        #self.webpage_url = self.info_dict['webpage_url'] = info_reset.get('webpage_url')
        
        self.frags_to_dl = []

        part = 0
        uri_ant = ""
        byte_range = {}
        
        self.info_dict['fragments'] = self.get_info_fragments()        


        for i, fragment in enumerate(self.info_dict['fragments']):

            try:                   
                if fragment.byterange:
                    if fragment.uri == uri_ant:
                        part += 1
                    else:
                        part = 1
                        
                    uri_ant = fragment.uri
                    
                    _file_path =  Path(f"{str(self.fragments_base_path)}.Frag{i}.part.{part}")
                    splitted_byte_range = fragment.byterange.split('@')
                    sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                    byte_range = {
                        'start': sub_range_start,
                        'end': sub_range_start + int(splitted_byte_range[0]),
                    }

                else:
                    _file_path = Path(f"{str(self.fragments_base_path)}.Frag{i}")
                    byte_range = {}

                _url = fragment.absolute_uri
                if '&hash=' in _url and _url.endswith('&='): _url += '&=' 

                if not self.info_frag[i]['downloaded'] or (self.info_frag[i]['downloaded'] and not self.info_frag[i]['headersize']):
                    self.frags_to_dl.append(i+1)                        
                    self.info_frag[i]['url'] = _url
                    self.info_frag[i]['file'] = _file_path
                    if not self.info_frag[i]['downloaded'] and self.info_frag[i]['file'].exists(): 
                        self.info_frag[i]['file'].unlink()
                    
                    self.info_frag[i]['n_retries'] = 0
                    self.info_frag[i]['byterange'] = byte_range
                    self.info_frag[i]['key'] = fragment.key
                    if fragment.key is not None and fragment.key.method == 'AES-128':
                        if fragment.key.absolute_uri not in self.key_cache:
                            self.key_cache[fragment.key.absolute_uri] = httpx.get(fragment.key.absolute_uri, headers=self.headers).content
                        
            except Exception as e:
                logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:prep_reset error with i = [{i}] \n\ninfo_dict['fragments'] {len(self.info_dict['fragments'])}\n\n{[str(f) for f in self.info_dict['fragments']]}\n\ninfo_frag {len(self.info_frag)}\n\n{self.info_frag}")
                raise

        if not self.frags_to_dl:
            self.status = "init_manipulating"
        else:
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:prep_reset:OK {self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}")

    async def check_speed(self):
        
        def getter(x):
            return x*CONF_HLS_SPEED_PER_WORKER


        _speed = []

        _num_chunks = self._CONF_HLS_MIN_N_TO_CHECK_SPEED
        
        pending = None
        _max_bd_detected = False
        _max_bd_extra = 0
        
        try:
            while True:

                done, pending = await asyncio.wait([_reset:=asyncio.create_task(self.video_downloader.reset_event.wait()), 
                                                    _stop:=asyncio.create_task(self.video_downloader.stop_event.wait()), 
                                                    _qspeed:=asyncio.create_task(self._qspeed.get())], 
                                                    return_when=asyncio.FIRST_COMPLETED)
                
                for _el in pending: _el.cancel()
                if any([_ in done for _ in [_reset, _stop]]):
                    logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] event detected")
                    return
                _input_speed = try_get(list(done), lambda x: x[0].result())
                if _input_speed == "KILL":
                    return
                _speed.append(_input_speed)

                if len(_speed) > _num_chunks:    
                
                    # if any([all([el == 0 for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):]]), (_speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):] == sorted(_speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):], reverse=True)), all([el < getter(self.n_workers_now) for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):]])]):
                    # if any([all([el == 0 for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):]]), (_speed[-self._CONF_HLS_MIN_N_TO_CHECK_SPEED:] == sorted(_speed[-self._CONF_HLS_MIN_N_TO_CHECK_SPEED:], reverse=True)), all([el < getter(self.n_workers_now) for el in _speed[-self._CONF_HLS_MIN_N_TO_CHECK_SPEED:]])]):
                    if any([all([el == 0 for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):]]), all([el < getter(self.count) for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):]])]):
                        #self.video_downloader.reset_event.set()                        
                        #_str_speed = ', '.join([f'{el}' for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 10):]])
                        #logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] speed reset: n_el_speed[{len(_speed)}]\n%no%{_str_speed}")
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] speed reset: n_el_speed[{len(_speed)}]")
                        #_speed = []

                        # self.video_downloader.pause()
                        # await async_wait_time(5)
                        # self.video_downloader.resume()
                        if self.fromplns:
                            self.video_downloader.reset_plns(self.fromplns)
                            
                        else:
                            self.video_downloader.reset_event.set()  
                       

                        break

                        #_num_chunks = len(_speed) + 40
                        
                        # if _max_bd_detected:
                        #     self._SEM.reset(self.count)
                        #     self.n_workers_now = self.count
                        #     _max_bd_detected = False
                        

                        #break
                        #self._CONF_HLS_MIN_N_TO_CHECK_SPEED += 10
                        
                    #elif all([el > CONF_HLS_MAX_SPEED_PER_DL for el in _speed[self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2:]]):
                    elif all([el > CONF_HLS_MAX_SPEED_PER_DL for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):]]):
                        #_str_speed = ', '.join([f'{el}' for el in _speed[self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2:]])
                        #_str_speed = ', '.join([f'{el}' for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 10):]])
                        #logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] MAX BD -> decrease speed: n_el_speed[{len(_speed)}]\n%no%{_str_speed}")
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] MAX BD -> decrease speed: n_el_speed[{len(_speed)}]")
                        #self.video_downloader.pause()
                        #await async_wait_time(1)
                        #self.video_downloader.resume()
                        
                        async with self._LOCK:
                            # if self.n_workers_now > 2:
                            #     self.n_workers_now -= 1
                            #     self._SEM._value -= 1
                            if not _max_bd_detected: 
                                _max_bd_detected = True
                                self.throttle += 0.1
                                _num_chunks = len(_speed) + 60
                                self.ema_s.reset()
                                self.ema_t.reset()
                            else: 
                                _max_bd_extra += 1                                
                                self.throttle += 0.005
                                _num_chunks = len(_speed) + 40
                        
                       
                        
                    
                    elif all([el < 0.8*CONF_HLS_MAX_SPEED_PER_DL for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):]]) and (_max_bd_detected or _max_bd_extra):
                        
                         #_str_speed = ', '.join([f'{el}' for el in _speed[-(self._CONF_HLS_MIN_N_TO_CHECK_SPEED // 2):]])
                        #logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] 0.8 MAX BD -> increase speed: n_el_speed[{len(_speed)}]\n%no%{_str_speed}")
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] 0.8 MAX BD -> increase speed: n_el_speed[{len(_speed)}]")

                        _num_chunks = len(_speed) + 40
                        
                        if _max_bd_detected and not _max_bd_extra:
                            self.throttle -= 0.01
                        else:
                            self.throttle -= 0.005
                            _max_bd_detected -= 1

                        if self.throttle < 0 or not _max_bd_extra:
                            self.throttle = 0
                            _max_bd_detected = False
                            _max_bd_extra = 0

                
                if pending: await asyncio.wait(pending)
                await asyncio.sleep(0)
            
            
        
        except Exception as e:
            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] {repr(e)}")
        finally:
            if pending: await asyncio.wait(pending)            
            self._speed.extend(_speed)
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] bye")
            
    async def fetch(self, nco):

        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: init worker")

        client = httpx.AsyncClient(proxies=self._proxy, limits=self.limits, follow_redirects=True, timeout=self.timeout, verify=self.verifycert, headers=self.headers)
        
        try:

            while not any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                
                q = await self.frags_queue.get()
                
                if any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                    return

                if q == "KILL":
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: KILL")
                    return  

                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]\n{self.info_frag[q - 1]}")

                if self.video_downloader.pause_event.is_set():
                    done, pending = await asyncio.wait([asyncio.create_task(self.video_downloader.resume_event.wait()), asyncio.create_task(self.video_downloader.reset_event.wait()), asyncio.create_task(self.video_downloader.stop_event.wait())], return_when=asyncio.FIRST_COMPLETED)
                    for _el in pending: _el.cancel()
                    await asyncio.wait(pending)
                    self.video_downloader.pause_event.clear()
                    self.video_downloader.resume_event.clear()
                    
                    #self.ema_s = EMA(smoothing=0.01)
                    #self.ema_t = EMA(smoothing=0.01)

                    self.ema_s.reset()
                    self.ema_t.reset()
                    
                    if any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                        return

                url = self.info_frag[q - 1]['url']
                filename = Path(self.info_frag[q - 1]['file'])
                filename_exists = await os.path.exists(filename)
                key = self.info_frag[q - 1]['key']
                cipher = None
                if key is not None and key.method == 'AES-128':
                    iv = binascii.unhexlify(key.iv[2:])
                    cipher = AES.new(self.key_cache[key.absolute_uri], AES.MODE_CBC, iv)
                byte_range = self.info_frag[q - 1].get('byterange')
                headers = {}
                if byte_range:
                    headers['range'] = f"bytes={byte_range['start']}-{byte_range['end'] - 1}"

                await asyncio.sleep(0)        
                
                while ((self.info_frag[q - 1]['n_retries'] < self._MAX_RETRIES) and not any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()])):

                    try: 

                        self.premsg = f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:[frag[{q}] "
                        
                        async with self._SEM:

                            #logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}] start to DL")

                            async with aiofiles.open(filename, mode='ab') as f:                            

                                async with client.stream("GET", url, headers=headers, timeout=15) as res:
                                                    
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: {res.status_code} {res.reason_phrase}")
                                    
                                    if res.status_code == 403:
                                        if self.fromplns:
                                            self.video_downloader.reset_plns(self.fromplns)
                                            
                                        else:
                                            self.video_downloader.reset_event.set()                                                   
                                        raise AsyncHLSDLErrorFatal(f"Frag:{str(q)} resp code:{str(res)}")
                                    elif res.status_code >= 400:
                                        raise AsyncHLSDLError(f"Frag:{str(q)} resp code:{str(res)}")                                
                                    else:
                                            
                                        _hsize = int_or_none(res.headers.get('content-length'))
                                        if _hsize:
                                            self.info_frag[q-1]['headersize'] = _hsize
                                            async with self._LOCK:
                                                if not self.filesize:
                                                    self.filesize = _hsize * len(self.info_dict['fragments'])
                                                    async with self.video_downloader.alock:
                                                        self.video_downloader.info_dl['filesize'] += self.filesize
                                        else:
                                            #self.video_downloader.reset_event.set()
                                            raise AsyncHLSDLErrorFatal(f"Frag:{str(q)} _hsize is None")                                    
                                        
                                        if self.info_frag[q-1]['downloaded']:                                    
                                            
                                            if filename_exists:
                                                _size = self.info_frag[q-1]['size'] = (await os.stat(filename)).st_size 
                                                if _size and  (_hsize - 100 <= _size <= _hsize + 100):                            
                                        
                                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: Already DL with hsize[{_hsize}] and size [{_size}] check[{_hsize - 100 <=_size <= _hsize + 100}]")                                    
                                                    break
                                                else:
                                                    await f.truncate(0)
                                                    self.info_frag[q-1]['downloaded'] = False
                                                    async with self._LOCK:
                                                        self.n_dl_fragments -= 1
                                                        self.down_size -= _size
                                                        self.down_temp -= _size
                                                        async with self.video_downloader.alock:
                                                            self.video_downloader.info_dl['down_size'] -= _size                                                            
                                            else:
                                                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}] frag with mark downloaded but file [{filename}] doesnt exists")
                                                self.info_frag[q-1]['downloaded'] = False
                                                async with self._LOCK:
                                                    self.n_dl_fragments -= 1

                                        if self.info_frag[q-1]['headersize'] < self._CHUNK_SIZE:
                                            _chunk_size = self.info_frag[q-1]['headersize']
                                        else:
                                            _chunk_size = self._CHUNK_SIZE
                                        
                                        num_bytes_downloaded = res.num_bytes_downloaded
                                    
                                        self.info_frag[q - 1]['time2dlchunks'] = []
                                        self.info_frag[q - 1]['sizechunks'] = []
                                        self.info_frag[q - 1]['nchunks_dl'] = 0
                                        # self.info_frag[q - 1]['statistics'] = []
                                        
                                        _started = time.monotonic()
                                        
                                        async for chunk in res.aiter_bytes(chunk_size=_chunk_size):
                                            
                                            
                                            if self.video_downloader.pause_event.is_set():
                                                done, pending = await asyncio.wait([asyncio.create_task(self.video_downloader.resume_event.wait()), asyncio.create_task(self.video_downloader.reset_event.wait()), asyncio.create_task(self.video_downloader.stop_event.wait())], return_when=asyncio.FIRST_COMPLETED)
                                                for _el in pending: _el.cancel()
                                                await asyncio.wait(pending)
                                                self.video_downloader.pause_event.clear()
                                                self.video_downloader.resume_event.clear()                                                

                                                self.ema_s.reset()
                                                self.ema_t.reset()             
                            

                                            if any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                                                raise AsyncHLSDLErrorFatal("event")
                                                                                        
                                            _timechunk = time.monotonic() - _started
                                            self.info_frag[q - 1]['time2dlchunks'].append(_timechunk)                             
                                            if cipher: data = cipher.decrypt(chunk)
                                            else: data = chunk 
                                            await f.write(data)
                                            
                                            async with self._LOCK:
                                                self.down_size += (_iter_bytes:=(res.num_bytes_downloaded - num_bytes_downloaded))
                                                if (_dif:=self.down_size - self.filesize) > 0: 
                                                        self.filesize += _dif
                                                self.first_data.set()

                                            async with self.video_downloader.alock:
                                                if _dif > 0:    
                                                    self.video_downloader.info_dl['filesize'] += _dif
                                                self.video_downloader.info_dl['down_size'] += _iter_bytes
                                                    
                                            num_bytes_downloaded = res.num_bytes_downloaded
                                            self.info_frag[q - 1]['nchunks_dl'] += 1
                                            self.info_frag[q - 1]['sizechunks'].append(_iter_bytes)
                                                                                
                                            if self.throttle: await async_wait_time(self.throttle)
                                            else: await asyncio.sleep(0)
                                            _started = time.monotonic()
                                        
                            _size = (await os.stat(filename)).st_size
                            _hsize = self.info_frag[q-1]['headersize']
                            if (_hsize - 100 <= _size <= _hsize + 100):
                                self.info_frag[q - 1]['downloaded'] = True 
                                self.info_frag[q - 1]['size'] = _size
                                async with self._LOCK:
                                    self.n_dl_fragments += 1     
                                    
                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:frag[{q}] OK DL: total[{self.n_dl_fragments}]\n{self.info_frag[q - 1]}")
                                break
                            else: 
                                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:frag[{q}] end of streaming. fragment not completed\n{self.info_frag[q - 1]}")                                    
                                raise AsyncHLSDLError(f"fragment not completed frag[{q}]")     
          
                                                       
                    except AsyncHLSDLErrorFatal as e:
                                                        
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: errorfatal {repr(e)}")
                        self.info_frag[q - 1]['error'].append(repr(e))
                        self.info_frag[q - 1]['downloaded'] = False
                        if await os.path.exists(filename):
                            _size = (await os.stat(filename)).st_size                           
                            await os.remove(filename)
                        
                            async with self._LOCK:
                                self.down_size -= _size
                                self.down_temp -= _size
                                async with self.video_downloader.alock:                                       
                                    self.video_downloader.info_dl['down_size'] -= _size                                               
                        
                        return                 
                    except (asyncio.exceptions.CancelledError, asyncio.CancelledError, CancelledError) as e:
                        #logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: cancelled exception")
                        self.info_frag[q - 1]['error'].append(repr(e))
                        self.info_frag[q - 1]['downloaded'] = False
                        lines = traceback.format_exception(*sys.exc_info())
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: CancelledError: \n{'!!'.join(lines)}")
                        if await os.path.exists(filename):
                            _size = (await os.stat(filename)).st_size                           
                            await os.remove(filename)
                            
                            async with self._LOCK:
                                self.down_size -= _size
                                self.down_temp -= _size
                                async with self.video_downloader.alock:                                       
                                    self.video_downloader.info_dl['down_size'] -= _size
                        return                   
                    except RuntimeError as e:
                        self.info_frag[q - 1]['error'].append(repr(e))
                        self.info_frag[q - 1]['downloaded'] = False
                        
                        if await os.path.exists(filename):
                            _size = (await os.stat(filename)).st_size                           
                            await os.remove(filename)
                            
                            async with self._LOCK:
                                self.down_size -= _size
                                self.down_temp -= _size
                                async with self.video_downloader.alock:                                       
                                    self.video_downloader.info_dl['down_size'] -= _size
                        
                        logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: runtime error: {repr(e)}")
                        return                   

                    except Exception as e:                        
                        self.info_frag[q - 1]['error'].append(repr(e))
                        self.info_frag[q - 1]['downloaded'] = False
                        lines = traceback.format_exception(*sys.exc_info())
                        if not "httpx" in str(e.__class__) and not "AsyncHLSDLError" in str(e.__class__):
                            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: error {str(e.__class__)}")
                            
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: error {repr(e)} \n{'!!'.join(lines)}")
                        self.info_frag[q - 1]['n_retries'] += 1
                        if await os.path.exists(filename):
                            _size = (await os.stat(filename)).st_size                           
                            await os.remove(filename)
                        
                            async with self._LOCK:
                                self.down_size -= _size
                                self.down_temp -= _size
                                async with self.video_downloader.alock:                                       
                                    self.video_downloader.info_dl['down_size'] -= _size
                        
                        if any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                            return
                        
                        if self.info_frag[q - 1]['n_retries'] < self._MAX_RETRIES:
                                                     

                                await async_wait_time(random.choice([i for i in range(1,5)]))
                                await asyncio.sleep(0)
                                
                                
                        else:
                            self.info_frag[q - 1]['error'].append("MaxLimitRetries")
                            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:frag[{q}]:MaxLimitRetries:skip")                           
                            self.info_frag[q - 1]['skipped'] = True
                            break
                        

        finally:    
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: bye worker")
            await client.aclose()
            async with self._LOCK:
                self.count -= 1

    async def fetch_async(self):
        
        self._LOCK = asyncio.Lock()        
        self._SEM = MySem(self.n_workers, dl=self)
        self.first_data = asyncio.Event()
        self.frags_queue = asyncio.Queue()
        for frag in self.frags_to_dl:
            self.frags_queue.put_nowait(frag)        
        
        for _ in range(self.n_workers):
            self.frags_queue.put_nowait("KILL")

        self._speed  = []

        n_frags_dl = 0

        _tstart = time.monotonic() 
        
        try:
                    
            while True:

                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] TASKS INIT")

                try:
                    
                    self.count = self.n_workers
                    self.down_temp = self.down_size
                    self.started = time.monotonic()
                    self.status = "downloading"                    
                    self.video_downloader.reset_event.clear()
                    self.first_data.clear()
                    self.throttle = 0
                    self._qspeed = asyncio.Queue()

                    check_task = [asyncio.create_task(self.check_speed())]
                    tasks = [asyncio.create_task(self.fetch(i), name=f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]") for i in range(self.n_workers)]
                    
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch_async]: done[{len(list(done))}] pending[{len(list(pending))}]")

                    _nfragsdl = len(self.fragsdl())
                    inc_frags_dl = _nfragsdl - n_frags_dl
                    n_frags_dl = _nfragsdl
                    
                    if n_frags_dl == len(self.info_dict['fragments']):
                        self._qspeed.put_nowait("KILL")
                        await asyncio.sleep(0)
                        await asyncio.wait(check_task)
                        break
                    
                    else:
                        if self.video_downloader.stop_event.is_set():

                            self.status = "stop"
                            await asyncio.wait(check_task)
                            return

                        elif self.video_downloader.reset_event.is_set():
                            
                            await asyncio.wait(check_task)

                            if self.n_reset < self._MAX_RESETS:

                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]")

                                try:

                                    await async_ex_in_executor(self.ex_hlsdl, self.reset)
                                    if self.video_downloader.stop_event.is_set():
                                        return
                                    self.frags_queue = asyncio.Queue()
                                    for frag in self.frags_to_dl: self.frags_queue.put_nowait(frag)
                                    if ((_t:=time.monotonic()) - _tstart) < self._MIN_TIME_RESETS:
                                        self.n_workers -= self.n_workers // 4
                                    _tstart = _t
                                    for _ in range(self.n_workers): self.frags_queue.put_nowait("KILL")
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:OK:Pending frags {len(self.fragsnotdl())}")
                                    await asyncio.sleep(0)
                                    continue 
                                    
                                except Exception as e:
                                    lines = traceback.format_exception(*sys.exc_info())
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR reset couldnt progress:[{repr(e)}]\n{'!!'.join(lines)}")
                                    self.status = "error"
                                    await self.clean_when_error()
                                    raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: ERROR reset couldnt progress")
                            
                            else:
                                
                                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR:Max_number_of_resets")  
                                self.status = "error"
                                await self.clean_when_error()
                                await asyncio.sleep(0)
                                raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: ERROR max resets")     

                        else:                

                            self._qspeed.put_nowait("KILL")
                            await asyncio.wait(check_task)
                            if (inc_frags_dl > 0):
                                
                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_frags_dl} -> {inc_frags_dl}] new cycle with no fatal error")
                                try:
                                    await async_ex_in_executor(self.ex_hlsdl, self.reset)
                                    if self.video_downloader.stop_event.is_set():
                                        return
                                    self.frags_queue = asyncio.Queue()
                                    for frag in self.frags_to_dl: self.frags_queue.put_nowait(frag)
                                    for _ in range(self.n_workers): self.frags_queue.put_nowait("KILL")
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET new cycle[{self.n_reset}]:OK:Pending frags {len(self.fragsnotdl())}") 
                                    self.n_reset -= 1
                                    continue 
                                    
                                except Exception as e:
                                    lines = traceback.format_exception(*sys.exc_info())
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR reset couldnt progress:[{repr(e)}]\n{'!!'.join(lines)}")
                                    self.status = "error"
                                    await self.clean_when_error()
                                    await asyncio.sleep(0)
                                    raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: ERROR reset couldnt progress")
                                
                            else:
                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_frags_dl} <-> {inc_frags_dl}] no improvement, lets raise an error")
                                
                                raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: no changes in number of dl frags in one cycle")

                except AsyncHLSDLErrorFatal:
                    raise            
                except Exception as e:
                    logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch_async] error {repr(e)}")
                finally:                   
                    await asyncio.sleep(0)
                    

        except Exception as e:
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] error {repr(e)}")
        finally:
            self.init_client.close()            
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Frags DL completed")
            if not self.video_downloader.stop_event.is_set() and not self.status == "error":
                self.status = "init_manipulating"
            await async_ex_in_executor(self.ex_hlsdl, self.dump_init_file)
            self.ex_hlsdl.shutdown(wait=False, cancel_futures=True)

    def dump_init_file(self):

        init_data = {el['frag']: el['headersize'] for el in self.info_frag if el['headersize']}
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] init data\n{init_data}")
        with open(self.init_file, "w") as f:
            json.dump(init_data, f)

    async def clean_when_error(self):
        
        for f in self.info_frag:
            if f['downloaded'] == False:
                
                if (await os.path.exists(f['file'])):
                    await os.path.remove(f['file'])
   
    def sync_clean_when_error(self):
            
        for f in self.info_frag:
            if f['downloaded'] == False:
                if f['file'].exists():
                    f['file'].unlink()
                    
    def ensamble_file(self):

        self.status = "manipulating"
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: Fragments DL \n{self.fragsdl()}")
        
        try:
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{self.filename}")
            with open(self.filename, mode='wb') as dest:
                _skipped = 0
                for f in self.info_frag: 
                    if f.get('skipped', False):
                        _skipped += 1
                        continue
                    if not f['size']:
                        if f['file'].exists(): f['size'] = f['file'].stat().st_size
                        if f['size'] and (f['headersize'] - 100 <= f['size'] <= f['headersize'] + 100):                
              
                            with open(f['file'], 'rb') as source:
                                dest.write(source.read())
                        else: raise AsyncHLSDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling: {f}")
                    else:
                        with open(f['file'], 'rb') as source:
                                dest.write(source.read())
                        
        
        except Exception as e:
            if self.filename.exists():
                self.filename.unlink()
            lines = traceback.format_exception(*sys.exc_info())
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Exception ocurred: \n{'!!'.join(lines)}")
            self.status = "error"
            self.sync_clean_when_error() 
            raise
        finally:
            if self.filename.exists():
                rmtree(str(self.download_path),ignore_errors=True)
                self.status = "done" 
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [ensamble_file] file ensambled")
                if _skipped:
                    logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [ensamble_file] skipped frags [{_skipped}]")             
            else:
                self.status = "error"  
                self.sync_clean_when_error()                        
                raise AsyncHLSDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling parts")

    def fragsnotdl(self):
        res = []
        for frag in self.info_frag:
            if (frag['downloaded'] == False) and not frag.get('skipped', False):
                #res.append(frag['frag'])
                res.append(frag)
        return res
    
    def fragsdl(self):
        res = []
        for frag in self.info_frag:
            if (frag['downloaded'] == True) or frag.get('skipped', False):
                #res.append({'frag': frag['frag'], 'headersize': frag['headersize'], 'size': frag['size']})
                res.append(frag)
        return res

    def format_frags(self):
        import math        
        return f'{(int(math.log(self.n_total_fragments, 10)) + 1)}d'
    
    def print_hookup(self):

       
        _filesize_str = naturalsize(self.filesize) if self.filesize else "--"
        _proxy = self._proxy['http://'].split(':')[-1] if self._proxy else False
        if self.status == "done":
            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] Completed \n"
        elif self.status == "init":
            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] Waiting to DL [{_filesize_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"           
        elif self.status == "error":
            _rel_size_str = f'{naturalsize(self.down_size)}/{naturalsize(self.filesize)}' if self.filesize else '--'
            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] ERROR [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "stop":
            _rel_size_str = f'{naturalsize(self.down_size)}/{naturalsize(self.filesize)}' if self.filesize else '--'
            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] STOPPED [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "downloading":
            
            _down_size = self.down_size
            _new = time.monotonic()                                  
            _speed = (_down_size - self.down_temp) / (_new - self.started)
            _speed_ema = (_diff_size_ema:=self.ema_s(_down_size - self.down_temp)) / (_diff_time_ema:=self.ema_t(_new - self.started))
            
            #_speed_ema = self.ema_s((_down_size - self.down_temp) / (_new - self.started))

            if not any([self.video_downloader.reset_event and self.video_downloader.reset_event.is_set(), self.video_downloader.stop_event and self.video_downloader.stop_event.is_set(), self.video_downloader.pause_event and self.video_downloader.pause_event.is_set()]):
                #if self._qspeed: self._qspeed.put_nowait(_speed)
                
                if self._qspeed and self.first_data.is_set(): self._qspeed.put_nowait(_speed_ema)

            if any([self.video_downloader.pause_event and self.video_downloader.pause_event.is_set(), self.video_downloader.reset_event and self.video_downloader.reset_event.is_set()]):
                _speed_str = "--" 
                _eta_str = "--"
            else:
                _speed_str = f'{naturalsize(_speed_ema,True)}ps'
                if _speed_ema and self.filesize:
                
                    if (_est_time:=((self.filesize - _down_size)/_speed_ema)) < 3600:
                        _eta = datetime.timedelta(seconds=_est_time)                    
                        _eta_str = ":".join([_item.split(".")[0] for _item in f"{_eta}".split(":")[1:]])
                    else: _eta_str = "--"
                else: _eta_str = "--"
                
                            
            
            _progress_str = f'{(_down_size/self.filesize)*100:5.2f}%' if self.filesize else '-----'
            self.down_temp = _down_size
            self.started = time.monotonic()             
                
            #msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] WK[{self.n_workers_now:2d}/{self.count:2d}/{self.n_workers:2d}] DL[{_speed_str}] FR[{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}] PR[{_progress_str}] ETA[{_eta_str}]\n"

            msg = f"[HLS][{self.info_dict['format_id']}]: PROXY[{_proxy}] WK[{self.count:2d}/{self.n_workers:2d}] DL[{_speed_str}] FR[{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}] PR[{_progress_str}] ETA[{_eta_str}]\n"
            
            
        elif self.status == "init_manipulating":                   
            msg = f"[HLS][{self.info_dict['format_id']}]: Waiting for Ensambling \n"
        elif self.status == "manipulating":
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0  
            _str = f'[{naturalsize(_size)}/{naturalsize(self.filesize)}]({(_size/self.filesize)*100:.2f}%)' if self.filesize else f'[{naturalsize(_size)}]'       
            msg = f"[HLS][{self.info_dict['format_id']}]: Ensambling {_str} \n"       
        
        
        return msg