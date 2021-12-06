import asyncio
from concurrent.futures import CancelledError

import httpx
import sys
import traceback
from shutil import rmtree
import m3u8
import binascii
from Cryptodome.Cipher import AES

from pathlib import Path

from urllib.parse import urlparse
import logging

import random

from utils import (
    print_norm_time,
    naturalsize,
    int_or_none,
    EMA
)

import aiofiles
import datetime
from statistics import median
import copy
import time

logger = logging.getLogger("async_HLS_DL")

class AsyncHLSDLErrorFatal(Exception):
    

    def __init__(self, msg):
        
        super(AsyncHLSDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncHLSDLError(Exception):
   

    def __init__(self, msg):
        
        super(AsyncHLSDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception
        
class AsyncHLSDLReset(Exception):
    
    def __init__(self, msg):
        
        super(AsyncHLSDLReset, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncHLSDownloader():

    _CHUNK_SIZE = 102400
    _MAX_RETRIES = 5
    _MAX_RESETS = 10
       
    def __init__(self, video_dict, vid_dl):


        self.info_dict = copy.deepcopy(video_dict)
        self.video_downloader = vid_dl
        self.iworkers = vid_dl.info_dl['n_workers'] 
        self.count = 0 #cuenta de los workers activos haciendo DL. Al comienzo serán igual a iworkers
        self.video_url = self.info_dict.get('url') #url del format
        self.webpage_url = self.info_dict.get('webpage_url') #url oioginal de la web
        self.manifest_url = self.info_dict.get('manifest_url') #url del manifiesto de donde salen todos los formatos

        self.id = self.info_dict['id']
        
        self.ytdl = vid_dl.info_dl['ytdl']
        proxies = self.ytdl.params.get('proxy', None)
        if proxies:
            self.proxies = {'http://': f"http://{proxies}", 'https://': f"http://{proxies}"}
        else: self.proxies = None
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')

        self.timeout = httpx.Timeout(10, connect=30)
        self.limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)
        self.headers = self.info_dict.get('http_headers')
        self.base_download_path = Path(str(self.info_dict['download_path']))
        if (_filename:=self.info_dict.get('_filename')):
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True) 
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + ".ts")
        else:
            _filename = self.info_dict.get('filename')
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + ".ts")

        self.key_cache = dict()
        
        self.n_reset = 0
       
        self.ema_s = EMA(smoothing=0.0001)
        self.ema_t = EMA(smoothing=0.0001)
        self.down_size = 0
        self.down_temp = 0        
        
        self.status = "init"
        self.error_message = "" 
        
        self.init()
        
        

    def get_info_fragments(self):
        
        try:
            
            client = httpx.Client(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies)
            
            if self.headers.get('Cookie'):
                
                del self.headers['Cookie']
                res = client.get(self.manifest_url, headers=self.headers)            
            
            res = client.get(self.video_url, headers=self.headers)

            self.m3u8_obj = m3u8.loads(res.text, uri=self.video_url)
            
            self.cookies = client.cookies.jar.__dict__['_cookies']
            #self.m3u8_obj = m3u8.load(self.video_url, headers=self.headers, verify_ssl=self.verifycert)
            if self.m3u8_obj.keys:
                for _key in self.m3u8_obj.keys:
                    if _key.method != 'AES-128': logger.warning(f"key AES method: {_key.method}")
            return self.m3u8_obj.segments
 
        except Exception as e:
            logger.exception(f"no m3u8 info - {repr(e)}")
            raise AsyncHLSDLErrorFatal("no m3u8 info")
        finally:
            client.close()
        
    def init(self):

        self.info_frag = []
        self.frags_to_dl = []
        

        self.n_dl_fragments = 0
        
        
        self.tbr = self.info_dict.get('tbr', 0) #for audio streams tbr is not present
        self.abr = self.info_dict.get('abr', 0)
        _br = self.tbr or self.abr

        part = 0
        uri_ant = ""
        byte_range = {}
        
        self.info_dict['fragments'] = self.get_info_fragments()

        for i, fragment in enumerate(self.info_dict['fragments']):
                                
            if fragment.byterange:
                if fragment.uri == uri_ant:
                    part += 1
                else:
                    part = 1
                    
                uri_ant = fragment.uri
                
                _file_path =  Path(self.download_path, f"{Path(urlparse(fragment.uri).path).name}_part_{part}")
                splitted_byte_range = fragment.byterange.split('@')
                sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                byte_range = {
                    'start': sub_range_start,
                    'end': sub_range_start + int(splitted_byte_range[0]),
                }

            else:
                _file_path =  Path(self.download_path,Path(urlparse(fragment.uri).path).name)
                byte_range = {}

            est_size = int(_br * fragment.duration * 1000 / 8)
            
            if _file_path.exists():
                size = _file_path.stat().st_size
                self.info_frag.append({"frag" : i+1, "url" : fragment.absolute_uri, "key": fragment.key, "file" : _file_path, "byterange" : byte_range, "downloaded" : True, "estsize" : est_size, "headersize" : None, "size": size, "n_retries": 0, "error" : ["AlreadyDL"]})                
                self.n_dl_fragments += 1
                self.down_size += size
                self.frags_to_dl.append(i+1)
                
            else:
                self.info_frag.append({"frag" : i+1, "url" : fragment.absolute_uri, "key": fragment.key, "file" : _file_path, "byterange" : byte_range, "downloaded" : False, "estsize" : est_size, "headersize": None, "size": None, "n_retries": 0, "error" : []})
                self.frags_to_dl.append(i+1)
                
 
                
            if fragment.key is not None and fragment.key.method == 'AES-128':
                if fragment.key.absolute_uri not in self.key_cache:
                    self.key_cache.update({fragment.key.absolute_uri : httpx.get(fragment.key.absolute_uri, headers=self.headers).content})
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{self.key_cache[fragment.key.absolute_uri]}")
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{fragment.key.iv}")




        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: \nFrags DL: {self.fragsdl()}\nFrags not DL: {self.fragsnotdl()}")
        
          
        self.n_total_fragments = len(self.info_dict['fragments'])
        self.calculate_duration() #get total duration
        self.calculate_filesize() #get filesize estimated
        
        if self.filesize == 0: _est_size = "NA"
        else: _est_size = naturalsize(self.filesize)
        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: total duration {print_norm_time(self.totalduration)} -- estimated filesize {_est_size} -- already downloaded {naturalsize(self.down_size)} -- total fragments {self.n_total_fragments} -- fragments already dl {self.n_dl_fragments}")  
        
        if self.filename.exists() and self.filename.stat().st_size > 0:
            self.status = "done"
            
        elif not self.frags_to_dl:
            self.status = "init_manipulating"
       
    def calculate_duration(self):
        self.totalduration = 0
        for fragment in self.info_dict['fragments']:
            self.totalduration += fragment.duration
            
    def calculate_filesize(self):
        _bitrate = self.tbr or self.abr                
        self.filesize = int(self.totalduration * 1000 * _bitrate / 8)
        
    def reset(self):         

        count = 0
        
        while (count < 5):
        
            try:
            
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]:get video dict: {self.webpage_url}")
                
                try:

                    _info = self.ytdl.extract_info(self.webpage_url, download=False)
                    info_reset = _info['entries'][0] if (_info.get('_type') == 'playlist') else _info
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:New info video\{info_reset}")
                    
                except Exception as e:
                    raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:fails no descriptor {e}")

                if not info_reset:
                    raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]: fails no descriptor")         

                try: 
                    if info_reset.get('requested_formats'):
                        info_format = [_info_format for _info_format in info_reset['requested_formats'] if _info_format['format_id'] == self.info_dict['format_id']]
                        self.prep_reset(info_format[0])
                    else: self.prep_reset(info_reset)
                    break
                except Exception as e:
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]: Exception occurred when reset: {repr(e)}")
                    raise AsyncHLSDLErrorFatal("RESET fails: preparation frags failed")
            except Exception as e:
                raise
            finally:
                count += 1
                if count == 5: raise AsyncHLSDLErrorFatal("Reset failed")

        self.n_reset += 1
    
    def prep_reset(self, info_reset):       
       
        self.headers = self.info_dict['http_headers'] = info_reset.get('http_headers')
        self.video_url = self.info_dict['url'] = info_reset.get('url')
        self.webpage_url = self.info_dict['webpage_url'] = info_reset.get('webpage_url')
        self.manifest_url = self.info_dict['manifest_url'] = info_reset.get('manifest_url')

        self.frags_to_dl = []

        part = 0
        uri_ant = ""
        byte_range = {}
        
        self.info_dict['fragments'] = self.get_info_fragments()

        for i, fragment in enumerate(self.info_dict['fragments']):
                                
            if fragment.byterange:
                if fragment.uri == uri_ant:
                    part += 1
                else:
                    part = 1
                    
                uri_ant = fragment.uri
                
                _file_path =  Path(self.download_path, f"{Path(urlparse(fragment.uri).path).name}_part_{part}")
                splitted_byte_range = fragment.byterange.split('@')
                sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                byte_range = {
                    'start': sub_range_start,
                    'end': sub_range_start + int(splitted_byte_range[0]),
                }

            else:
                _file_path =  Path(self.download_path,Path(urlparse(fragment.uri).path).name)
                byte_range = {}



            if not self.info_frag[i]['downloaded'] or (self.info_frag[i]['downloaded'] and not self.info_frag[i]['headersize']):
                self.frags_to_dl.append(i+1)                        
                self.info_frag[i]['url'] = fragment.absolute_uri
                self.info_frag[i]['file'] = _file_path
                if not self.info_frag[i]['downloaded'] and self.info_frag[i]['file'].exists(): 
                    self.info_frag[i]['file'].unlink()
                
                self.info_frag[i]['n_retries'] = 0
                self.info_frag[i]['byterange'] = byte_range
                self.info_frag[i]['key'] = fragment.key
                if fragment.key is not None and fragment.key.method == 'AES-128':
                    if fragment.key.absolute_uri not in self.key_cache:
                        self.key_cache[fragment.key.absolute_uri] = httpx.get(fragment.key.absolute_uri, headers=self.headers).content
                        
        if not self.frags_to_dl:
            self.status = "init_manipulating"
        else:
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:prep_reset:OK {self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}")

    async def wait_time(self, n):

        _started = time.monotonic()
        while True:
            if (_t:=(time.monotonic() - _started)) >= n:
                return _t
            else:
                await asyncio.sleep(0)
                
    async def fetch(self, nco):

        try:

            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: init worker")
            
            while True:

                q = await self.frags_queue.get()
                
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]")
                
                if q == "KILL":
                    break  
                         
                url = self.info_frag[q - 1]['url']
                filename = Path(self.info_frag[q - 1]['file'])
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
                
                while self.info_frag[q - 1]['n_retries'] < self._MAX_RETRIES:
    
                    try: 
                        
                        async with aiofiles.open(filename, mode='ab') as f:
                            
                            async with self.client.stream("GET", url, headers=headers, timeout=30) as res:
                                                 
                            
                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: {res.status_code} {res.reason_phrase}")
                                if res.status_code == 403:                                                   
                                    raise AsyncHLSDLErrorFatal(f"Frag:{str(q)} resp code:{str(res)}")
                                elif res.status_code >= 400:
                                    raise AsyncHLSDLError(f"Frag:{str(q)} resp code:{str(res)}")                                
                                else:
                                        
                                    _hsize = int_or_none(res.headers.get('content-length'))
                                    if _hsize:
                                        self.info_frag[q-1]['headersize'] = _hsize
                                    else:
                                        raise AsyncHLSDLErrorFatal(f"Frag:{str(q)} _hsize is None")                                    
                                    
                                    if self.info_frag[q-1]['downloaded']:                                    
                                        
                                        if (await asyncio.to_thread(filename.exists)):
                                            _size = self.info_frag[q-1]['size'] = (await asyncio.to_thread(filename.stat)).st_size
                                            if _size and  (_hsize - 5 <= _size <= _hsize + 5):                            
                                    
                                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: Already DL with hsize[{_hsize}] and size [{_size}] check[{_hsize - 5 <=_size <= _hsize + 5}]")                                    
                                                break
                                            else:
                                                await f.truncate()
                                                self.info_frag[q-1]['downloaded'] = False
                                                async with self._LOCK:
                                                    self.n_dl_fragments -= 1
                                                    self.down_size -= _size
                                                    self.down_temp -= _size
                                                    async with self.video_downloader.lock:
                                                        self.video_downloader.info_dl['down_size'] -= _size                                                            
                                        else:
                                            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}:] frag with mark downloaded but file doesnt exists")
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
                                    self.info_frag[q - 1]['statistics'] = []
                                    
                                    _started = time.monotonic()
                                    async for chunk in res.aiter_bytes(chunk_size=_chunk_size): 
                                        
                                        _timechunk = time.monotonic() - _started
                                        self.info_frag[q - 1]['time2dlchunks'].append(_timechunk)                             
                                        #await asyncio.sleep(0)
                                        if cipher: data = cipher.decrypt(chunk)
                                        else: data = chunk 
                                        await f.write(data)
                                        
                                        async with self._LOCK:
                                            self.down_size += (_iter_bytes:=(res.num_bytes_downloaded - num_bytes_downloaded)) 
                                            async with self.video_downloader.lock:                                       
                                                self.video_downloader.info_dl['down_size'] += _iter_bytes 
                                        num_bytes_downloaded = res.num_bytes_downloaded
                                        self.info_frag[q - 1]['nchunks_dl'] += 1
                                        self.info_frag[q - 1]['sizechunks'].append(_iter_bytes)
                                        _median = median(self.info_frag[q-1]['time2dlchunks'])
                                        self.info_frag[q - 1]['statistics'].append(_median)
                                        if self.info_frag[q -1]['nchunks_dl'] > 10:
                            
                                            _time = self.info_frag[q -1]['time2dlchunks'][-5:]
                                            _max = [20*_el for _el in self.info_frag[q -1]['statistics'][-5:]]
                                            if set([_el1>_el2 for _el1,_el2 in zip(_time, _max)]) == {True}:
                                            
                                          
                                                raise AsyncHLSDLError(f"timechunk [{_time}] > [{_max}]=20*mean time accumulated for 5 consecutives chunks, nchunks[{self.info_frag[q -1]['nchunks_dl']}]")
                                                                               
                                        await asyncio.sleep(0)
                                        _started = time.monotonic()
                                    
                        _size = (await asyncio.to_thread(filename.stat)).st_size
                        _hsize = self.info_frag[q-1]['headersize']
                        if (_hsize - 5 <= _size <= _hsize + 5):
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
                        self.info_frag[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: fatalError: \n{'!!'.join(lines)}")
                        if (await asyncio.to_thread(filename.exists)):
                            _size = (await asyncio.to_thread(filename.stat)).st_size                            
                            await asyncio.to_thread(filename.unlink)
                        
                            async with self._LOCK:
                                self.down_size -= _size
                                self.down_temp -= _size
                                async with self.video_downloader.lock:                                       
                                    self.video_downloader.info_dl['down_size'] -= _size                                               
                        raise                 
                    except (asyncio.exceptions.CancelledError, asyncio.CancelledError, CancelledError) as e:
                        self.info_frag[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: CancelledError: \n{'!!'.join(lines)}")
                        if (await asyncio.to_thread(filename.exists)):
                            _size = (await asyncio.to_thread(filename.stat)).st_size                            
                            await asyncio.to_thread(filename.unlink)
                        
                            async with self._LOCK:
                                self.down_size -= _size
                                self.down_temp -= _size
                                async with self.video_downloader.lock:                                       
                                    self.video_downloader.info_dl['down_size'] -= _size
                        raise                   
                    except Exception as e:                        
                        self.info_frag[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        if not "httpx" in str(e.__class__) and not "AsyncHLSDLError" in str(e.__class__):
                            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: error {str(e.__class__)}")
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: error {repr(e)} \n{'!!'.join(lines)}")
                        self.info_frag[q - 1]['n_retries'] += 1
                        if (await asyncio.to_thread(filename.exists)):
                            _size = (await asyncio.to_thread(filename.stat)).st_size                            
                            await asyncio.to_thread(filename.unlink)
                        
                            async with self._LOCK:
                                self.down_size -= _size
                                self.down_temp -= _size
                                async with self.video_downloader.lock:                                       
                                    self.video_downloader.info_dl['down_size'] -= _size
                        
                        if self.info_frag[q - 1]['n_retries'] < self._MAX_RETRIES:
                                                     

                                await self.wait_time(random.choice([i for i in range(1,5)]))
                                await asyncio.sleep(0)
                                
                                
                        else:
                            self.info_frag[q - 1]['error'].append("MaxLimitRetries")
                            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:frag[{q}]:MaxLimitRetries:skip")                           
                            self.info_frag[q - 1]['skipped'] = True
                            break
                        

        finally:    
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: bye worker")
            async with self._LOCK:
                self.count -= 1
    
    async def fetch_async(self):
        
        self._LOCK = asyncio.Lock()
        self.frags_queue = asyncio.Queue()
        for frag in self.frags_to_dl:
            self.frags_queue.put_nowait(frag)        
        
        for _ in range(self.iworkers):
            self.frags_queue.put_nowait("KILL")

        n_frags_dl = 0

        while True:

            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] TASKS INIT")

            try:
                self.client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)

                for domain, value in self.cookies.items():
                    for _, _value in value["/"].items():
                        self.client._cookies.set(name=_value.__dict__['name'], value=_value.__dict__['value'], domain=domain)
                
                self.count = self.iworkers
                self.status = "downloading"
                self.down_temp = self.down_size
                self.started = time.monotonic()
                
                self.tasks = [asyncio.create_task(self.fetch(i), name=f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]") for i in range(self.iworkers)]
                
                done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)

                if pending:

                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] PENDING {pending}") 
                    for t in pending: t.cancel()
                    await asyncio.gather(*pending,return_exceptions=True)
                    await asyncio.sleep(0)
                    
                inc_frags_dl = (_nfragsdl:=len(self.fragsdl())) - n_frags_dl
                n_frags_dl = _nfragsdl
                
                if done:
                    _res = []
                    for t in done:
                        try:                            
                            t.result()  
                        except Exception as e:
                            _res.append(repr(e))                            
                    
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] DONE [{len(done)}] with exceptions {_res}")
                    for e in _res:
                        if (("AsyncHLSDLErrorFatal" in e) or ("CancelledError" in e)): raise AsyncHLSDLReset(e)
                        
            
            except Exception as e:
                
                lines = traceback.format_exception(*sys.exc_info())
                
                if "AsyncHLSDLReset" in  str(e.__class__):
                
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:fetch_async:RESET:{repr(e)}\n{'!!'.join(lines)}")

                    if self.n_reset < self._MAX_RESETS:

                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}] {e}")
                        await asyncio.sleep(0)
                        
                        try:
                            
                            await asyncio.to_thread(self.reset)
                            
                            self.frags_queue = asyncio.Queue()
                            for frag in self.frags_to_dl: self.frags_queue.put_nowait(frag)
                            for _ in range(self.iworkers): self.frags_queue.put_nowait("KILL")
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:OK:Pending frags {len(self.fragsnotdl())}") 
                            continue 
                            
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR reset couldnt progress:[{repr(e)}]\n{'!!'.join(lines)}")
                            self.status = "error"
                            await self.clean_when_error()
                            await asyncio.sleep(0)
                            raise AsyncHLSDLErrorFatal("ERROR reset couldnt progress")
                    
                    else:
                        
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR:Max_number_of_resets")  
                        self.status = "error"
                        await self.clean_when_error()
                        await asyncio.sleep(0)
                        raise AsyncHLSDLErrorFatal("ERROR max resets")     
                
                else:

                    logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:fetch_async:Exception {repr(e)} \n{'!!'.join(lines)}")

                
            else:
                
                if not self.fragsnotdl(): 
                    #todos los fragmentos en local
                    break
                else:                    
                    #raise AsyncHLSDLErrorFatal("reset") 
                    if (inc_frags_dl > 0):
                        
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_frags_dl} -> {inc_frags_dl}] new cycle with no fatal error")
                        try:
                            await asyncio.to_thread(self.reset)
                          
                            self.frags_queue = asyncio.Queue()
                            for frag in self.frags_to_dl: self.frags_queue.put_nowait(frag)
                            for _ in range(self.iworkers): self.frags_queue.put_nowait("KILL")
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET new cycle[{self.n_reset}]:OK:Pending frags {len(self.fragsnotdl())}") 
                            self.n_reset -= 1
                            continue 
                            
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR reset couldnt progress:[{repr(e)}]\n{'!!'.join(lines)}")
                            self.status = "error"
                            await self.clean_when_error()
                            await asyncio.sleep(0)
                            raise AsyncHLSDLErrorFatal("ERROR reset couldnt progress")
                        
                    else:
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_frags_dl} <-> {inc_frags_dl}] no improvement, lets raise an error")
                        
                        raise AsyncHLSDLError("no changes in number of dl frags in once cycle")
            finally:
                await self.client.aclose()
                del self.client


    
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Frags DL completed")


        self.status = "init_manipulating"

    async def clean_when_error(self):
        
        for f in self.info_frag:
            if f['downloaded'] == False:
                
                if (await asyncio.to_thread(f['file'].exists)):
                    await asyncio.to_thread(f['file'].unlink)
   
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
                        if f['size'] and (f['headersize'] - 5 <= f['size'] <= f['headersize'] + 5):                
              
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
                res.append(frag['frag'])
        return res
    
    def fragsdl(self):
        res = []
        for frag in self.info_frag:
            if (frag['downloaded'] == True) or frag.get('skipped', False):
                res.append({'frag': frag['frag'], 'headersize': frag['headersize'], 'size': frag['size']})
        return res

    def format_frags(self):
        import math        
        return f'{(int(math.log(self.n_total_fragments, 10)) + 1)}d'
    
    async def print_hookup(self):         
        
        if self.status == "done":
            msg = f"[HLS][{self.info_dict['format_id']}]: Completed \n"
        elif self.status == "init":
            msg = f"[HLS][{self.info_dict['format_id']}]: Waiting to DL [{naturalsize(self.filesize)}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"           
        elif self.status == "error":
            _rel_size_str = f'{naturalsize(self.down_size)}/{naturalsize(self.filesize)}' if self.filesize else '--'
            msg = f"[HLS][{self.info_dict['format_id']}]: ERROR [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "downloading":
            async with self._LOCK:
                _new = time.monotonic()                                  
                _speed = (self.down_size - self.down_temp) / (_new - self.started)
                _speed_ema = (_diff_size_ema:=self.ema_s(self.down_size - self.down_temp)) / (_diff_time_ema:=self.ema_t(_new - self.started)) 
                _speed_str = f'{naturalsize(_speed_ema,True)}ps'
                _progress_str = f'{(self.down_size/self.filesize)*100:5.2f}%' if self.filesize else '-----'
                        
                if _speed_ema and self.filesize:
                    
                    if (_est_time:=((self.filesize - self.down_size)/_speed_ema)) < 3600:
                        _eta = datetime.timedelta(seconds=_est_time)                    
                        _eta_str = ":".join([_item.split(".")[0] for _item in f"{_eta}".split(":")[1:]])
                    else: _eta_str = "--"
                else: _eta_str = "--"
                
                self.down_temp = self.down_size
                self.started = time.monotonic()             
                
            msg = f"[HLS][{self.info_dict['format_id']}]:(WK[{self.count:2d}]) DL[{_speed_str}] FR[{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}] PR[{_progress_str}] ETA[{_eta_str}]\n"
            
            
        elif self.status == "init_manipulating":                   
            msg = f"[HLS][{self.info_dict['format_id']}]: Waiting for Ensambling \n"
        elif self.status == "manipulating":
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0  
            _str = f'[{naturalsize(_size)}/{naturalsize(self.filesize)}]({(_size/self.filesize)*100:.2f}%)' if self.filesize else f'[{naturalsize(_size)}]'       
            msg = f"[HLS][{self.info_dict['format_id']}]: Ensambling {_str} \n"
        
        
        
        return msg