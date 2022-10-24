import asyncio
from concurrent.futures import CancelledError

import httpx
import sys
import traceback
from shutil import rmtree

from pathlib import Path

from urllib.parse import urljoin
import logging

import random

from utils import (EMA, async_ex_in_executor, async_wait_time, int_or_none,
                   naturalsize, print_norm_time, try_get, CONF_DASH_SPEED_PER_WORKER)

import aiofiles
import aiofiles.os as os
import datetime
from statistics import median
import copy
import time
from queue import Queue

from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("async_DASH_DL")

class AsyncDASHDLErrorFatal(Exception):
    

    def __init__(self, msg):
        
        super(AsyncDASHDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncDASHDLError(Exception):
   

    def __init__(self, msg):
        
        super(AsyncDASHDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception
        
class AsyncDASHDLReset(Exception):
    
    def __init__(self, msg):
        
        super(AsyncDASHDLReset, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncDASHDownloader:

    _CHUNK_SIZE = 102400
    _MAX_RETRIES = 5
    _MAX_RESETS = 10
    _MIN_TIME_RESETS = 15
       
    def __init__(self, video_dict, vid_dl):


        self.info_dict = video_dict.copy()
        self.video_downloader = vid_dl
        self.n_workers = vid_dl.info_dl['n_workers'] 
        self.count = 0 #cuenta de los workers activos haciendo DL. Al comienzo serán igual a n_workers
        self.video_url = self.info_dict.get('url')
        self.webpage_url = self.info_dict.get('webpage_url')
        self.fragment_base_url = self.info_dict.get('fragment_base_url')

        self.id = self.info_dict['id']
        
        self.ytdl = vid_dl.info_dl['ytdl']

        self.verifycert = not self.ytdl.params.get('nocheckcertificate')
        

        self._proxy = None
        
        self.timeout = httpx.Timeout(30, connect=30)
        self.limits = httpx.Limits(max_keepalive_connections=None, max_connections=None, keepalive_expiry=30)
        self.headers = self.info_dict.get('http_headers')
        self.base_download_path = Path(str(self.info_dict['download_path']))
        _filename = self.info_dict.get('_filename') or self.info_dict.get('filename')
        self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
        self.download_path.mkdir(parents=True, exist_ok=True) 
        self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + '.' + self.info_dict['ext'])


        #self.key_cache = dict()
        
        self.n_reset = 0
        self.down_size = 0
        self.down_temp = 0
        self.status = "init"
        self.error_message = "" 
        self.init()
        
        self.ema_s = EMA(smoothing=0.0001)
        self.ema_t = EMA(smoothing=0.0001)
        
        
        self.ex_dashdl = ThreadPoolExecutor(thread_name_prefix="ex_dashdl")


        self.init_client = httpx.Client(proxies=self._proxy, follow_redirects=True, headers=self.headers, limits=self.limits, timeout=self.timeout, verify=False)

        self.init()


    def init(self):

        self.info_frag = []
        self.frags_to_dl = []
        
        self.n_dl_fragments = 0
        
        self.tbr = self.info_dict.get('tbr', 0) #for audio streams tbr is not present
        self.abr = self.info_dict.get('abr', 0)
        _br = self.tbr or self.abr

        for i, fragment in enumerate(self.info_dict['fragments']):
                                
            if not (_url:=fragment.get('url')):
                _url = urljoin(self.fragment_base_url, fragment['path'])
            _file_path =  Path(self.download_path, fragment['path'])
            
            est_size = int(_br * fragment.get('duration', 0) * 1000 / 8)
            if _file_path.exists():
                size = _file_path.stat().st_size
                self.info_frag.append({"frag" : i+1, "url" : _url, "file" : _file_path, "downloaded" : True, "estsize" : est_size, "headersize" : None, "size": size, "n_retries": 0, "error" : ["AlreadyDL"]})
                 
                self.n_dl_fragments += 1
                self.down_size += size
                self.frags_to_dl.append(i+1)
                
            else:
                self.info_frag.append({"frag" : i+1, "url" : _url, "file" : _file_path, "downloaded" : False, "estsize" : est_size, "headersize": None, "size": None, "n_retries": 0, "error" : []})
                self.frags_to_dl.append(i+1)


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
       
        self._CONF_DASH_MIN_N_TO_CHECK_SPEED= 30
                                         
    def calculate_duration(self):
        self.totalduration = 0
        for fragment in self.info_dict['fragments']:
            self.totalduration += fragment.get('duration', 0)
            
    def calculate_filesize(self):
        _bitrate = self.tbr or self.abr                
        self.filesize = int(self.totalduration * 1000 * _bitrate / 8)
        
    
    def reset(self):         

        
        self.ema_s = EMA(smoothing=0.0001)
        self.ema_t = EMA(smoothing=0.0001)
        count = 0
        
        while (count < 5):
        
            try:
            
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{count}]:get video dict: {self.webpage_url}")
                
                try:

                    _info = self.ytdl.sanitize_info(self.ytdl.extract_info(self.webpage_url, download=False))
                    info_reset = _info['entries'][0] if (_info.get('_type') == 'playlist') else _info
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:New info video\n{info_reset}")
                    
                except Exception as e:
                    raise AsyncDASHDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:fails no descriptor {e}")

                if not info_reset:
                    raise AsyncDASHDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]: fails no descriptor")         

                try: 
                    if info_reset.get('requested_formats'):
                        info_format = [_info_format for _info_format in info_reset['requested_formats'] if _info_format['format_id'] == self.info_dict['format_id']]
                        self.prep_reset(info_format[0])
                    else: self.prep_reset(info_reset)
                    self.n_reset += 1
                    break
                except Exception as e:
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]: Exception occurred when reset: {repr(e)}")
                    raise AsyncDASHDLErrorFatal("RESET fails: preparation segs failed")
            except Exception as e:
                raise
            finally:
                count += 1
                if count == 5: raise AsyncDASHDLErrorFatal("Reset failed")
                self.init_client.close()


    def prep_reset(self, info_reset):       
       
        self.headers = self.info_dict['http_headers'] = info_reset.get('http_headers')
        self.init_client = httpx.Client(proxies=self._proxy, follow_redirects=True, headers=self.headers, limits=self.limits, timeout=self.timeout, verify=False)
        self.video_url = self.info_dict['url'] = info_reset.get('url')
        self.webpage_url = self.info_dict['webpage_url'] = info_reset.get('webpage_url')
        self.fragment_base_url = self.info_dict['fragment_base_url'] = info_reset.get('fragment_base_url')

        self.frags_to_dl = []


        for i, fragment in enumerate(self.info_dict['fragments']):

            
            if not (_url:=fragment.get('url')):
                _url = urljoin(self.fragment_base_url, fragment['path'])
            _file_path =  Path(self.download_path, fragment['path'])            



            if not self.info_frag[i]['downloaded'] or (self.info_frag[i]['downloaded'] and not self.info_frag[i]['headersize']):
                self.frags_to_dl.append(i+1)                        
                self.info_frag[i]['url'] = _url
                self.info_frag[i]['file'] = _file_path
                if not self.info_frag[i]['downloaded'] and self.info_frag[i]['file'].exists(): 
                    self.info_frag[i]['file'].unlink()
                
                self.info_frag[i]['n_retries'] = 0



        if not self.frags_to_dl:
            self.status = "init_manipulating"
        else:
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:prep_reset:OK {self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}")


    async def check_speed(self):
        
        def getter(x):
            return x*CONF_DASH_SPEED_PER_WORKER

        
        try:
            while True:
                done, pending = await asyncio.wait([asyncio.create_task(self.video_downloader.reset_event.wait()), asyncio.create_task(self.video_downloader.stop_event.wait()), asyncio.create_task(self._qspeed.get())], return_when=asyncio.FIRST_COMPLETED)
                for _el in pending: _el.cancel()
                await asyncio.wait(pending)
                if any([self.video_downloader.reset_event.is_set(), self.video_downloader.stop_event.is_set()]):
                    logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] event detected")
                    
                    break
                _speed = try_get(list(done), lambda x: x[0].result())
                if _speed == "KILL":
                    break
                self._speed.append(_speed)
                if len(self._speed) > self._CONF_DASH_MIN_N_TO_CHECK_SPEED:    
                
                    if any([all([el == 0 for el in self._speed[self._CONF_DASH_MIN_N_TO_CHECK_SPEED // 2:]]), (self._speed[self._CONF_DASH_MIN_N_TO_CHECK_SPEED // 2:] == sorted(self._speed[self._CONF_DASH_MIN_N_TO_CHECK_SPEED // 2:], reverse=True)), all([el < getter(self.n_workers) for el in self._speed[-self._CONF_DASH_MIN_N_TO_CHECK_SPEED:]])]):
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] speed reset: n_el_speed[{len(self._speed)}]\n{self._speed[-self._CONF_DASH_MIN_N_TO_CHECK_SPEED:]}")
                        self.video_downloader.reset_event.set()
                        
                        break
                    
                    else: self._speed = self._speed[1:]
                
                await asyncio.sleep(0)
            
            await asyncio.sleep(0)
        
        except Exception as e:
            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] {repr(e)}")
        finally:
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] bye")


                
    async def fetch(self, nco):

        try:

            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: init worker")
            
            while not any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):

                q = await self.frags_queue.get()
                
                if any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                    return

                if q == "KILL":
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: KILL")
                    break  

                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]\n{self.info_frag[q - 1]}")

                if self.video_downloader.pause_event.is_set():
                    done, pending = await asyncio.wait([asyncio.create_task(self.video_downloader.resume_event.wait()), asyncio.create_task(self.video_downloader.reset_event.wait()), asyncio.create_task(self.video_downloader.stop_event.wait())], return_when=asyncio.FIRST_COMPLETED)
                    for _el in pending: _el.cancel()
                    await asyncio.wait(pending)
                    self.video_downloader.pause_event.clear()
                    self.video_downloader.resume_event.clear()
                    
                    self.ema_s = EMA(smoothing=0.0001)
                    self.ema_t = EMA(smoothing=0.0001)
                    
                    if any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                        return
                    


                url = self.info_frag[q - 1]['url']
                filename = Path(self.info_frag[q - 1]['file'])
                filename_exists = await os.path.exists(filename)
                # key = self.info_frag[q - 1]['key']
                # cipher = None
                # if key is not None and key.method == 'AES-128':
                #     iv = binascii.unhexlify(key.iv[2:])
                #     cipher = AES.new(self.key_cache[key.absolute_uri], AES.MODE_CBC, iv)
                byte_range = self.info_frag[q - 1].get('byterange')
                headers = {}
                if byte_range:
                    headers['range'] = f"bytes={byte_range['start']}-{byte_range['end'] - 1}"

                await asyncio.sleep(0)        
                
                while ((self.info_frag[q - 1]['n_retries'] < self._MAX_RETRIES) and not any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()])):

                    try: 

                        async with aiofiles.open(filename, mode='ab') as f:                            

                            async with self.client.stream("GET", url, headers=headers, timeout=5) as res:
                                                 
                            
                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: {res.status_code} {res.reason_phrase}")
                                
                                if res.status_code == 403:
                                    self.video_downloader.reset_event.set()                                                   
                                    raise AsyncDASHDLErrorFatal(f"Frag:{str(q)} resp code:{str(res)}")
                                elif res.status_code >= 400:
                                    raise AsyncDASHDLError(f"Frag:{str(q)} resp code:{str(res)}")                                
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
                                        self.video_downloader.reset_event.set()
                                        raise AsyncDASHDLErrorFatal(f"Frag:{str(q)} _hsize is None")                                    
                                    
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
                                        if any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                                        #if any([self.video_downloader.reset_event.is_set(), self.video_downloader.stop_event.is_set()]):
                                            raise AsyncDASHDLErrorFatal("event")
                                                                                    
                                        _timechunk = time.monotonic() - _started
                                        self.info_frag[q - 1]['time2dlchunks'].append(_timechunk)                             
                                        # #await asyncio.sleep(0)
                                        # if cipher: data = cipher.decrypt(chunk)
                                        # else: data = chunk 
                                        await f.write(chunk)
                                        
                                        async with self._LOCK:
                                            self.down_size += (_iter_bytes:=(res.num_bytes_downloaded - num_bytes_downloaded))
                                            if (_dif:=self.down_size - self.filesize) > 0: 
                                                    self.filesize += _dif                                            
                                        async with self.video_downloader.alock:
                                            if _dif > 0:    
                                                self.video_downloader.info_dl['filesize'] += _dif
                                            self.video_downloader.info_dl['down_size'] += _iter_bytes
                                                 
                                        num_bytes_downloaded = res.num_bytes_downloaded
                                        self.info_frag[q - 1]['nchunks_dl'] += 1
                                        self.info_frag[q - 1]['sizechunks'].append(_iter_bytes)
                                                                               
                                        await asyncio.sleep(0)
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
                            raise AsyncDASHDLError(f"fragment not completed frag[{q}]")     
          
                                                       
                    except AsyncDASHDLErrorFatal as e:
                                                        
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
                        
                        #await asyncio.sleep(0)
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
                        if not "httpx" in str(e.__class__) and not "AsyncDASHDLError" in str(e.__class__):
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
            async with self._LOCK:
                self.count -= 1
    
    async def fetch_async(self):
        
        
        self._LOCK = asyncio.Lock()        
        self.frags_queue = asyncio.Queue()
        for frag in self.frags_to_dl:
            self.frags_queue.put_nowait(frag)        
        
        for _ in range(self.n_workers):
            self.frags_queue.put_nowait("KILL")

        n_frags_dl = 0

        
        _tstart = time.monotonic() 
        
        try:
                    
            while True:

                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] TASKS INIT")

                try:
                    self.client = httpx.AsyncClient(proxies=self._proxy, limits=self.limits, follow_redirects=True, timeout=self.timeout, verify=self.verifycert, headers=self.headers)

                    
                    self.count = self.n_workers
                    self.down_temp = self.down_size
                    self.started = time.monotonic()
                    self.status = "downloading"                    
                    self.video_downloader.reset_event.clear()
                    self._speed = []
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

                                logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]")

                                try:

                                    await async_ex_in_executor(self.ex_dashdl, self.reset)
                                    self.frags_queue = asyncio.Queue()
                                    for frag in self.frags_to_dl: self.frags_queue.put_nowait(frag)
                                    if ((_t:=time.monotonic()) - _tstart) < self._MIN_TIME_RESETS:
                                        self.n_workers -= self.n_workers // 4
                                    _tstart = _t
                                    for _ in range(self.n_workers): self.frags_queue.put_nowait("KILL")
                                    await self.client.aclose()
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:OK:Pending frags {len(self.fragsnotdl())}")
                                    await asyncio.sleep(0)
                                    continue 
                                    
                                except Exception as e:
                                    lines = traceback.format_exception(*sys.exc_info())
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR reset couldnt progress:[{repr(e)}]\n{'!!'.join(lines)}")
                                    self.status = "error"
                                    await self.clean_when_error()
                                    raise AsyncDASHDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: ERROR reset couldnt progress")
                            
                            else:
                                
                                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR:Max_number_of_resets")  
                                self.status = "error"
                                await self.clean_when_error()
                                await asyncio.sleep(0)
                                raise AsyncDASHDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: ERROR max resets")     

                        else:                

                            self._qspeed.put_nowait("KILL")
                            await asyncio.wait(check_task)
                            if (inc_frags_dl > 0):
                                
                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_frags_dl} -> {inc_frags_dl}] new cycle with no fatal error")
                                try:
                                    await async_ex_in_executor(self.ex_dashdl, self.reset)
                                    self.frags_queue = asyncio.Queue()
                                    for frag in self.frags_to_dl: self.frags_queue.put_nowait(frag)
                                    for _ in range(self.n_workers): self.frags_queue.put_nowait("KILL")
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET new cycle[{self.n_reset}]:OK:Pending frags {len(self.fragsnotdl())}") 
                                    self.n_reset -= 1
                                    await self.client.aclose()
                                    continue 
                                    
                                except Exception as e:
                                    lines = traceback.format_exception(*sys.exc_info())
                                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ERROR reset couldnt progress:[{repr(e)}]\n{'!!'.join(lines)}")
                                    self.status = "error"
                                    await self.clean_when_error()
                                    await asyncio.sleep(0)
                                    raise AsyncDASHDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: ERROR reset couldnt progress")
                                
                            else:
                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_frags_dl} <-> {inc_frags_dl}] no improvement, lets raise an error")
                                
                                raise AsyncDASHDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: no changes in number of dl frags in one cycle")
                            
                except Exception as e:
                    logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch_async] error {repr(e)}")
                finally:                   
                    await self.client.aclose()                    
                    await asyncio.sleep(0)


        except Exception as e:
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] error {repr(e)}")
        finally:
            if self._proxy:
                self.video_downloader.hosts_dl[self._host]['queue'].put_nowait(self._index)
                #self.video_downloader.hosts_dl[self._host]['count'] -= 1
                _ytdl_opts = self.ytdl.params.copy()
                _proxy = self._proxy['http://']
                with ProxyYTDL(opts=_ytdl_opts, proxy=_proxy, quiet=False) as proxy_ytdl: #logout
                    _info = proxy_ytdl.sanitize_info(proxy_ytdl.extract_info(self.webpage_url, download=False))
            self.init_client.close()            
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Frags DL completed")
            self.status = "init_manipulating"
            self.ex_dashdl.shutdown(wait=False, cancel_futures=True)
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
                        if f['size'] and (f['headersize'] - 100 <= f['size'] <= f['headersize'] + 100):                
              
                            with open(f['file'], 'rb') as source:
                                dest.write(source.read())
                        else: raise AsyncDASHDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling: {f}")
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
            raise AsyncDASHDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling parts")

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
    
    def print_hookup(self):

       
        _filesize_str = naturalsize(self.filesize) if self.filesize else "--"
        
        if self.status == "done":
            msg = f"[DASH][{self.info_dict['format_id']}]:  Completed \n"
        elif self.status == "init":
            msg = f"[DASH][{self.info_dict['format_id']}]:  Waiting to DL [{_filesize_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"           
        elif self.status == "error":
            _rel_size_str = f'{naturalsize(self.down_size)}/{naturalsize(self.filesize)}' if self.filesize else '--'
            msg = f"[DASH][{self.info_dict['format_id']}]:  ERROR [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "stop":
            _rel_size_str = f'{naturalsize(self.down_size)}/{naturalsize(self.filesize)}' if self.filesize else '--'
            msg = f"[DASH][{self.info_dict['format_id']}]:  STOPPED [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "downloading":
            
            _down_size = self.down_size
            _new = time.monotonic()                                  
            _speed = (_down_size - self.down_temp) / (_new - self.started)
            _speed_ema = (_diff_size_ema:=self.ema_s(_down_size - self.down_temp)) / (_diff_time_ema:=self.ema_t(_new - self.started))
            
            if not any([self.video_downloader.reset_event and self.video_downloader.reset_event.is_set(), self.video_downloader.stop_event and self.video_downloader.stop_event.is_set(), self.video_downloader.pause_event and self.video_downloader.pause_event.is_set()]):
                if self._qspeed: self._qspeed.put_nowait(_speed)

            if self.video_downloader.pause_event and self.video_downloader.pause_event.is_set():
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
                
            msg = f"[DASH][{self.info_dict['format_id']}]:  WK[{self.count:2d}/{self.n_workers:2d}] DL[{_speed_str}] FR[{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}] PR[{_progress_str}] ETA[{_eta_str}]\n"
            
            
        elif self.status == "init_manipulating":                   
            msg = f"[DASH][{self.info_dict['format_id']}]: Waiting for Ensambling \n"
        elif self.status == "manipulating":
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0  
            _str = f'[{naturalsize(_size)}/{naturalsize(self.filesize)}]({(_size/self.filesize)*100:.2f}%)' if self.filesize else f'[{naturalsize(_size)}]'       
            msg = f"[DASH][{self.info_dict['format_id']}]: Ensambling {_str} \n"       
        
        
        return msg
        
        _filesize_str = naturalsize(self.filesize) if self.filesize else "--" 
        if self.status == "done":
            msg = f"[DASH][{self.info_dict['format_id']}]: Completed \n"
        elif self.status == "init":
            msg = f"[DASH][{self.info_dict['format_id']}]: Waiting to DL [{_filesize_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"           
        elif self.status == "error":
            _rel_size_str = f'{naturalsize(self.down_size)}/{naturalsize(self.filesize)}' if self.filesize else '--'
            msg = f"[DASH][{self.info_dict['format_id']}]: ERROR [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        elif self.status == "stop":
            _rel_size_str = f'{naturalsize(self.down_size)}/{naturalsize(self.filesize)}' if self.filesize else '--'
            msg = f"[DASH][{self.info_dict['format_id']}]: STOPPED [{_rel_size_str}] [{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}]\n"
        
        elif self.status == "downloading":
            
            _down_size = self.down_size
            _new = time.monotonic()                                  
            _speed = (_down_size - self.down_temp) / (_new - self.started)
            _speed_ema = (_diff_size_ema:=self.ema_s(_down_size - self.down_temp)) / (_diff_time_ema:=self.ema_t(_new - self.started))
            if self.video_downloader.pause_event and self.video_downloader.pause_event.is_set():
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
                        
                
            msg = f"[DASH][{self.info_dict['format_id']}]:(WK[{self.count:2d}]) DL[{_speed_str}] FR[{self.n_dl_fragments:{self.format_frags()}}/{self.n_total_fragments}] PR[{_progress_str}] ETA[{_eta_str}]\n"
            
            
        elif self.status == "init_manipulating":                   
            msg = f"[DASH][{self.info_dict['format_id']}]: Waiting for Ensambling \n"
        elif self.status == "manipulating":
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0  
            _str = f'[{naturalsize(_size)}/{naturalsize(self.filesize)}]({(_size/self.filesize)*100:.2f}%)' if self.filesize else f'[{naturalsize(_size)}]'       
            msg = f"[DASH][{self.info_dict['format_id']}]: Ensambling {_str} \n"
        
        
        
        return msg