import asyncio
import copy
import datetime
import logging
import sys
import time
import traceback
from concurrent.futures import CancelledError, ThreadPoolExecutor
from pathlib import Path
from shutil import rmtree
from statistics import median

import aiofiles
import httpx

from utils import (EMA, int_or_none, naturalsize, none_to_zero, try_get, 
                   async_ex_in_executor, limiter_non,
                   dec_retry_error, traverse_obj, CONFIG_EXTRACTORS, get_domain)


from threading import Lock

from urllib.parse import unquote

logger = logging.getLogger("async_http_DL")

class AsyncHTTPDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        
        super().__init__(msg)

        self.exc_info = exc_info

class AsyncHTTPDLError(Exception):
    def __init__(self, msg, exc_info=None):
        
        super().__init__(msg)

        self.exc_info = exc_info 


class AsyncHTTPDownloader():
    
    _MIN_SIZE = 10485760 #10MB
    _CHUNK_SIZE = 102400 #100KB
    _MAX_RETRIES = 10    
    _CONFIG = CONFIG_EXTRACTORS.copy()   
    
    _EX_HTTPDL = ThreadPoolExecutor(thread_name_prefix="ex_httpdl")
    
    def __init__(self, video_dict, vid_dl):

             
        if not video_dict or not vid_dl: return
        self.info_dict = video_dict.copy()
        self.video_downloader = vid_dl

        self.video_url = self.info_dict.get('url')

        self.uris = [unquote(self.video_url)]

        self.ytdl = traverse_obj(self.video_downloader.info_dl, ('ytdl'))

        self.proxies = None

        self.verifycert = False
        self.timeout = httpx.Timeout(60, connect=60)
        
        self.limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)
        self.headers = self.info_dict.get('http_headers')  
        
        self.init_client = httpx.Client(limits=self.limits, follow_redirects=True, timeout=self.timeout, verify=self.verifycert, headers=self.headers)
        
        self.base_download_path = self.info_dict.get('download_path')
        if (_filename:=self.info_dict.get('_filename')):
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True) 
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
        else:
            _filename = self.info_dict.get('filename')
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])

        #self.filesize = none_to_zero(self.info_dict.get('filesize', 0))
        self.down_size = 0
        self.down_temp = 0
        
        self.n_parts_dl = 0        
        self.parts = []
        if self.filename.exists() and self.filename.stat().st_size > 0:
            self.status = "init_manipulating"
        else:
            self.status = "init"
        
        self.error_message = ""        

        self.count = 0#cuenta de los workers activos
        
        self.ema_s = EMA(smoothing=0.0001)
        self.ema_t = EMA(smoothing=0.0001)

        self.reset_event = None
        
        self.init()

    
    def init(self): 

        try:


            def getter(x):
                            
                value, key_text = try_get([(v,kt) for k,v in self._CONFIG.items() if any(x==(kt:=_) for _ in k)], lambda y: y[0]) or ("","") 
                if value:
                    return(value['ratelimit'].ratelimit(key_text, delay=True), value['maxsplits'])

            self.n_parts = traverse_obj(self.video_downloader.info_dl, ('n_workers'), default=16)
            
            _extractor = self.info_dict.get('extractor_key').lower()            
            self.auto_pasres = False            
            _sem = False
            if _extractor and _extractor.lower() != 'generic':
                self._decor, self.n_parts = getter(_extractor) or (limiter_non.ratelimit("transp", delay=True), self.n_parts)
                if _extractor in ['doodstream', 'vidoza']:
                    self.auto_pasres = True
                if self.n_parts < 16: 
                    _sem = True                

            else: 
                self._decor, self.n_parts = limiter_non.ratelimit("transp", delay=True), self.n_parts

            self._NUM_WORKERS = self.n_parts 

            self._host = get_domain(self.uris[0])
                
            if _sem:
                
                with self.ytdl.params['lock']:                
                    if not (_temp:=traverse_obj(self.ytdl.params, ('sem', self._host))):
                        _temp = Lock()
                        self.ytdl.params['sem'].update({self._host: _temp})
                        
                self.sem = _temp
                
                self.sem.acquire()
                
            else: 
                self.sem = None 
            
            
            _mult_ranges, _filesize = self.check_server()
            
            if not _mult_ranges:
                logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: server cant handle ranges")
                self.n_parts = 1
                self._NUM_WORKERS = 1
            
            if _filesize: 
                self.filesize = none_to_zero(_filesize)
                
            if self.filesize:
                
                self.create_parts() 
                self.get_parts_to_dl()
            
            else:                                
                raise AsyncHTTPDLErrorFatal("Can't get filesize")
        
        except (KeyboardInterrupt, Exception) as e:
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: {str(e)}")
            self.init_client.close()
            self.status = "error"
            self.error_message = repr(e)            
            raise
        finally:
            if self.sem:
                self.sem.release()
                
    def check_server(self):
        
        @dec_retry_error
        @self._decor    
        def _check_server():
            try:             
                res = self.init_client.head(self.uris[0], headers={'range': 'bytes=0-'})
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[check_server] {res} {res.request.headers} {res.headers}") 
                res.raise_for_status()
                return(res.headers.get('accept-ranges') or res.headers.get('content-range'), int_or_none(res.headers.get('content-length')))

            except Exception as e:
                lines = traceback.format_exception(*sys.exc_info())
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[check_server] {repr(e)} \n{'!!'.join(lines)}")
                raise
            
        if (_ar:=self.info_dict.get('accept_ranges')) and (_fs:=self.info_dict.get('filesize')):
            return((_ar, _fs))
        else:
            return (_check_server() or (None,None))
        
    def upt_hsize(self, i, offset=None):

        @dec_retry_error
        @self._decor
        def _upt_hsize():    
            try: 

                if offset:
                    self.parts[i]['headers'].append({'range' : f"bytes={self.parts[i]['offset'] + self.parts[i]['start']}-{self.parts[i]['end']}"})

                res = self.init_client.head(self.parts[i]['url'], headers=self.parts[i]['headers'][-1])
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[upt_hsize] {res} {res.request} {res.request.headers} {res.headers.get('content-length')}")
                res.raise_for_status()
                headers_size = int_or_none(res.headers.get('content-length'))
                if not offset: self.parts[i].update({'headersize' : headers_size})
                else: 
                    self.parts[i].update({'hsizeoffset' : headers_size})
                
                        
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[upt_hsize] OK for part[{i}] \n{self.parts[i]}")
                
                return headers_size
            
            except Exception as e:
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[upt_hsize] NOTOK for part[{i}] {repr(e)}")
    
        return _upt_hsize()
        
    def create_parts(self):       
       
        if (_partsize:=self.filesize // self.n_parts) < self._MIN_SIZE: #size of parts cant be less than _MIN_SIZE
            temp = self.filesize // self._MIN_SIZE + 1            
            logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: size parts [{_partsize}] < {self._MIN_SIZE} -> change nparts [{self.n_parts} -> {temp}]")
            self.n_parts = temp
            self._NUM_WORKERS = self.n_parts            
 
        try:
            start_range = 0
            for i in range(1, self.n_parts+1):
                
                if i == self.n_parts:
                    self.parts.append({'part': i, 'offset': 0, 'start': start_range, 'end': '', 'headers' : [{'range' : f'bytes={start_range}-'}], 'downloaded': False, 
                                    'filepath': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                    'tempfilesize': (_tfs:=(self.filesize // self.n_parts + self.filesize % self.n_parts)), 'headersize' : None, 'hsizeoffset': None, 'size' : -1,
                                    'nchunks': ((_nchunks:=(_tfs // self._CHUNK_SIZE)) + 1) if (_tfs % self._CHUNK_SIZE) else _nchunks, 'nchunks_dl': dict(), 'n_retries': 0, 'time2dlchunks': dict(), 'statistics': dict()})             
                else:
                    end_range = start_range + (self.filesize//self.n_parts - 1)
                    self.parts.append({'part': i , 'offset': 0, 'start': start_range, 'end': end_range, 'headers' : [{'range' : f'bytes={start_range}-{end_range}'}], 'downloaded' : False,
                                    'filepath': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                    'tempfilesize': (_tfs:=(self.filesize // self.n_parts)), 'headersize' : None, 'hsizeoffset': None, 'size': -1, 
                                    'nchunks': ((_nchunks:=(_tfs // self._CHUNK_SIZE)) + 1) if (_tfs % self._CHUNK_SIZE) else _nchunks, 'nchunks_dl': dict(), 'n_retries': 0, 'time2dlchunks': dict(), 'statistics': dict()})
                    
                    
                    start_range = end_range + 1
            
            for i,part in enumerate(self.parts):
                part.update({'url': self.uris[i % len(self.uris)]})     
            
            if self.n_parts == 1: self.parts[0].update({'headersize' : self.filesize})
            else:
                with ThreadPoolExecutor(thread_name_prefix="ex_httphsize") as ex:
                #with AsyncHTTPDownloader._EX_HTTPDL as ex:
                    fut = [ex.submit(self.upt_hsize, i) for i in range(self.n_parts)]
                    #done, pending = wait(fut, return_when=ALL_COMPLETED)
                
            _not_hsize = [_part for _part in self.parts if not _part['headersize']]
            if len(_not_hsize) > 0: logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[create parts] not headersize in [{len(_not_hsize)}/{self.n_parts}]")

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[create parts] {repr(e)} \n{'!!'.join(lines)}")

    def get_parts_to_dl(self):
        
        self.parts_to_dl = []

        for i, part in enumerate(self.parts):
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue]\n{part}")
            if not part['filepath'].exists():
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] doesn't exits, lets DL")
                self.parts_to_dl.append(part['part'])
            else:
                partsize = part['filepath'].stat().st_size
                _headersize = part.get('headersize')
                if not _headersize:
                    _headersize = self.upt_hsize(i)
                 
                if partsize == 0:
                    part['filepath'].unlink()
                    self.parts_to_dl.append(part['part'])
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize}. Re-download from scratch")
                       
                elif _headersize:
                    if (partsize > _headersize + 100):
                        part['filepath'].unlink()
                        self.parts_to_dl.append(part['part'])
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize}. Re-download from scratch")
                        
                    elif _headersize - 100 <= partsize <= _headersize + 100: #with a error margen of +-100bytes,file is fully downloaded
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize} and full downloaded")
                        self.down_size += partsize                        
                        part['downloaded'] = True
                        part['size'] = partsize
                        self.n_parts_dl += 1
                        continue
                    else: #there's something previously downloaded
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize} and not full downloaded {_headersize}. Re-define header range to start from the dl size")
                        _old_part = part 
                        part['offset'] = partsize                        
                        #part['headers'] = {'range' : f"bytes={part['offset']}-{part['end']}"}
                        _hsizeoffset = self.upt_hsize(i, offset=True)
                        if _hsizeoffset:
                            part['hsizeoffset'] = _hsizeoffset
                            self.parts_to_dl.append(part['part'])
                            self.down_size += partsize 
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue]\n{part}\n{self.parts[i]}")
                        else:
                            part = _old_part
                            part['filepath'].unlink()
                            self.parts_to_dl.append(part['part'])
                            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] couldnt get new headersize. Re-download from scratch")
                            
                        
                else:
                    
                    logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize} but without headersize. Re-download")
                    part['filepath'].unlink()
                    self.parts_to_dl.append(part['part'])
                

            
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[get_parts_to_dl] \n{list(self.parts_to_dl)}")  
        
        if not self.parts_to_dl:
            self.status = "manipulating"      

    async def wait_time(self, n):
   
        _started = time.monotonic()
        while True:
            if (_t:=(time.monotonic() - _started)) >= n:
                return _t
            else:
                await asyncio.sleep(0)
        
    async def rate_limit(self):
        
        @self._decor
        async def _rate_limit():
            await asyncio.sleep(0)
            
        await _rate_limit()
    
    async def fetch(self,i):
        
        try:

            client = httpx.AsyncClient(proxies=try_get(self.proxies, lambda x: x[i]), limits=self.limits, follow_redirects=True, timeout=self.timeout, verify=self.verifycert, headers=self.headers)
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}] launched")
            
            while True:

                part = await self.parts_queue.get()     
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][worker-{i}]:part[{part}]")       
                if part == "KILL": break            
                tempfilename = self.parts[part-1]['filepath']

                await asyncio.sleep(0)
          
                while True: #bucle del worker
                        
                    try:
                    
                        async with aiofiles.open(tempfilename, mode='ab') as f:
                        
                            await self.rate_limit()
                                                       
                            async with client.stream("GET", self.parts[part-1]['url'], headers=self.parts[part-1]['headers'][-1]) as res:
                            
                                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:part[{part}]: [fetch] resp code {str(res.status_code)}: rep {self.parts[part-1]['n_retries']}\n{res.request.headers}")

                                nth_key = str(self.parts[part-1]['n_retries'])
                                self.parts[part-1]['nchunks_dl'].update({nth_key: 0})
                                self.parts[part-1]['time2dlchunks'].update({nth_key : []})
                                self.parts[part-1]['statistics'].update({nth_key : []})
                                
                                if res.status_code >= 400:
                                    if self.parts[part-1]['n_retries'] == self._MAX_RETRIES:
                                        raise AsyncHTTPDLError(f"error[{res.status_code}] part[{part}]")
                                    else:
                                        self.parts[part-1]['n_retries'] += 1
                                        await self.wait_time(5) 
                                        continue
                                else:
                                    if (len(self.parts[part-1]['headers']) == 1) and (not self.parts[part-1]['headersize']):
                                        self.parts[part-1]['headersize'] = int_or_none(res.headers.get('content-length'))
                                    
                                    num_bytes_downloaded = res.num_bytes_downloaded
                                                                        
                                    _started = time.monotonic()
                                    
                                    async for chunk in res.aiter_bytes(chunk_size=self._CHUNK_SIZE): 
                                    
                                        _timechunk = time.monotonic() - _started 
                                        self.parts[part-1]['time2dlchunks'][nth_key].append(_timechunk)
                                        await f.write(chunk)                                        
                                           
                                        async with self._ALOCK:
                                            self.down_size += (_iter_bytes:=(res.num_bytes_downloaded - num_bytes_downloaded)) 
                                            async with self.video_downloader.alock:                                       
                                                self.video_downloader.info_dl['down_size'] += _iter_bytes
                                        num_bytes_downloaded = res.num_bytes_downloaded
                                        self.parts[part-1]['nchunks_dl'][nth_key] += 1
                                        _median = median(self.parts[part-1]['time2dlchunks'][nth_key])
                                        self.parts[part-1]['statistics'][nth_key].append(_median)
                                                
                                        if self.parts[part-1]['nchunks_dl'][nth_key] > 50:
                                        
                                            _time = self.parts[part -1]['time2dlchunks'][nth_key][-25:]
                                            _max = [1000*_el for _el in self.parts[part -1]['statistics'][nth_key][-25:]]
                                            if set([_el1>_el2 for _el1,_el2 in zip(_time, _max)]) == {True}:
                                                raise AsyncHTTPDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:[fetch-stream] part[{part}] iter[{nth_key}] error max time for consecutives chunks [{_time}] > [{_max}]")
                                        
                                        await asyncio.sleep(0)
                                        if self.video_downloader.stop_event.is_set():
                                            return
                                        if self.video_downloader.pause_event.is_set():
                                            await self.video_downloader.resume_event.wait()
                                            self.video_downloader.pause_event.clear()
                                            self.video_downloader.resume_event.clear()
                                            self.ema_s = EMA(smoothing=0.0001)
                                            self.ema_t = EMA(smoothing=0.0001)
                                        _started = time.monotonic()

                        _tempfile_size = (await asyncio.to_thread(tempfilename.stat)).st_size                        
                        if (self.parts[part-1]['headersize'] - 100 <=  _tempfile_size <= self.parts[part-1]['headersize'] + 100):
                            self.parts[part-1]['downloaded'] = True
                            self.parts[part-1]['size'] =  _tempfile_size
                            async with self._ALOCK:
                                self.n_parts_dl += 1
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:part[{part}] OK DL: total {self.n_parts_dl}\n{self.parts[part-1]}")
                            break
                        
                        else:
                            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:[fetch-stream] part[{part}] end of stream not completed: {self.parts[part-1]['headersize'] - 100} <=  {_tempfile_size} <= {self.parts[part-1]['headersize'] + 100}")
                            
                            raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:[fetch-stream] part[{part}] end of stream not completed: {self.parts[part-1]['headersize'] - 100} <=  {_tempfile_size} <= {self.parts[part-1]['headersize'] + 100}") 
                        
                    except (asyncio.exceptions.CancelledError, asyncio.CancelledError, CancelledError, AsyncHTTPDLErrorFatal) as e:
                        
                        logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch-res] part[{part}] error: {repr(e)}")
                        raise
                            
                    except (AsyncHTTPDLError, httpx.HTTPError, httpx.StreamError) as e:                        
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch-res] part[{part}] error: {repr(e)}")                           
                            
                        if self.parts[part-1]['n_retries'] < self._MAX_RETRIES:
                            self.parts[part-1]['n_retries'] += 1
                            _tempfile_size = (await asyncio.to_thread(tempfilename.stat)).st_size                                
                            self.parts[part-1]['offset'] = _tempfile_size                               
                            if not self.upt_hsize(part - 1, offset=True):
                                raise AsyncHTTPDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch-res] part[{part}] Error to upt hsize part[{part}]")

                            await asyncio.sleep(0)
                        else:
                            logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch-res] part[{part}] error: maxnumrepeats")                            
                            raise AsyncHTTPDLErrorFatal(f"MaxNumRepeats part[{part}]")
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch-res] part[{part}] error unexpected: {repr(e)}\n{'!!'.join(lines)}") 
                
        finally:
            await client.aclose()
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}] says bye")
            self.count -= 1
            
    
    async def fetch_async(self):

        try:
            self._ALOCK = asyncio.Lock()    
            self.parts_queue = asyncio.Queue()
            
            for part in self.parts_to_dl:
                self.parts_queue.put_nowait(part)            
            
            for _ in range(self._NUM_WORKERS):
                self.parts_queue.put_nowait("KILL")

            await asyncio.sleep(0)

            self.count = self._NUM_WORKERS
            self.down_temp = self.down_size
            self.started = time.monotonic()
            self.status = "downloading"
            
            if self.sem: 
                await async_ex_in_executor(AsyncHTTPDownloader._EX_HTTPDL, self.sem.acquire)  
            
            self.tasks = [asyncio.create_task(self.fetch(i)) for i in range(self._NUM_WORKERS)]
            
            done, _ = await asyncio.wait(self.tasks)
            
            for d in done:
                try:
                    d.result()
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())                
                    logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {repr(e)}\n{'!!'.join(lines)}")


            if self.video_downloader.stop_event.is_set():
                self.status = "stop"
            else:
                if (_parts_not_dl:= await asyncio.to_thread(self.partsnotdl)):
                    self.status = "error"
            
                else:
                    self.status = "init_manipulating"  
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {repr(e)}\n{'!!'.join(lines)}")
        finally:
            if self.init_client:
                self.init_client.close()
            if self.sem:                 
                await async_ex_in_executor(AsyncHTTPDownloader._EX_HTTPDL, self.sem.release)

    def partsnotdl(self):
        res = []
        for part in self.parts:
            if part['downloaded'] == False:
                res.append(part['part'])
        return res
       
    def sync_clean_when_error(self):            
        for f in self.parts:
            if f['downloaded'] == False:
                if f['filepath'].exists():
                    f['filepath'].unlink()
                    
    def ensamble_file(self):        
   
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] start ensambling {self.filename}")
        
        try:           
        
            with open(self.filename, 'wb') as dest:
                for p in self.parts:
                    if p['filepath'].exists():
                        p['size'] = p['filepath'].stat().st_size
                        if (p['headersize'] - 100 <= p['size'] <= p['headersize'] + 100):
                            
                            with open(p['filepath'], 'rb') as source:
                                dest.write(source.read())
                        else: raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] error when ensambling: {p} ")
                    else:
                        raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] error when ensambling: {p} ")
                

        except Exception as e:
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] error when ensambling parts {str(e)}")
            if self.filename.exists(): self.filename.unlink()
            self.status = "error"
            self.sync_clean_when_error()               
            raise   
        
        if self.filename.exists():
            rmtree(str(self.download_path),ignore_errors=True)
            self.status = "done" 
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] file ensambled")               
        else:
            self.status = "error"  
            self.sync_clean_when_error()                        
            raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] error when ensambling parts")
            
    def format_parts(self):
        import math        
        return f'{(int(math.log(self.n_parts, 10)) + 1)}d'        
            
    def print_hookup(self):
        
        _filesize_str = naturalsize(self.filesize) if self.filesize else "--"      
        if self.status == "done":
            return (f"[HTTP][{self.info_dict['format_id']}]: Completed\n")
        elif self.status == "init":
            return (f"[HTTP][{self.info_dict['format_id']}]: Waiting to DL [{_filesize_str}][{self.n_parts_dl} of {self.n_parts}]\n")            
        elif self.status == "error":
            return (f"[HTTP][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size)} [{_filesize_str}][{self.n_parts_dl} of {self.n_parts}]")
        elif self.status == "stop":
            return (f"[HTTP][{self.info_dict['format_id']}]: STOPPED {naturalsize(self.down_size)} [{_filesize_str}][{self.n_parts_dl} of {self.n_parts}]")
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
            return (f"[HTTP][{self.info_dict['format_id']}]:(WK[{self.count:2d}]) DL[{_speed_str}] PARTS[{self.n_parts_dl:{self.format_parts()}}/{self.n_parts}] PR[{_progress_str}] ETA[{_eta_str}]\n")
        
        elif self.status == "init_manipulating":                   
            return (f"[HTTP][{self.info_dict['format_id']}]: Waiting for Ensambling \n")
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            return (f"[HTTP][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size)} [{_filesize_str}]\n")       

        