import asyncio

import httpx
import sys
from pathlib import Path
import logging
from utils import (
    naturalsize,
    int_or_none

)

from concurrent.futures import CancelledError, ThreadPoolExecutor, wait, ALL_COMPLETED

from natsort import (
    natsorted,
    ns
)

from shutil import rmtree
import time
import aiofiles
import traceback
from aiotools import TaskGroup
from statistics import median
import datetime

class AsyncHTTPDLErrorFatal(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncHTTPDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncHTTPDLError(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncHTTPDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception


class AsyncHTTPDownloader():
    
    
    
    _MIN_SIZE = 1048576 #1MB
    _CHUNK_SIZE = 102400 #100KB
    #_CHUNK_SIZE = 1048576 #1MB
    
    
    
    
    def __init__(self, video_dict, vid_dl):

        self.logger = logging.getLogger("async_http_DL")
        
       
        # self.proxies = "http://atgarcia:ID4KrSc6mo6aiy8@proxy.torguard.org:6060"
        # #self.proxies = "http://192.168.1.133:5555"
        
        #self.proxies = f"http://atgarcia:ID4KrSc6mo6aiy8@{get_ip_proxy()}:6060"
                
        self._type = "http"
        self.info_dict = video_dict
        self.video_downloader = vid_dl
        self.n_parts = self.video_downloader.info_dl['n_workers']
        self._NUM_WORKERS = self.n_parts 
        self.video_url = video_dict.get('url')
        self.webpage_url = video_dict.get('webpage_url')
        

        self.id = self.info_dict['id']
        
        self.ytdl = self.video_downloader.info_dl['ytdl']
        self.proxies = self.ytdl.params.get('proxy', None)
        if self.proxies:
            self.proxies = f"http://{self.proxies}"
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')

        self.timeout = httpx.Timeout(5, connect=30)
        
        self.limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)
        self.headers = self.info_dict.get('http_headers')  
        
        self.base_download_path = self.info_dict['download_path']
        if (_filename:=self.info_dict.get('_filename')):
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True) 
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
        else:
            # self.download_path = self.base_download_path
            _filename = self.info_dict.get('filename')
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
            
        
        
        self.filesize = self.info_dict.get('filesize', None)        
        self.down_size = 0
        self.down_temp = 0
        self.timer = httpx._utils.Timer()
        self.timer.sync_start()
        self.n_parts_dl = 0        
        self.parts = []
        self.status = "init"        
        self.prepare_parts()
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[init] {self.parts}")        


    def check_server(self):
        #checks if server can handle ranges
        cont = 5
        try:
            cl = httpx.Client(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
            while (cont > 0): 
            
                try:             
                    res = cl.head(self.video_url, allow_redirects=True, headers={'range': 'bytes=0-'})
                    if res.status_code > 400:
                        time.sleep(1)
                        cont -= 1
                    else: 
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[check_server] {res} {res.request.headers} {res.headers}") 
                        break
                except Exception as e:
                    cont -= 1
            
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[check_server] {repr(e)} \n{'!!'.join(lines)}")
            cont = 0            
        finally:
            cl.close()
            
        if cont == 0: _res = None
        else:
            _res = res.headers.get('accept-ranges') or res.headers.get('content-range')
        
        
        return (_res)
        
    
    def upt_hsize(self, i, offset=None):
            
        
        try:
        
            cl = httpx.Client(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
            
           
            if offset:
                    self.parts[i]['headers'].append({'range' : f"bytes={self.parts[i]['offset'] + self.parts[i]['start']}-{self.parts[i]['end']}"})
            
            cont = 5
            while (cont > 0):
                
                try:
                
                    res = cl.head(self.video_url, allow_redirects=True, headers=self.parts[i]['headers'][-1])
                
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[upt_hsize] {res} {res.request} {res.request.headers} {res.headers}")
                    if res.status_code > 400:
                        time.sleep(1)
                        cont -= 1
                    else: break
                    
                except Exception as e:
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[upt_hsize] {repr(e)}")
                    time.sleep(1)
                    cont -= 1
                    
            
            if cont >0: headers_size = res.headers.get('content-length')
            else: headers_size = None
            if headers_size:
                headers_size = int(headers_size)
                if not offset: self.parts[i].update({'headersize' : headers_size})
                else: 
                    self.parts[i].update({'hsizeoffset' : headers_size})
                    
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: \n{self.parts[i]}")
        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[upt_hsize] {repr(e)} \n{'!!'.join(lines)}")
            headers_size = None            
        finally:
            cl.close()
        
        return headers_size         
            
  
    def create_parts(self):
       
        start_range = 0
        if not self.check_server(): #server can not handle ranges
            self._NUM_WORKERS = self.n_parts = 1
            self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: server cant handle ranges")
            
        elif (_chunksize:=self.filesize // self.n_parts) < self._MIN_SIZE: #size of parts cant be less than _MIN_SIZE
            temp = self.filesize // self._MIN_SIZE            
            self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: size chunk [{_chunksize}] < {self._MIN_SIZE} -> change nparts [{self.n_parts} -> {temp}]")
            self.n_parts = temp
            self._NUM_WORKERS = self.n_parts            
        
        try:
            for i in range(1, self.n_parts+1):
                
                if i == self.n_parts:
                    self.parts.append({'part': i, 'offset': 0, 'start': start_range, 'end': '', 'headers' : [{'range' : f'bytes={start_range}-'}], 'dl': False, 
                                    'filepath': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                    'tempfilesize': (_tfs:=(self.filesize // self.n_parts + self.filesize % self.n_parts)), 'headersize' : None, 'hsizeoffset': None, 'size' : -1,
                                    'nchunks': _tfs // self._CHUNK_SIZE, 'nchunks_dl': 0, 'time2dlchunks': [], 'statistics': []})             
                else:
                    end_range = start_range + (self.filesize//self.n_parts - 1)
                    self.parts.append({'part': i , 'offset': 0, 'start': start_range, 'end': end_range, 'headers' : [{'range' : f'bytes={start_range}-{end_range}'}], 'dl' : False,
                                    'filepath': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                    'tempfilesize': (_tfs:=(self.filesize // self.n_parts)), 'headersize' : None, 'hsizeoffset': None, 'size': -1, 
                                    'nchunks': _tfs // self._CHUNK_SIZE, 'nchunks_dl': 0, 'time2dlchunks': [], 'statistics': []})
                    
                    
                    start_range = end_range + 1
                    
            with ThreadPoolExecutor(max_workers=16) as ex:
                fut = [ex.submit(self.upt_hsize, i) for i in range(self.n_parts)]
                done, pending = wait(fut, return_when=ALL_COMPLETED)
                
            _not_hsize = [_part for _part in self.parts if not _part['headersize']]
            if len(_not_hsize) > 0: self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[create parts] not headersize in [{len(_not_hsize)}/{self.n_parts}]")
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[create parts] {repr(e)} \n{'!!'.join(lines)}")

        
    def prepare_parts(self): 
        
        if self.filesize:
            
            self.create_parts() 
            self.get_parts_to_dl()
        
        else:
            
            try:
                size = None
                cl = httpx.Client(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
                res = cl.head(self.video_url, allow_redirects=True)
                #self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{res.headers}:{res.request.headers}")
                if res.status_code > 400: #repeat request without header referer
                    h_ref = cl.headers.pop('referer', None)
                    res = cl.head(self.video_url, allow_redirects=True)

                if res.status_code < 400:
                    size = res.headers.get('content-length', None)
                    if size:
                        self.filesize = int(size)                      
                
                
            except Exception as e:
                self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when trying to get filesize {e}")
            finally:
                cl.close()
            
            if size: 
                self.create_parts()
                self.get_parts_to_dl()
                
            else:
                self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: {res.status_code}: Can't get size of file")                
                raise AsyncHTTPDLErrorFatal("Can't get filesize")          
           

    def get_parts_to_dl(self):
        
        self.parts_to_dl = []

        for i, part in enumerate(self.parts):
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue]\n{part}")
            if not part['filepath'].exists():
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] doesn't exits, lets DL")
                self.parts_to_dl.append(part['part'])
            else:
                partsize = part['filepath'].stat().st_size
                _headersize = part.get('headersize')
                if not _headersize:
                    _headersize = self.upt_hsize(i)
                 
                if partsize == 0:
                    part['filepath'].unlink()
                    self.parts_to_dl.append(part['part'])
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize}. Re-download from scratch")
                       
                elif _headersize:
                    if (partsize > _headersize + 5):
                        part['filepath'].unlink()
                        self.parts_to_dl.append(part['part'])
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize}. Re-download from scratch")
                        
                    elif _headersize - 5 <= partsize <= _headersize + 5: #with a error margen of +-100bytes,file is fully downloaded
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize} and full downloaded")
                        self.down_size += partsize                        
                        part['dl'] = True
                        part['size'] = partsize
                        self.n_parts_dl += 1
                        continue
                    else: #there's something previously downloaded
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize} and not full downloaded {_headersize}. Re-define header range to start from the dl size")
                        _old_part = part 
                        part['offset'] = partsize + 1                        
                        #part['headers'] = {'range' : f"bytes={part['offset']}-{part['end']}"}
                        _hsizeoffset = self.upt_hsize(i, offset=True)
                        if _hsizeoffset:
                            part['hsizeoffset'] = _hsizeoffset
                            self.parts_to_dl.append(part['part'])
                            self.down_size += partsize 
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue]\n{part}\n{self.parts[i]}")
                        else:
                            part = _old_part
                            part['filepath'].unlink()
                            self.parts_to_dl.append(part['part'])
                            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] couldnt get new headersize. Re-download from scratch")
                            
                        
                else:
                    
                    self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[feed queue] part[{part['part']}] exits with size {partsize} but without headersize. Re-download")
                    part['filepath'].unlink()
                    self.parts_to_dl.append(part['part'])
                

            
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[get_parts_to_dl] \n{list(self.parts_to_dl)}")  
        
        if not self.parts_to_dl:
            self.status = "manipulating"      
                
    
    async def wait_time(self, n):
        _timer = httpx._utils.Timer()
        await _timer.async_start()
        while True:
            _t = await _timer.async_elapsed()
            if _t > n: break
            else: await asyncio.sleep(0)
        
        
    async def fetch(self,i):
        
                
        try:
            
            _timer = httpx._utils.Timer()
        
            client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}] launched")
            
            while True:
                
                
                part = await self.parts_queue.get()     
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][worker-{i}]:part[{part}]")       
                if part == "KILL": break            
                tempfilename = self.parts[part-1]['filepath']

                n_repeat = 0
                await asyncio.sleep(0)
                
                               
                while True: #bucle del worker
                        
                    try:
                    
                        async with aiofiles.open(tempfilename, mode='ab') as f:
                        
                                                       
                            async with client.stream("GET", self.video_url, headers= self.parts[part-1]['headers'][-1]) as res:
                            
                                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:part[{part}]: [fetch] resp code {str(res.status_code)}: rep {n_repeat}\n{res.request.headers}")
                        
                                if res.status_code >= 400:
                                    
                                    n_repeat += 1
                                    if n_repeat == 10:
                                        raise AsyncHTTPDLError(f"error[{res.status_code}] part[{part}]")
                                    else:
                                        await self.wait_time(5) 
                                        continue
                                            
                                else:
                                    if (len(self.parts[part-1]['headers']) == 1) and (not self.parts[part-1]['headersize']):
                                        self.parts[part-1]['headersize'] = int_or_none(res.headers.get('content-length'))
                                    
                                    num_bytes_downloaded = res.num_bytes_downloaded
                                    
                                    self.parts[part-1]['nchunks_dl'] = 0
                                    self.parts[part-1]['time2dlchunks'] = []
                                    self.parts[part-1]['statistics'] = []
                                    
                                    await _timer.async_start()
                                    
                                    async for chunk in res.aiter_bytes(chunk_size=self._CHUNK_SIZE): 
                                    
                                        _timechunk = await _timer.async_elapsed() 
                                        self.parts[part-1]['time2dlchunks'].append(_timechunk)
                                        await f.write(chunk)                                        
                                           
                                        async with self.video_downloader.lock:
                                            self.down_size += (_iter_bytes:=(res.num_bytes_downloaded - num_bytes_downloaded))                                        
                                            self.video_downloader.info_dl['down_size'] += _iter_bytes 
                                        num_bytes_downloaded = res.num_bytes_downloaded
                                        self.parts[part-1]['nchunks_dl'] += 1
                                        _median = median(self.parts[part-1]['time2dlchunks'])
                                        self.parts[part-1]['statistics'].append(_median)
                                                
                                        if self.parts[part-1]['nchunks_dl'] > 10:
                                            if  (((_time1:=self.parts[part-1]['time2dlchunks'][-1]) > (_max1:=15*self.parts[part-1]['statistics'][-1])) and
                                                    ((_time2:=self.parts[part-1]['time2dlchunks'][-2]) > (_max2:=15*self.parts[part-1]['statistics'][-2])) and   
                                                        ((_time3:=self.parts[part-1]['time2dlchunks'][-3]) > (_max3:=15*self.parts[part-1]['statistics'][-3]))): 
                                            
                                                raise AsyncHTTPDLError(f"timechunk [{_time1}, {_time2}, {_time3}] > [{_max1}, {_max2}, {_max3}] for 3 consecutives chunks")
                                        
                                        await asyncio.sleep(0)
                                        await _timer.async_start()
                                                                        
                        _size = (await asyncio.to_thread(tempfilename.stat)).st_size
                        self.parts[part-1]['size'] = _size
                        if (self.parts[part-1]['headersize'] - 5 <= _size <= self.parts[part-1]['headersize'] + 5):
                            self.parts[part-1]['dl'] = True
                            
                            async with self.video_downloader.lock:
                                self.n_parts_dl += 1
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:part[{part}] OK DL: total {self.n_parts_dl}\n{self.parts[part-1]}")
                            break
                        
                        else:
                            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:[fetch-stream] part[{part}] end of stream not completed\n{self.parts[part-1]}")
                           
                            raise AsyncHTTPDLError(f"Part not completed in streaming: part[{part}]") 
                        
                    except (asyncio.exceptions.CancelledError, asyncio.CancelledError, CancelledError) as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch-res] part[{part}] error {repr(e)}, will retry \n{'!!'.join(lines)}")
                        raise
                            
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        n_repeat += 1
                        
                        if "httpx" in str(e.__class__) or "AsyncHTTPDLError" in str(e.__class__): 
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch]-res] part[{part}] error {repr(e)}, will retry \n{'!!'.join(lines)}")                           
                            
                            if n_repeat < 10:
                                _size = (await asyncio.to_thread(tempfilename.stat)).st_size
                                ##self.parts[part-1]['offset'] = _size + 1 
                                self.parts[part-1]['offset'] = _size                               
                                _hsizeoffset = self.upt_hsize(part - 1, offset=True) 
                                #await res.aclose()
                                await client.aclose()
                                await self.wait_time(1)
                                client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
                                await asyncio.sleep(0)
                            else:
                                raise AsyncHTTPDLErrorFatal(f"MaxNumRepeats part[{part}]")
                        else:
                            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch-res] part[{part}] error unexpected {repr(e)}, will retry \n{'!!'.join(lines)}") 
                            
                            
                    
                    # finally:
                    #     try:
                    #         await res.aclose()
                    #     except Exception as e:
                    #         self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch]-res] part[{part}] error {repr(e)}")
                             
                    
        # except Exception as e:
        #     lines = traceback.format_exception(*sys.exc_info())            
        #     if not "AsyncHTTPErrorFatal" in str(e.__class__):
        #         self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}] error {repr(e)} \n{'!!'.join(lines)}")
        #     else: raise
                
        finally:
            await client.aclose()
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}] says bye")
            
    
    async def fetch_async(self):

            
        self.parts_queue = asyncio.Queue()
        
        for part in self.parts_to_dl:
            self.parts_queue.put_nowait(part)            
        
        for _ in range(self._NUM_WORKERS):
            self.parts_queue.put_nowait("KILL")
            
        self.status = "downloading"        
    
        
        await asyncio.sleep(0)        
        
        
        try:
                
            async with TaskGroup() as tg:
                
                self.tasks_fetch = [tg.create_task(self.fetch(i)) for i in range(self._NUM_WORKERS)]
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {type(e)}\n{'!!'.join(lines)}")

        if (_parts_not_dl:= await asyncio.to_thread(self.partsnotdl)):
            self.status = "error"
        
        else:
            self.status = "manipulating"  

    def partsnotdl(self):
        res = []
        for part in self.parts:
            if part['dl'] == False:
                res.append(part['part'])
        return res
       
    def sync_clean_when_error(self):
            
        for f in self.parts:
            if f['dl'] == False:
                if f['filepath'].exists():
                    f['filepath'].unlink()
   
    def ensamble_file(self):
        
   
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] start ensambling {self.filename}")
        
        try:           
        
            with open(self.filename, 'wb') as dest:
                for p in self.parts:
                    if p['filepath'].exists():
                        p['size'] = p['filepath'].stat().st_size
                        if (p['headersize'] - 5 <= p['size'] <= p['headersize'] + 5):
                            
                            with open(p['filepath'], 'rb') as source:
                                dest.write(source.read())
                        else: raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] error when ensambling: {p} ")
                    else:
                        raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] error when ensambling: {p} ")
                

        except Exception as e:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] error when ensambling parts {str(e)}")
            if self.filename.exists(): self.filename.unlink()
            self.status = "error"
            self.sync_clean_when_error()               
            raise   
        
        if self.filename.exists():
            rmtree(str(self.download_path),ignore_errors=True)
            self.status = "done" 
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] file ensambled")               
        else:
            self.status = "error"  
            self.sync_clean_when_error()                        
            raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[ensamble_file] error when ensambling parts")
            
            
            
    def print_hookup(self):
        
        _time = self.timer.sync_elapsed()
        _bytes = self.down_size - self.down_temp
        _speed = _bytes / _time
        if _speed != 0: 
            _eta = datetime.timedelta(seconds=((self.filesize - self.down_size)/_speed))
            _eta_str = ":".join([_item.split(".")[0] for _item in f"{_eta}".split(":")])
        else: _eta_str = "--"
        
        
        if self.status == "done":
            return (f"[HTTP][{self.info_dict['format_id']}]: Completed\n")
        elif self.status == "init":
            return (f"[HTTP][{self.info_dict['format_id']}]: Waiting to DL [{naturalsize(self.filesize)}][{self.n_parts_dl} of {self.n_parts}]\n")            
        elif self.status == "error":
            return (f"[HTTP][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size)} [{naturalsize(self.filesize)}][{self.n_parts_dl} of {self.n_parts}]")
        elif self.status == "downloading":           
            return (f"[HTTP][{self.info_dict['format_id']}]: DL[{naturalsize(_speed)}s] PR[{naturalsize(self.down_size)}/{naturalsize(self.filesize)}]({(self.down_size/self.filesize)*100:.2f}%) ETA[{_eta_str}] PARTS[{self.n_parts_dl}/{self.n_parts}]\n")
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            return (f"[HTTP][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size)} [{naturalsize(self.filesize)}]\n")       

        self.timer.sync_start()
        self.down_temp = self.down_size