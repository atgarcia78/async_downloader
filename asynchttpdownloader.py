import asyncio

import httpx
import sys
from pathlib import Path
import logging
from common_utils import (
    naturalsize,
    int_or_none

)

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

from natsort import (
    natsorted,
    ns
)

from shutil import rmtree
import time
import aiofiles
import traceback
from aiotools import TaskGroup

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
    
    
    _CHUNK_SIZE = 1048576
    _MIN_SIZE = 1048576
    
    def __init__(self, video_dict, vid_dl):

        self.logger = logging.getLogger("async_http_DL")
        
        #self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:83.0) Gecko/20100101 Firefox/83.0"
        # self.proxies = "http://atgarcia:ID4KrSc6mo6aiy8@proxy.torguard.org:6060"
        # #self.proxies = "http://192.168.1.133:5555"
        
        #self.proxies = f"http://atgarcia:ID4KrSc6mo6aiy8@{get_ip_proxy()}:6060"
                
        self.info_dict = video_dict
        self.video_downloader = vid_dl
        self.n_parts = self.video_downloader.info_dl['n_workers']
        self._NUM_WORKERS = self.n_parts 
        self.video_url = video_dict.get('url')
        self.webpage_url = video_dict.get('webpage_url')
        

        self.videoid = self.info_dict['id']
        
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
        self.n_parts_dl = 0        
        self.parts = []
        self.status = "init"        
        self.prepare_parts()
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [init] {self.parts}")        


    def upt_hsize(self, i):
            
        
        try:
        
            cl = httpx.Client(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
            
            cont = 5
            while (cont > 0):
                res = cl.head(self.video_url, allow_redirects=True, headers=self.parts[i]['headers'])
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: {res} {res.request} {res.request.headers} {res.headers}")
                if res.status_code > 400:
                    time.sleep(1)
                    cont -= 1
                else: break
            
            if cont >0: headers_size = res.headers.get('content-length')
            else: headers_size = None
            if headers_size:
                headers_size = int(headers_size)
                self.parts[i].update({'headersize' : headers_size})
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: \n{self.parts[i]}")
        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[upt_hsize] {repr(e)} \n{'!!'.join(lines)}")            
        finally:
            cl.close()
        
        return headers_size         
            
  
    def create_parts(self):
       
        start_range = 0
        if (_chunksize:=self.filesize // self.n_parts) < self._MIN_SIZE:
            temp = self.filesize // self._MIN_SIZE            
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: size chunk [{_chunksize}] < {self._MIN_SIZE} -> change nparts [{self.n_parts} -> {temp}]")
            self.n_parts = temp
            self._NUM_WORKERS = self.n_parts
            
        
        try:
            for i in range(1, self.n_parts+1):

                
                if i == self.n_parts:
                    self.parts.append({'part': i, 'headers' : {'range' : f'bytes={start_range}-'}, 'dl': False, 
                                    'filepath': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                    'tempfilesize': self.filesize // self.n_parts + self.filesize % self.n_parts, 'headersize' : None, 'size' : -1})             
                else:
                    end_range = start_range + (self.filesize//self.n_parts)
                    self.parts.append({'part': i , 'headers' : {'range' : f'bytes={start_range}-{end_range}'}, 'dl' : False,
                                    'filepath': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                    'tempfilesize': self.filesize // self.n_parts, 'headersize' : None, 'size': -1}) 
                    
                    
                    start_range = end_range + 1
                    
            with ThreadPoolExecutor(max_workers=16) as ex:
                fut = [ex.submit(self.upt_hsize, i) for i in range(self.n_parts)]
                done, pending = wait(fut, return_when=ALL_COMPLETED)
                
            _not_hsize = [_part for _part in self.parts if not _part['headersize']]
            if len(_not_hsize) > 0: self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [create parts] not headersize in [{len(_not_hsize)}/{self.n_parts}]")
                
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
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] {part}")
            if not part['filepath'].exists():
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] Part_{part['part']} doesn't exits, lets DL")
                self.parts_to_dl.append(part['part'])
            else:
                partsize = part['filepath'].stat().st_size
                _headersize = part.get('headersize')
                if not _headersize:
                    _headersize = self.upt_hsize(i)
                 
                if partsize == 0:
                    part['filepath'].unlink()
                    self.parts_to_dl.append(part['part'])
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] Part_{part['part']} exits with size {partsize}. Re-download")
                       
                elif _headersize:
                    if _headersize - 5 <= partsize <= _headersize + 5:
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] Part_{part['part']} exits with size {partsize} and full downloaded")
                        self.down_size += partsize                        
                        part['dl'] = True
                        part['size'] = partsize
                        self.n_parts_dl += 1
                        continue
                    else:
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] Part_{part['part']} exits with size {partsize} and not full downloaded {_headersize}. Re-download")
                        part['filepath'].unlink()
                        self.parts_to_dl.append(part['part'])
                else:
                    
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] Part_{part['part']} exits with size {partsize} but without headersize. Re-download")
                    part['filepath'].unlink()
                    self.parts_to_dl.append(part['part'])
                

            
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [get_parts_to_dl] \n{list(self.parts_to_dl)}")  
        
        if not self.parts_to_dl:
            self.status = "manipulating"      
                
    
    async def await_time(self, n):
        await asyncio.to_thread(time.sleep, n)
        
        
    async def fetch(self,i):   
        
        try:
        
            client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}] launched")
            
            while True:
                
                await asyncio.sleep(0)
                part = await self.parts_queue.get()     
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][worker-{i}]:{part}")       
                if part == "KILL": break            
                tempfilename = self.parts[part-1]['filepath']

                n_repeat = 0
                
                await asyncio.sleep(0)
                
                while(n_repeat < 5):
                    
                    try:       
                        
                        async with aiofiles.open(tempfilename, mode='wb') as f:
                            
                            async with client.stream("GET", self.video_url, headers=self.parts[part-1]['headers']) as res:            
                            
                                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:Part[{part}]: [fetch] resp code {str(res.status_code)}: rep {n_repeat}")
                           
                                if res.status_code >= 400:                               
                                    ndl_enter = self.n_parts_dl                             
                                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:Part[{part}]: awaiting enter {ndl_enter}")
                                    count = 10                           
                                    while(count > 10):
                                        _t = asyncio.create_task(self.await_time(1))
                                        await asyncio.wait({_t})
                                        if ndl_enter != self.n_parts_dl: break                                                                
                                        count -= 1                    
                                        await asyncio.sleep(0)                                        
                                    n_repeat += 1
                                    continue
                                else:
                                    self.parts[part-1]['headersize'] = int_or_none(res.headers.get('content-length'))
                                
                            
                                    num_bytes_downloaded = res.num_bytes_downloaded
                                    async for chunk in res.aiter_bytes(chunk_size=self._CHUNK_SIZE):
                                        if chunk:
                                            await f.write(chunk)   
                                            async with self.video_downloader.lock:
                                                self.down_size += (_iter_bytes:=res.num_bytes_downloaded - num_bytes_downloaded)                                        
                                                self.video_downloader.info_dl['down_size'] += _iter_bytes 
                                            num_bytes_downloaded = res.num_bytes_downloaded
                                        await asyncio.sleep(0)
                                        
                            
                            async with self.video_downloader.lock:
                                self.n_parts_dl += 1
                            self.parts[part-1]['dl'] = True
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]:Part[{part}] OK DL: total {self.n_parts_dl}")
                            await asyncio.sleep(0)
                            break

                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{i}]: [fetch] Part[{part}] error {repr(e)}, will retry \n{'!!'.join(lines)}")
                        n_repeat += 1
                        if "httpx" in str(e.__class__):                            
                            await client.aclose()
                            _t = asyncio.create_task(self.await_time(1))
                            await asyncio.wait({_t})
                            client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
                            await asyncio.sleep(0)
                
                if n_repeat == 5:
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{i}]:part[{part}]:max num repeats")
                    raise AsyncHTTPDLErrorFatal(f"MaxNumRepeats part[{part}]")
            
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


        self.status = "manipulating"        
   
    
    def print_hookup(self):
        
        if self.status == "done":
            return (f"[HTTP][{self.info_dict['format_id']}]: Completed\n")
        elif self.status == "init":
            return (f"[HTTP][{self.info_dict['format_id']}]: Waiting to DL [{naturalsize(self.filesize)}][{self.n_parts_dl} of {self.n_parts}]\n")            
        elif self.status == "error":
            return (f"[HTTP][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size)} [{naturalsize(self.filesize)}][{self.n_parts_dl} of {self.n_parts}]")
        elif self.status == "downloading":           
            return (f"[HTTP][{self.info_dict['format_id']}]: Progress {naturalsize(self.down_size)} [{naturalsize(self.filesize)}][{self.n_parts_dl} of {self.n_parts}]\n")
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            return (f"[HTTP][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size)} [{naturalsize(self.filesize)}]\n")
    

       
    def clean_when_error(self):
            
        for f in self.parts:
            if f['dl'] == False:
                if f['filepath'].exists():
                    f['filepath'].unlink()
   
    def ensamble_file(self):
        
        part_files = natsorted((file for file in self.download_path.iterdir() if file.is_file() and file.name.startswith(self.filename.stem)), alg=ns.PATH)
        
        #self.logger.debug(part_files)
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [ensamble_file] start ensambling {self.filename}")
                    
        if len(part_files) != self.n_parts:
            
            raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Number of part files {len(part_files)} != parts {self.n_parts}")
        
        else:
            
            try:           
            
                with open(self.filename, 'wb') as dest:
                    for p in self.parts:
                        if p['size'] == -1:
                            if p['filepath'].exists(): p['size'] = p['filepath'].stat().st_size
                            if (p['size'] != -1) and (p['headersize'] - 5 <= p['size'] <= p['headersize'] + 5):
                                
                                with open(p['filepath'], 'rb') as source:
                                    dest.write(source.read())
                            else: raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling: {p} ")
                        else:
                            with open(p['filepath'], 'rb') as source:
                                dest.write(source.read())
                 

            except Exception as e:
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [ensamble_file] error when ensambling parts {str(e)}")
                if self.filename.exists(): self.filename.unlink()
                self.status = "error"
                self.clean_when_error()               
                raise   
            
            if self.filename.exists():
                rmtree(str(self.download_path),ignore_errors=True)
                self.status = "done" 
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [ensamble_file] file ensambled")               
            else:
                self.status = "error"  
                self.clean_when_error()                        
                raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling parts")
            
            
            
           
