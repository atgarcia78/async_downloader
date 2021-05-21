import asyncio
from asyncio.tasks import ALL_COMPLETED

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

from asynclogger import AsyncLogger

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
    
    #_CHUNK_SIZE = 1024
    _CHUNK_SIZE = 1048576
    
   
    
    def __init__(self, video_dict, vid_dl):

        self.logger = logging.getLogger("async_http_DL")
        self.alogger = AsyncLogger(self.logger)
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
            
        cont = 5
        while (cont > 0):
            res = self.cl.head(self.video_url, allow_redirects=True, headers=self.parts[i]['headers'])
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: {res} {res.request} {res.request.headers} {res.headers}")
            if res.status_code > 400:
                time.sleep(1)
                cont += 1
            else: break
        
        headers_size = res.headers.get('content-length')
        if headers_size:
            headers_size = int(headers_size)
        self.parts[i].update({'headersize' : headers_size})
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: \n{self.parts[i]}")
                 
            
  
    def create_parts(self):
       
        start_range = 0
        
        try:
            for i in range(1, self.n_parts+1):

                
                if i == self.n_parts:
                    self.parts.append({'part': i, 'headers' : {'range' : f'bytes={start_range}-'}, 'dl': False, 
                                    'filepath': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                    'tempfilesize': self.filesize // self.n_parts + self.filesize % self.n_parts, 'headersize' : -1, 'size' : -1})             
                else:
                    end_range = start_range + (self.filesize//self.n_parts)
                    self.parts.append({'part': i , 'headers' : {'range' : f'bytes={start_range}-{end_range}'}, 'dl' : False,
                                    'filepath': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                    'tempfilesize': self.filesize // self.n_parts, 'headersize' : -1, 'size': -1}) 
                    
                    
                    start_range = end_range + 1
                    
            with ThreadPoolExecutor(max_workers=16) as ex:
                fut = [ex.submit(self.upt_hsize, i) for i in range(self.n_parts)]
                done, pending = wait(fut, return_when=ALL_COMPLETED)
                
  
                    
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [create parts] \n{'!!'.join(lines)}")

        
    def prepare_parts(self):
        
        self.cl = httpx.Client(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
        
        if self.filesize:
            
            self.create_parts() 
            self.get_parts_to_dl()
        
        else:
            
            try:
                size = None
                
                res = self.cl.head(self.video_url, allow_redirects=True)
                #self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{res.headers}:{res.request.headers}")
                if res.status_code > 400: #repeat request without header referer
                    h_ref = self.cl.headers.pop('referer', None)
                    res = self.cl.head(self.video_url, allow_redirects=True)

                if res.status_code < 400:
                    size = res.headers.get('content-length', None)
                    if size:
                        self.filesize = int(size)                      
                
                
            except Exception as e:
                self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when trying to get filesize {e}")
                
            
            
            if size: 
                self.create_parts()
                self.get_parts_to_dl()
                self.cl.close()
            else:
                self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: {res.status_code}: Can't get size of file")
                self.cl.close()
                raise AsyncHTTPDLErrorFatal("Can't get filesize")          
           

    def get_parts_to_dl(self):
        
        self.parts_to_dl = []
        

        for part in self.parts:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] {part}")
            if not part['filepath'].exists():
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] Part_{part['part']} doesn't exits, lets DL")
                self.parts_to_dl.append(part['part'])
            else:
                partsize = part['filepath'].stat().st_size
                if (_tempfilesize:=part.get('headersize')):
                    if _tempfilesize - 5 <= partsize <= _tempfilesize + 5:
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] Part_{part['part']} exits with size {partsize} and full downloaded")
                        self.down_size += partsize                        
                        part['dl'] = True
                        part['size'] = partsize
                        self.n_parts_dl += 1
                        continue
                    else:
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [feed queue] Part_{part['part']} exits with size {partsize} and not full downloaded {part['tempfilesize']}. Re-download")
                        part['filepath'].unlink()
                        self.parts_to_dl.append(part['part'])
                else:
                    self.down_size += partsize
                    part['dl'] = True
                    self.n_parts_dl += 1
                    continue
                

            
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [get_parts_to_dl] \n{list(self.parts_to_dl)}")  
        
        if not self.parts_to_dl:
            self.status = "manipulating"      
                
    
    async def await_time(self, n):
        ex = ThreadPoolExecutor(max_workers=1)
        loop = asyncio.get_running_loop()
        
        blocking_task = [loop.run_in_executor(ex, time.sleep, n)]
        await asyncio.wait(blocking_task)
        
    async def fetch(self,i):        
         
        client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
        
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: worker_fetch [{i}] launched")
        
        while True:
            
            await asyncio.sleep(0)
            part = await self.parts_queue.get()     
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]: {part}")       
            if part == "KILL": break            
            tempfilename = self.parts[part-1]['filepath']

            n_repeat = 0
               
            await asyncio.sleep(0)
            
            while(n_repeat < 5):
                try:       
                    
                    async with client.stream("GET", self.video_url, headers=self.parts[part-1]['headers']) as res:            
                        
                        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]: Part_{part}: [fetch] resp code {str(res.status_code)}: rep {n_repeat}")
                        #await self.alogger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]: \n {res.headers} \n {self.parts[part-1]['headers']}")
                        if res.status_code >= 400:                               
                            n_repeat += 1 
                            ndl_enter = self.n_parts_dl                             
                            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]: Part_{part}: awaiting enter {ndl_enter}")
                            count = 10                           
                            while(count > 10):
                                await self.await_time(1)
                                if ndl_enter != self.n_parts_dl: break                                                                
                                count -= 1                    
                                await asyncio.sleep(0)                                   
                                
                            #await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]: Part_{part}: end awaiting with (enter {ndl_enter}), will retry")    
                            continue
                            #raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Part_{part} resp code:{str(res)}")
                        else:
                            self.parts[part-1]['headersize'] = int_or_none(res.headers.get('content-length'))
                            async with aiofiles.open(tempfilename, mode='wb') as f:
                           
                                num_bytes_downloaded = res.num_bytes_downloaded
                                async for chunk in res.aiter_bytes(chunk_size=self._CHUNK_SIZE):
                                    if chunk:
                                        await f.write(chunk)   
                                        async with self.video_downloader.lock:
                                            self.down_size += (_iter_bytes:=res.num_bytes_downloaded - num_bytes_downloaded)                                        
                                            self.video_downloader.info_dl['down_size'] += _iter_bytes 
                                        num_bytes_downloaded = res.num_bytes_downloaded
                                    await asyncio.sleep(0)
                                    
                            self.parts[part-1]['dl'] = True
                            async with self.video_downloader.lock:
                                self.n_parts_dl += 1
                            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]: Part_{part} DL: total {self.n_parts_dl}")
                            await asyncio.sleep(0)
                            break

                except (httpx.CloseError, httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                    lines = traceback.format_exception(*sys.exc_info())
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]: [fetch] Part_{part} error {type(e)}, will retry \n{'!!'.join(lines)}")
                    n_repeat += 1
                    await client.aclose()
                    await self.await_time(1)
                    client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
                    await asyncio.sleep(0)
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())
                    await self.alogger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]: [fetch] Part_{part} error {type(e)} not DL \n{'!!'.join(lines)}")
                    break
                                
             
        
        await client.aclose()
        await asyncio.sleep(0)
        
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: worker_fetch [{i}] says bye")
        
    
    async def fetch_async(self):


            
        self.parts_queue = asyncio.Queue()
        
        for part in self.parts_to_dl:
            self.parts_queue.put_nowait(part)            
        
        for _ in range(self._NUM_WORKERS):
            self.parts_queue.put_nowait("KILL")
            
        self.status = "downloading"        
    
        
        await asyncio.sleep(0)        
        
                
        
        tasks_fetch = [asyncio.create_task(self.fetch(i)) for i in range(self._NUM_WORKERS)]
        for i,t in enumerate(tasks_fetch):
            t.set_name(f"[{self.info_dict['title']}][{i}]") 
        
        done_tasks, _ = await asyncio.wait(tasks_fetch,return_when=asyncio.ALL_COMPLETED)    
        
            
    
        if done_tasks:
            
            for done in done_tasks:
                try:                        
                    done.result()  
                except Exception as e:
                    await self.alogger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: {str(e)}", exc_info=True)

        self.status = "manipulating"
        
        # try:
        #     loop = asyncio.get_running_loop()
        #     ex = ThreadPoolExecutor(max_workers=1)
        #     blocking_task = [loop.run_in_executor(ex, self.ensamble_file)]
        #     completed, pending = await asyncio.wait(blocking_task)
        #     for t in completed: t.result() 
            
        #     await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [fetch_async] ensambled OK")
        # except Exception as e:
        #     await self.alogger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [fetch_async] error when ensambling parts {str(e)}", exc_info=True)
        #     if self.filename.exists(): self.filename.unlink()
        #     self.status = "error"
        #     self.clean_when_error()
        #     await asyncio.sleep(0)
        #     raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [fetch_async] error when ensambling parts {str(e)}")
            
        # if self.filename.exists():
        #     rmtree(str(self.download_path),ignore_errors=True)
        #     self.status = "done"
        #     await asyncio.sleep(0)
        # else:
        #     self.status = "error"  
        #     self.clean_when_error()
        #     await asyncio.sleep(0)          
        #     raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling parts")
            
    
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
            
            
            
           
