import asyncio

from os import CLD_CONTINUED
import httpx
import sys
from pathlib import Path
import logging
from common_utils import (
    naturalsize

)

import concurrent.futures

from youtube_dl.utils import sanitize_filename

from datetime import datetime

from natsort import (
    natsorted,
    ns
)

from shutil import rmtree

from asyncio_pool import AioPool


import aiofiles

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
    
    def __init__(self, video_dict, ytdl, n_parts):

        self.logger = logging.getLogger("async_http_DL")
        #self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:83.0) Gecko/20100101 Firefox/83.0"
        # self.proxies = "http://atgarcia:ID4KrSc6mo6aiy8@proxy.torguard.org:6060"
        # #self.proxies = "http://192.168.1.133:5555"
        
        #self.proxies = f"http://atgarcia:ID4KrSc6mo6aiy8@{get_ip_proxy()}:6060"
                
        self.info_dict = video_dict
        self.n_parts = n_parts 
        self.video_url = video_dict.get('url')
        self.webpage_url = video_dict.get('webpage_url')
        
        _video_id = str(self.info_dict['id'])
        self.info_dict['id'] = _video_id[:15] if len(_video_id) > 15 else _video_id 
        self.videoid = self.info_dict['id']
        
        self.ytdl = ytdl
        self.proxies = ytdl.params.get('proxy', None)
        if self.proxies:
            self.proxies = f"http://{self.proxies}"
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')

        timeout = httpx.Timeout(20, connect=30)
        #timeout = None
        limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)
        self.headers = self.info_dict.get('http_headers')
        self.client = httpx.AsyncClient(limits=limits, timeout=timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
        self.cl = httpx.Client(limits=limits, timeout=timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
        self.date_file = datetime.now().strftime("%Y%m%d")
        self.download_path = Path(Path.home(),"testing", self.date_file, self.info_dict['id'])
        self.download_path.mkdir(parents=True, exist_ok=True)    
        self.filename = Path(Path.home(),"testing", self.date_file,
            str(self.info_dict['id']) + "_" + sanitize_filename(self.info_dict['title'], restricted=True)  + "." + self.info_dict['ext']) 
        self.filesize = self.info_dict.get('filesize', None)        
        self.down_size = 0
        self.n_parts_dl = 0        
        self.parts = []
        self.status = "init"
        self.prepare_parts()
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [init] {self.parts}")        

    def create_parts(self):
       
        start_range = 0
        for i in range(1, self.n_parts+1):
            if i == self.n_parts:
                self.parts.append({'part': i, 'headers' : {'range' : f'bytes={start_range}-'}, 'dl': False, 
                                   'tempfilename': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                   'tempfilesize': self.filesize // self.n_parts + self.filesize % self.n_parts})                
            else:
                end_range = start_range + (self.filesize//self.n_parts)
                self.parts.append({'part': i , 'headers' : {'range' : f'bytes={start_range}-{end_range}'}, 'dl' : False,
                                   'tempfilename': Path(self.download_path, f"{self.filename.stem}_part_{i}_of_{self.n_parts}"),
                                   'tempfilesize': self.filesize // self.n_parts})
                
                start_range = end_range + 1

    def remove(self):
        rmtree(str(self.download_path),ignore_errors=True)
    
        
    def prepare_parts(self):
        
        if not self.filesize:
            
            try:
                res = self.cl.head(self.video_url, allow_redirects=True)
                #self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{res.headers}:{res.request.headers}")
                if res.status_code > 400: #repeat request without header referer
                    h_ref = self.cl.headers.pop('referer', None)
                    res = self.cl.head(self.video_url, allow_redirects=True)

                if res.status_code < 400:
                    size = res.headers.get('content-length', None)
                    if size:
                        self.filesize = int(size)
                        self.create_parts()
                        return
                
                else: logging.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {res.status_code}: Can't get size of file, will download http without parts")
            except Exception as e:
                logging.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]: Can't get size of file, will download http without parts {e}")
                
        if not self.filesize:
            self.n_parts = 1
            self.parts = [{'part': 1, 'headers' : {'range' : 'bytes=0-'}, 'dl' : False,
                                   'tempfilename': Path(self.download_path, f"{self.filename.stem}_part_1_of_1"),
                                   'tempfilesize': None}]
           

        else: self.create_parts()    
    

    def feed_queue(self):
        

        for part in self.parts:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [feed queue] {part}")
            if not part['tempfilename'].exists():
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [feed queue] Part_{part['part']} doesn't exits, lets DL")
                self.parts_queue.put_nowait(part['part'])
            else:
                partsize = part['tempfilename'].stat().st_size
                if part.get('tempfilesize'):
                    if partsize in range(part['tempfilesize'] - 100, part['tempfilesize'] + 100):
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [feed queue] Part_{part['part']} exits with size {partsize} and full downloaded")
                        self.down_size += partsize
                        part['dl'] = True
                        self.n_parts_dl += 1
                        continue
                    else:
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [feed queue] Part_{part['part']} exits with size {partsize} and not full downloaded {part['tempfilesize']}. Re-downloaded")
                        part['tempfilename'].unlink()
                        self.parts_queue.put_nowait(part['part'])
                else:
                    self.down_size += partsize
                    part['dl'] = True
                    self.n_parts_dl += 1
                    continue
                
        for _ in range(self.n_parts):
            self.parts_queue.put_nowait("KILL")
            
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: \n{list(self.parts_queue._queue)}")        
                
    
    
    async def fetch(self,i):        
         
        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: worker_fetch [{i}] launched")
        
        while True:
            
            await asyncio.sleep(1)
            part = await self.parts_queue.get()     
            self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: {part}")       
            if part == "KILL": break            
            tempfilename = self.parts[part-1]['tempfilename']
            #self.logger.debug(tempfilename)
            # req = self.client.build_request("GET", self.video_url, headers=range_header)
            # self.logger.debug(f"Part:{part} {req.headers}")
            n_repeat = 0
               

            while(n_repeat < 5):
                try:       
                    
                    async with self.client.stream("GET", self.video_url, 
                        headers=self.parts[part-1]['headers']) as res:            
                        
                        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: Part_{part}: [fetch] resp code {str(res.status_code)}: rep {n_repeat}")
                        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: \n {res.headers} \n {self.parts[part-1]['headers']}")
                        if res.status_code >= 400:                               
                            #self.parts_queue.put_nowait(part)                            
                            n_repeat += 1
                            if n_repeat == 5: 
                                self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: Part_{part} not DL max repeats reached")
                                break
                            ndl_enter = self.n_parts_dl
                            ndl_while = ndl_enter 
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: Part_{part}: awaiting enter {ndl_enter}")
                            count = 30                           
                            while(ndl_while == ndl_enter):
                                await asyncio.sleep(1)
                                ndl_while = self.n_parts_dl
                                count -= 1
                                if count == 0:
                                    break                                    
                                
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: Part_{part}: end awaiting with {ndl_while} (enter {ndl_enter}), will retry")    
                            continue
                            #raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Part_{part} resp code:{str(res)}")
                        else:
                            async with aiofiles.open(tempfilename, mode='wb') as f:
                            #with open(tempfilename, mode='wb') as f:
                                num_bytes_downloaded = res.num_bytes_downloaded
                                async for chunk in res.aiter_bytes(chunk_size=self._CHUNK_SIZE):
                                    await f.write(chunk)
                                    #f.write(chunk)
                                    self.down_size += res.num_bytes_downloaded - num_bytes_downloaded
                                    num_bytes_downloaded = res.num_bytes_downloaded
                            self.parts[part-1]['dl'] = True
                            self.n_parts_dl += 1
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: Part_{part} DL: total {self.n_parts_dl}")
                            break

                except (httpx.HTTPError, httpx.CloseError, httpx.RemoteProtocolError, httpx.ReadTimeout, 
                    httpx.ProxyError, AttributeError, RuntimeError) as e:
                    self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: [fetch] Part_{part} error {e}, will retry")
                    n_repeat += 1
                except Exception as e:
                    self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{i}]: [fetch] Part_{part} error {e} not DL")
                    break
                    
             
             
        
        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: worker_fetch [{i}] says bye")
    
    async def fetch_async(self):

        self.parts_queue = asyncio.Queue()
        
        self.feed_queue()        
            
        self.status = "downloading"
        
        workers = self.n_parts
        
        await asyncio.sleep(1)
                
        
        tasks_fetch = [asyncio.create_task(self.fetch(i)) for i in range(self.n_parts)]
        for i,t in enumerate(tasks_fetch):
            t.set_name(f"[{self.info_dict['title']}][{i}]") 
        
        done_tasks, pending_tasks = await asyncio.wait(tasks_fetch)
        
        # async with AioPool(size=workers) as pool:

        #     futures = [pool.spawn_n(self.fetch()) for _ in range(self.n_parts)]        
            
        #     done_tasks, pending_tasks = await asyncio.wait(futures, return_when=asyncio.ALL_COMPLETED)        
            
        if pending_tasks:            
            try:
                await asyncio.gather(*pending_tasks, return_exceptions=True)                
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {len(pending_tasks)} tasks pending waited")
            except Exception as e:
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {str(e)}", exc_info=True)
            
    
        if done_tasks:
            for done in done_tasks:
                try:                        
                    done.result()  
                except Exception as e:
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {str(e)}", exc_info=True)
            
        await self.client.aclose()        

        try:
            loop = asyncio.get_running_loop()
            ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            blocking_task = [loop.run_in_executor(ex, self.ensamble_file())]
            completed, pending = await asyncio.wait(blocking_task)
            if completed:
                results = [t.exception() for t in completed]
            # self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: out of the coroutine in other thread")
            #result.result()
            #self.ensamble_file()
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [fetch_async] ensambled OK")
        except TypeError as e:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [fetch_async] type error", exc_info=True) 
        except Exception as e:
            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [fetch_async] error when ensambling parts {str(e)}", exc_info=True)
            if self.filename.exists(): self.filename.unlink()
            self.status = "error"
            raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [fetch_async] error when ensambling parts {str(e)}")
            
        if self.filename.exists() and self.filename.stat().st_size in range(self.filesize - 100, self.filesize + 100):
            rmtree(str(self.download_path),ignore_errors=True)
            self.status = "done"
        else:
            self.status = "error"
            raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when ensambling parts", exc_info=True)
    
    def print_hookup(self):
        
        if self.status == "done":
            return (f"[HTTP][[{self.info_dict['id']}][{self.info_dict['title']}]: Completed [{naturalsize(self.filename.stat().st_size)}][{self.n_parts_dl} of {self.n_parts}]\n")
        elif self.status == "init":
            return (f"[HTTP][{self.info_dict['id']}][{self.info_dict['title']}]: Waiting to enter in the pool [{naturalsize(self.filesize)}][{self.n_parts_dl} of {self.n_parts}]\n")            
        elif self.status == "error":
            return (f"[HTTP][{self.info_dict['id']}][{self.info_dict['title']}]: ERROR {naturalsize(self.down_size)} [{naturalsize(self.filesize)}][{self.n_parts_dl} of {self.n_parts}]\n")
        else:            
            return (f"[HTTP][{self.info_dict['id']}][{self.info_dict['title']}]: Progress {naturalsize(self.down_size)} [{naturalsize(self.filesize)}][{self.n_parts_dl} of {self.n_parts}]\n")

       
    def ensamble_file(self):
        
        part_files = natsorted((file for file in self.download_path.iterdir() if file.is_file() and file.name.startswith(self.filename.stem)), alg=ns.PATH)
        
        #self.logger.debug(part_files)
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [ensamble_file] start ensambling {self.filename}")
                    
        if len(part_files) != self.n_parts:
            self.status = "error"
            raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Number of part files {len(part_files)} != parts {self.n_parts}")
        
        else:
            
            for i, f in enumerate(part_files):
                # if i != (self.n_parts-1): tempfilesize = self.filesize // self.n_parts
                # else: tempfilesize = self.filesize // self.n_parts + self.filesize % self.n_parts
                tempfilesize = self.parts[i]['tempfilesize']            
                partsize = f.stat().st_size
                #self.logger.debug(f"part_{i}:{partsize}:{tempfilesize}")
                if partsize not in range(tempfilesize - 100, tempfilesize + 100):
                    #self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: Part_{i+1} exits with size {partsize} and not full downloaded {tempfilesize}")
                    self.status = "error"
                    raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}]: Part_{i+1} file size {partsize} doesnt match expected {tempfilesize}")
                else:
                    #self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: Part_{i+1} exits {partsize} full downloaded {tempfilesize}")
                    pass    
            
            with open(self.filename, 'wb') as dest:
                try:
                    for f in part_files:
                        #self.logger.debug(f)
                        with open(f, 'rb') as source:
                            #self.logger.debug(f"{f} open")
                            dest.write(source.read())
                            #self.logger.debug(f"{f} read and write")
                except Exception as e:
                    self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [ensamble_file] error when ensambling parts {e}")
                    if self.filename.exists(): self.filename.unlink()
                    self.status = "error"
                    raise AsyncHTTPDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when ensambling parts {e}")

            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [ensamble_file] file ensambled")
