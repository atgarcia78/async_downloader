import asyncio
from asyncio.exceptions import (
    CancelledError,
    InvalidStateError
)
import httpx
import aiofile
import sys
from pathlib import Path
import logging
from common_utils import (
    get_ip_proxy,
    naturalsize

)

from youtube_dl.utils import sanitize_filename

from datetime import datetime

from natsort import (
    natsorted,
    ns
)

from shutil import rmtree

from asyncio_pool import AioPool


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
        self.n_part = n_parts
        
        self.ytdl = ytdl
        self.proxies = ytdl.params.get('proxy', None)
        if self.proxies:
            self.proxies = f"http://{self.proxies}"
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')

        #timeout = httpx.Timeout(20, connect=60)
        timeout = None
        self.headers = self.info_dict.get('http_headers')
        self.client = httpx.AsyncClient(timeout=timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
        

        self.date_file = datetime.now().strftime("%Y%m%d")

        self.download_path = Path(Path.home(),"testing", self.date_file, self.info_dict['id'])

        self.download_path.mkdir(parents=True, exist_ok=True)

        self.filename = Path(Path.home(),"testing", self.date_file, self.date_file + "_" + \
            str(self.info_dict['id']) + "_" + sanitize_filename(self.info_dict['title'], restricted=True)  + "." + self.info_dict['ext'])
        
        self.filesize = self.info_dict.get('filesize', None)
        
        self.down_size = 0
        
        self.parts_header = []

        if self.filesize:
            
            self.create_byte_ranges()
        
        self.logger.debug(f"{self.filename}:{self.filesize}:{self.parts_header}")
        
        self.status = "init"
        

    def create_byte_ranges(self):
       
        start_range = 0
        for i in range(self.n_parts):
            if i == (self.n_parts - 1):
                self.parts_header.append({"range" : f"bytes={start_range}-"})                
            else:
                end_range = start_range + (self.filesize//self.n_parts)
                self.parts_header.append({"range" : f"bytes={start_range}-{end_range}"})
                start_range = end_range + 1

    def remove(self):
        rmtree(str(self.download_path),ignore_errors=True)
    
    async def fetch(self):        
        
        while not self.parts_queue.empty():

            part, range_header = await self.parts_queue.get()

            req = self.client.build_request("GET", self.video_url, headers=range_header)
            self.logger.debug(f"Part:{part} {req.headers}")
            tempfilename = Path(self.download_path, f"{self.filename.stem}_part_{part}")
            self.logger.debug(tempfilename)
        
            try:
        
                async with self.client.stream("GET", self.video_url, 
                    headers=range_header) as res:            
                    
                    if res.status_code >= 400:                               
                        raise AsyncHTTPDLError(f"Part:{str(part)} resp code:{str(res)}")
                    else:
                        async with aiofile.async_open(tempfilename, mode='wb') as f:
                            async for chunk in res.aiter_bytes(chunk_size=1024*1024):
                                await f.write(chunk)
                                self.down_size += chunk.__sizeof__()

            except (httpx.HTTPError, httpx.CloseError, httpx.RemoteProtocolError, httpx.ReadTimeout, 
                httpx.ProxyError, AttributeError, RuntimeError) as e:
                self.logger.warning(f"{e}")

    async def fetch_async(self):

        done_task = []
        pending_tasks = []
        self.tasks = []

        if not self.filesize:
            try:
                
                res = await self.client.head(self.video_url, allow_redirects=True)
                self.logger.debug(f"{self.webpage_url}:{res.headers}:{res.request.headers}")
                if res.status_code > 400: #repeat request without header referer
                    h_ref = self.client.headers.pop('referer', None)
                    res = await self.client.head(self.video_url, allow_redirects=True)

                if res.status_code < 400:
                    size = res.headers.get('content-length', None)
                    if size:
                        self.filesize = int(size)
                        self.create_byte_ranges()
                
                else: logging.warning(f"{self.webpage_url}:{res.status_code}: Can't get size of file, will download http without parts")
            except Exception as e:
                logging.warning(f"{self.webpage_url}: Can't get size of file, will download http without parts {e}")
                
        if not self.filesize:
            self.n_parts = 1
            self.parts_header = [{'Range' : 'bytes=0-'}]

        

        self.parts_queue = asyncio.Queue()

        for i, r in enumerate(self.parts_header):
            self.parts_queue.put_nowait((i, r))
        
        self.status = "downloading"
        async with AioPool(size=self.n_parts) as pool:

            futures = [pool.spawn_n(self.fetch()) for _ in range(self.n_parts)]        
            
            done_tasks, pending_tasks = await asyncio.wait(futures, return_when=asyncio.ALL_COMPLETED)        
            
            if pending_tasks:
                try:
                    await pool.cancel(pending_tasks)
                    self.logger.debug(f"{self.webpage_url}: {len(pending_tasks)} tasks pending cancelled")
                except Exception as e:
                    self.logger.debug(f"{self.webpage_url}:{e}")


                await asyncio.gather(*pending_tasks, return_exceptions=True)
        
            if done_tasks:
                for done in done_tasks:
                    try:                        
                        done.result()  
                    except Exception as e:
                        self.logger.debug(f"{self.webpage_url}:{e}")
            
            await self.client.aclose()        

            part_files = natsorted(self.download_path.iterdir(), alg=ns.PATH)
                        
            if len(part_files) != self.n_parts:
                self.status = "error"
                raise AsyncHTTPDLError(f"{self.webpage_url}:Number of part files {len(part_files)} < parts {self.n_parts}")
            
            else:        
                async with aiofile.async_open(self.filename, mode='wb') as dest:
                    try:
                        for f in part_files:
                            async with aiofile.async_open(f, mode='rb') as source:
                                await dest.write(await source.read())
                    except Exception as e:
                        self.logger.warning(f"{self.webpage_url}: error when ensambling parts {e}")
                        if self.filename.exists(): self.filename.unlink()
                        self.status = "error"
                        raise AsyncHTTPDLError(f"{self.webpage_url}: error when ensambling parts {e}")

                if self.filename.exists(): rmtree(str(self.download_path),ignore_errors=True)
                self.status = "done"
    
    def print_hookup(self):
        
        #self.down_size = naturalsize(foldersize(str(self.base_download_path)))
        return (f"{self.webpage_url}: Progress {naturalsize(self.down_size)} [{naturalsize(self.filesize)}]\n")

       
        



                
       

        



   
