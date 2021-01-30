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
    get_ip_proxy

)

from datetime import datetime

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
        self.n_part = n_parts
        
        self.ytdl = ytdl
        self.proxies = ytdl.params.get('proxy', None)
        if self.proxies:
            self.proxies = f"http://{self.proxies}"
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')

        #timeout = httpx.Timeout(20, connect=60)
        timeout = None
        self.client = httpx.AsyncClient(timeout=timeout, verify=self.verifycert, proxies=self.proxies)
        self.client.headers = self.info_dict.get('http_headers')


        self.date_file = datetime.now().strftime("%Y%m%d")

        self.filename = Path(Path.home(),"testing", self.date_file, self.date_file + "_" + \
            str(self.info_dict['id']) + "_" + self.info_dict['title'] + "." + self.info_dict['ext'])
        
        self.filesize = self.info_dict.get('filesize', None)
        
        self.parts_header = []

        if self.filesize:
            
            self.create_byte_ranges()
        
        self.logger.debug(f"{self.filename}:{self.filesize}:{self.parts_header}")
        

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
        pass
    
    async def fetch(self):        
        
        while not self.parts_queue.empty():

            n_part, range_header = await self.parts_queue.get()

            req = self.client.build_request("GET", self.video_url, headers=range_header)
            self.logger.debug(f"Part:{n_part} {req.headers}")
            tempfilename = Path(self.filename.parent, f"{self.filename.stem}_part_{n_part}")
            self.logger.debug(tempfilename)
        
            try:
        
                async with self.client.stream("GET", self.video_url, 
                    headers=range_header) as res:            
                    
                    if res.status_code >= 400:                               
                        raise AsyncHTTPDLError(f"Part:{str(n_part)} resp code:{str(res)}")
                    else:
                        async with aiofile.async_open(tempfilename, mode='wb') as f:
                            async for chunk in res.aiter_bytes(chunk_size=1024*1024):
                                await f.write(chunk)

            except (httpx.HTTPError, httpx.CloseError, httpx.RemoteProtocolError, httpx.ReadTimeout, 
                httpx.ProxyError, AttributeError, RuntimeError) as e:
                self.logger.warning(f"{e}")

    async def fetch_async(self):

        done_task = []
        pending_tasks = []
        self.tasks = []

        if not self.filesize:
            try:
                res = await self.client.head(self.video_url)
                if res:
                    self.filesize = int(res.headers['content-length'])
                    self.create_byte_ranges()
            except Exception as e:
                logging.warning("Can't get size of file")     

        

        self.parts_queue = asyncio.Queue()

        for i, r in enumerate(self.parts_header):
            self.parts_queue.put_nowait((i, r))
        
        for i in range(self.n_parts):
            task = asyncio.create_task(self.fetch())
            self.tasks.append(task)
        
        done_tasks, pending_tasks = await asyncio.wait(self.tasks, return_when=asyncio.ALL_COMPLETED)

                
        if pending_tasks:
            for pending in pending_tasks:
                try:
                    pending.cancel()
                    self.logger.debug(f"task cancelled")
                except (CancelledError, InvalidStateError) as e:
                    self.logger.debug(f"{self.info_dict['title']}:{e}")

        group = await asyncio.gather(*pending_tasks, return_exceptions=True)

        if done_tasks:
            for done in done_tasks:                        
                done.result()  

        await self.client.aclose()

        
        async with aiofile.async_open(self.filename, mode='wb') as dest:
            for i in range(self.n_parts):
                tempfilename = Path(self.filename.parent, f"{self.filename.stem}_part_{i}")
                async with aiofile.async_open(tempfilename, mode='rb') as source:
                    await dest.write(await source.read())
                    tempfilename.unlink()                
