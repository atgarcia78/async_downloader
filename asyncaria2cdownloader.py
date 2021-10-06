import asyncio

import httpx
import sys
from pathlib import Path
import logging
from utils import (
    naturalsize,
    std_headers
)
import traceback

import aria2p
import copy

class AsyncARIA2CDLErrorFatal(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncARIA2CDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncARIA2CDLError(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncARIA2CDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception


class AsyncARIA2CDownloader():
    
    
    
    def __init__(self, video_dict, vid_dl):

        self.logger = logging.getLogger("async_http_DL")
        
       
        # self.proxies = "http://atgarcia:ID4KrSc6mo6aiy8@proxy.torguard.org:6060"
        # #self.proxies = "http://192.168.1.133:5555"
        
        #self.proxies = f"http://atgarcia:ID4KrSc6mo6aiy8@{get_ip_proxy()}:6060"
                

        self.info_dict = copy.deepcopy(video_dict)
        self.video_downloader = vid_dl                
        
        self.aria2_client = aria2p.API(aria2p.Client())    
        
        #self.webpage_url = video_dict.get('webpage_url')

        self.id = self.info_dict['id']
        
        self.ytdl = self.video_downloader.info_dl['ytdl']
       
        
        self.proxies = self.ytdl.params.get('proxy', None)
        if self.proxies:
            self.proxies = f"http://{self.proxies}"
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')
        
        self.video_url = video_dict.get('url')
        
        try:
            client = httpx.Client(headers=std_headers, verify=self.verifycert, proxies=self.proxies, timeout=60)
            res = client.head(self.video_url)
            _video_url = str(res.url)
            if _video_url != self.video_url:
                self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] url video changed after checking\n{self.video_url}\n{_video_url}")
                self.video_url = _video_url
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {type(e)}\n{'!!'.join(lines)}")
        finally:
            client.close()
            

        
        self.headers = self.info_dict.get('http_headers')  
        
        self.download_path = self.info_dict['download_path']
        self.download_path.mkdir(parents=True, exist_ok=True) 
        if (_filename:=self.info_dict.get('_filename')):            
            
            self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
        else:
            # self.download_path = self.base_download_path
            _filename = self.info_dict.get('filename')            
            self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
            
        
        
        self.filesize = self.info_dict.get('filesize')
        
        self.down_size = 0
        self.speed = ""
        self.progress = ""
        self.connections = 0
        
        self.status = 'init'       
              
              


                   
    
    async def wait_time(self, n):
        _timer = httpx._utils.Timer()
        await _timer.async_start()
        while True:
            _t = await _timer.async_elapsed()
            if _t > n: break
            else: await asyncio.sleep(0)
        
   
            
    
    async def fetch_async(self):

       
        
        opts_dict = {'header': [f'{key} : {value}' for key,value in self.headers.items()],
                     'dir': str(self.download_path),
                     'out': self.filename.name,
                     'check-certificate': self.verifycert }
        
        
        opts = aria2p.Options(self.aria2_client, opts_dict)
        
        
        try:
         
            
            self.dl_cont = await asyncio.to_thread(self.aria2_client.add_uris,[self.video_url], opts)
            
            while True:
                await asyncio.to_thread(self.dl_cont.update)
                if self.dl_cont.total_length or (self.dl_cont.status not in ('active', 'waiting')):
                    break               
                await asyncio.sleep(0)
            
            if self.dl_cont.total_length:
                self.filesize = self.dl_cont.total_length 
            if self.dl_cont.status in ('active', 'waiting'):        
                self.status = "downloading"  
                
                while self.dl_cont.status in ('active', 'waiting'):
                    
                                    
                    _incsize = self.dl_cont.completed_length - self.down_size
                    self.down_size = self.dl_cont.completed_length
                    async with self.video_downloader.lock: 
                        self.video_downloader.info_dl['down_size'] += _incsize 
                                                
                    #await self.wait_time(0.1)
                    await asyncio.sleep(0)
                    await asyncio.to_thread(self.dl_cont.update)
            
            if self.dl_cont.status in ('complete'): self.status = "done"
            else: self.status = 'error'
                
            
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {type(e)}\n{'!!'.join(lines)}")
            self.status = "error"
            
            
    def print_hookup(self):
        
        if self.status == "done":
            return (f"[ARIA2C][{self.info_dict['format_id']}]: Completed\n")
        elif self.status == "init":
            return (f"[ARIA2C][{self.info_dict['format_id']}]: Waiting to DL [{naturalsize(self.filesize) if self.filesize else 'NA'}]\n")            
        elif self.status == "error":
            return (f"[ARIA2C][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size)} [{naturalsize(self.filesize) if self.filesize else 'NA'}]")
        elif self.status == "downloading":
            _temp = copy.deepcopy(self.dl_cont)    #mientras calculamos strings progreso no puede haber update de dl_cont, así que deepcopy de la instancia      
            
            _speed_str = _temp.download_speed_string()
            _progress_str = _temp.progress_string()
            _connections = _temp.connections
            _eta_str = _temp.eta_string()
                       
            return (f"[ARIA2C][{self.info_dict['format_id']}]: DL[{_speed_str}] Conn[{_connections}] PR[{_progress_str}] [{naturalsize(self.down_size)}/{naturalsize(self.filesize)}]({(self.down_size/self.filesize)*100:.2f}%) ETA[{_eta_str}]\n")
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            return (f"[ARIA2C][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size)} [{naturalsize(self.filesize)}]\n")       
