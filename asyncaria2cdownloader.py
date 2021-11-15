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
import time

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
    
    
    
    def __init__(self, port, video_dict, vid_dl):

        self.logger = logging.getLogger("async_ARIA2C_DL")
        
       
        # self.proxies = "http://atgarcia:ID4KrSc6mo6aiy8@proxy.torguard.org:6060"
        # #self.proxies = "http://192.168.1.133:5555"
        
        #self.proxies = f"http://atgarcia:ID4KrSc6mo6aiy8@{get_ip_proxy()}:6060"
                

        self.info_dict = copy.deepcopy(video_dict)
        self.video_downloader = vid_dl                
        
        self.aria2_client = aria2p.API(aria2p.Client(port=port))    
        
        #self.webpage_url = video_dict.get('webpage_url')

        self.id = self.info_dict['id']
        
        self.ytdl = self.video_downloader.info_dl['ytdl']
       
        
        self.proxies = self.ytdl.params.get('proxy', None)
        if self.proxies:
            self.proxies = f"http://{self.proxies}"
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')
        
        self.video_url = self.info_dict.get('url')
        
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
        self.error_message = ""
                   
    
    async def wait_time(self, n):
   
        _started = time.monotonic()
        while True:
            if (_t:=(time.monotonic() - _started)) >= n:
                return _t
            else:
                await asyncio.sleep(0)
        

    async def fetch_async(self):


        opts_dict = {'header': [f'{key}: {value}' for key,value in self.headers.items() if not key in ['User-Agent','Accept-Charset']],
                     'dir': str(self.download_path),
                     'out': self.filename.name,
                     'check-certificate': self.verifycert,
                     'connect-timeout': '10',
                     'timeout': '10',
                     'max-tries': '2',
                     'user-agent': std_headers['User-Agent']}
        
        opts = self.aria2_client.get_global_options()
        for key,value in opts_dict.items():
            rc = opts.set(key, value)
            if not rc:
                self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] options - couldnt set [{key}] to [{value}]")
                

        try:         
            
            self.dl_cont = await asyncio.to_thread(self.aria2_client.add_uris,[self.video_url], opts)
            
            while True:
                await asyncio.to_thread(self.dl_cont.update)
                if self.dl_cont.total_length or self.dl_cont.status in ('error', 'complete'):
                    break               
                await asyncio.sleep(0)
            
            if self.dl_cont.total_length:
                self.filesize = self.dl_cont.total_length
                self.video_downloader.info_dl['filesize'] += self.filesize
                 
            
            if self.dl_cont.status in ('active'):        
                self.status = "downloading"  
                
                while self.dl_cont.status in ('active'):
                    
                                    
                    _incsize = self.dl_cont.completed_length - self.down_size
                    self.down_size = self.dl_cont.completed_length
                    async with self.video_downloader.lock: 
                        self.video_downloader.info_dl['down_size'] += _incsize 
                                                

                    await asyncio.sleep(0)
                    await asyncio.to_thread(self.dl_cont.update)
            
            if self.dl_cont.status in ('complete'): self.status = "done"
            elif self.dl_cont.status in ('error'): 
                self.status = 'error'
                self.error_message = self.dl_cont.error_message
                
            
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {type(e)}\n{'!!'.join(lines)}")
            self.status = "error"
            
            
    async def print_hookup(self):
        
        if self.status == "done":
            return (f"[ARIA2C][{self.info_dict['format_id']}]: Completed\n")
        elif self.status == "init":
            return (f"[ARIA2C][{self.info_dict['format_id']}]: Waiting to DL [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n")            
        elif self.status == "error":
            return (f"[ARIA2C][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]")
        elif self.status == "downloading":
            _temp = copy.deepcopy(self.dl_cont)    #mientras calculamos strings progreso no puede haber update de dl_cont, así que deepcopy de la instancia      
            
            _speed_str = _temp.download_speed_string()
            _progress_str = _temp.progress_string()
            _connections = _temp.connections
            _eta_str = _temp.eta_string()
                       
            return (f"[ARIA2C][{self.info_dict['format_id']}]:(CONN[{_connections:2d}]) DL[{_speed_str}] PR[{_progress_str}] ETA[{_eta_str}]\n")
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            return (f"[ARIA2C][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f')}]\n")       
