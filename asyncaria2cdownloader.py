import asyncio

from urllib.parse import unquote
import sys
from pathlib import Path
import logging
from utils import (
    naturalsize,    
    none_to_cero
)
import traceback

import aria2p
import copy
import time


logger = logging.getLogger("async_ARIA2C_DL")

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


        self.info_dict = copy.deepcopy(video_dict)
        self.video_downloader = vid_dl                
        
        self.aria2_client = aria2p.API(aria2p.Client(port=port))
        
        self.ytdl = self.video_downloader.info_dl['ytdl']
                
        proxies = self.ytdl.params.get('proxy', None)
        if proxies:
            self.proxies = f"http://{proxies}"
        
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')        
        self.video_url = self.info_dict.get('url')
        
        self.headers = self.info_dict.get('http_headers')  
        
        self.download_path = self.info_dict['download_path']
        self.download_path.mkdir(parents=True, exist_ok=True) 
        if (_filename:=self.info_dict.get('_filename')):            
            
            self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + "aria2."  + self.info_dict['ext'])
        else:
            # self.download_path = self.base_download_path
            _filename = self.info_dict.get('filename')            
            self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + "aria2."  + self.info_dict['ext'])

        self.filesize = none_to_cero((self.info_dict.get('filesize', 0)))
        
        self.down_size = 0               
        
        self.status = 'init'
        self.error_message = ""
        
        self.init()
        
        '''
                'connect-timeout': '10',
                'timeout': '10',
                'max-tries': '2',
        '''
 
    def init(self):
        
        opts_dict = {
            'split': self.video_downloader.info_dl['n_workers'],
            'header': [f'{key}: {value}' for key,value in self.headers.items()],
            'dir': str(self.download_path),
            'out': self.filename.name,
            'check-certificate': self.verifycert,              
            'user-agent': self.video_downloader.args.useragent}
        
        opts = self.aria2_client.get_global_options()
        for key,value in opts_dict.items():
            rc = opts.set(key, value)
            if not rc:
                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] options - couldnt set [{key}] to [{value}]")
                

        try:         
            
                    
            self.dl_cont = self.aria2_client.add_uris([unquote(self.video_url)], opts)
            while True:
                self.dl_cont.update()
                #logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [init]\n{self.dl_cont._struct}")
                if self.dl_cont.total_length or self.dl_cont.status in ('complete'):
                    break
                if self.dl_cont.status in ('error'):
                    raise AsyncARIA2CDLError(self.dl_cont.error_message)                
                
            if self.dl_cont.status in ('active'):
                self.aria2_client.pause([self.dl_cont])
                
            elif self.dl_cont.status in ('complete'):
                self.status = "done"
                
            if self.dl_cont.total_length:
                self.filesize = self.dl_cont.total_length
                           

        
        except Exception as e:                         
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {repr(e)}")
            self.status = "error"
            try:
                self.error_message = self.dl_cont.error_message
            except Exception:
                self.error_message = repr(e)
            
            raise AsyncARIA2CDLErrorFatal(self.error_message)
                
            
        
        
    async def wait_time(self, n):
   
        _started = time.monotonic()
        while True:
            if (_t:=(time.monotonic() - _started)) >= n:
                return _t
            else:
                await asyncio.sleep(0)
        

    async def fetch_async(self):        

        try: 
            
            await asyncio.to_thread(self.aria2_client.resume,[self.dl_cont])
            
            while True:
                await asyncio.to_thread(self.dl_cont.update)
                if self.dl_cont.status in ('active', 'error'):
                    break                    
                await asyncio.sleep(0)
            
            if self.dl_cont.status in ('active'):        
                self.status = "downloading"  
                
                while self.dl_cont.status in ('active'):                    
                                    
                    _incsize = self.dl_cont.completed_length - self.down_size
                    self.down_size = self.dl_cont.completed_length
                    async with self.video_downloader.lock: 
                        self.video_downloader.info_dl['down_size'] += _incsize
                    if self.video_downloader.pause_event.is_set():
                        await asyncio.to_thread(self.aria2_client.pause,[self.dl_cont])                        
                        await self.video_downloader.resume_event.wait()
                        await asyncio.to_thread(self.aria2_client.resume,[self.dl_cont])                        
                        self.video_downloader.pause_event.clear()
                        self.video_downloader.resume_event.clear()
                        while True:
                            await asyncio.to_thread(self.dl_cont.update)
                            if self.dl_cont.status in ('active', 'error', 'complete'):
                                break                    
                            await asyncio.sleep(0)
                        
                    else: await asyncio.to_thread(self.dl_cont.update)
                    await asyncio.sleep(0)
            
            if self.dl_cont.status in ('complete'): self.status = "done"
            elif self.dl_cont.status in ('error'): 
                self.status = 'error'
                self.error_message = self.dl_cont.error_message
                
            
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {repr(e)}\n{'!!'.join(lines)}")
            self.status = "error"
            self.error_message = repr(e)
            
            
    async def print_hookup(self):
        
        msg = ""
        
        if self.status == "done":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Completed\n"
        elif self.status == "init":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Waiting to DL [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n"       
        elif self.status == "error":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]"
        elif self.status == "downloading":
            _temp = copy.deepcopy(self.dl_cont)    #mientras calculamos strings progreso no puede haber update de dl_cont, así que deepcopy de la instancia      
            
            _speed_str = _temp.download_speed_string()
            _progress_str = _temp.progress_string()
            _connections = _temp.connections
            _eta_str = _temp.eta_string()
                       
            msg = f"[ARIA2C][{self.info_dict['format_id']}]:(CONN[{_connections:2d}/{self.video_downloader.info_dl['n_workers']:2d}]) DL[{_speed_str}] PR[{_progress_str}] ETA[{_eta_str}]\n"
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f')}]\n"
            
        return msg
        
               
