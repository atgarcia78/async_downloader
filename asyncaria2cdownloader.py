import asyncio
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote
import copy

import aria2p

from utils import (
    async_ex_in_executor, 
    naturalsize, 
    none_to_cero, 
    try_get, 
    CONFIG_EXTRACTORS, 
    traverse_obj, 
    limiter_non, 
    AsyncYTDL, 
    get_format_id,
    MyLogger,
    get_domain
)

from cs.threads import PriorityLock

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
    
    _CONFIG = CONFIG_EXTRACTORS.copy()    
    #_LOCK = Lock()    
    _EX_ARIA2DL = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")
        
    
    def __init__(self, port, video_dict, vid_dl):

        self.info_dict = video_dict.copy()
        self.video_downloader = vid_dl                
        
        self.aria2_client = aria2p.API(aria2p.Client(port=port))
        
        self.ytdl = self.video_downloader.info_dl['ytdl']
        
        
        self.proxies = [f'http://127.0.0.1:123{i + 4}' for i in range(6)]
        
        self._ytdl_opts = self.ytdl.params.copy()        
        self._ytdl_opts['quiet'] = True
        self._ytdl_opts['verbose'] = False
        self._ytdl_opts['verboseplus'] = False
        self._ytdl_opts['logger'] = MyLogger(logging.getLogger("proxy_yt_dlp"), quiet=True, verbose=False, superverbose=False)

        
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')        
        
        self.video_url = self.info_dict.get('url')
        self.uris = [unquote(self.video_url)]        
        self._host = get_domain(self.uris[0])        
        
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
        
                
        self.status = 'init'
        self.error_message = ""
        
        self.nworkers = self.video_downloader.info_dl['n_workers']
        
        # with AsyncARIA2CDownloader._LOCK:
        #     if not AsyncARIA2CDownloader._EX_ARIA2DL:
        #         AsyncARIA2CDownloader._EX_ARIA2DL = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")
        
        self.reset_event = None
        
        self.prep_init()
        
        

    def prep_init(self):        
        
        
        def getter(x):
        
            value, key_text = try_get([(v,kt) for k,v in self._CONFIG.items() if any(x==(kt:=_) for _ in k)], lambda y: y[0]) or ("","") 
            if value:
                return(value['ratelimit'].ratelimit(key_text, delay=True), value['maxsplits'])
        
        _extractor = self.info_dict.get('extractor')
        self.auto_pasres = False
        _sem = False
        if _extractor and _extractor.lower() != 'generic':
            self._decor, _nsplits = getter(_extractor) or (limiter_non.ratelimit("transp", delay=True), self.nworkers)
            if _extractor in ['doodstream', 'vidoza']:
                self.auto_pasres = True #ojo review
            if _nsplits < 16: 
                _sem = True
        else: 
            self._decor, _nsplits = limiter_non.ratelimit("transp", delay=True), self.nworkers
            

        self.nworkers = min(_nsplits, self.nworkers)

        opts_dict = {
            'split': self.nworkers,
            'max-connection-per-server': self.nworkers,
            'header': [f"{key}: {value}" for key,value in self.headers.items()],
            'dir': str(self.download_path),
            'out': self.filename.name,
        }
        

        self.opts = self.aria2_client.get_global_options()
        for key,value in opts_dict.items():
            rc = self.opts.set(key, value)
            if not rc:
                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] options - couldnt set [{key}] to [{value}]")
                

        if _sem:
            
            with self.ytdl.params['lock']:                
                if not (_sem:=traverse_obj(self.ytdl.params.get('sem'), self._host)):
                    _sem = PriorityLock()
                    self.ytdl.params['sem'].update({self._host: _sem})
                    
            self.sem = _sem
            
            
        else: 
            self.sem = None
                    

    async def init(self):

        self.down_size = 0
        
        try:
            if self.sem:
                
                while not self.reset_event.is_set():
                    async with self.video_downloader.master_hosts_alock:
                        if not self.video_downloader.hosts_dl.get(self._host):
                            self.video_downloader.hosts_dl.update({self._host: {'count': 1, 'queue': asyncio.Queue()}})
                            for el in self.proxies:
                                self.video_downloader.hosts_dl[self._host]['queue'].put_nowait(el)
                            self._proxy = "get_one"
                            break
                            
                        else:
                            if self.video_downloader.hosts_dl[self._host]['count'] < len(self.proxies):
                                self.video_downloader.hosts_dl[self._host]['count'] += 1
                                self._proxy = "get_one"
                                break                        
                            else:
                                self._proxy = "wait_for_one"
                    
                    if self._proxy == "wait_for_one":
                        await asyncio.sleep(5)
                        continue
                
                if self.reset_event.is_set():
                    return
                if self._proxy == "get_one":
                    self._proxy = await self.video_downloader.hosts_dl[self._host]['queue'].get()
                    self.opts.set("all-proxy", self._proxy) 
                    _ytdl_opts = self._ytdl_opts.copy()
                    _ytdl_opts['proxy'] = self._proxy
                    async with AsyncYTDL(_ytdl_opts) as proxy_ytdl:
                        proxy_info = get_format_id(proxy_ytdl.sanitize_info(await proxy_ytdl.async_extract_info(AsyncARIA2CDownloader._EX_ARIA2DL, self.info_dict.get('webpage_url'), download=False)), self.info_dict['format_id'])
                    
                    self.video_url = proxy_info.get('url')
                    self.uris = [unquote(self.video_url)]
                    self.headers = proxy_info.get('headers').copy()
                    self.opts.set("header", [f"{key}: {value}" for key,value in self.headers.items()])
                         
        
            async with self._decor: 
                self.dl_cont = await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.add_uris, self.uris, self.opts)

            _tstart = time.monotonic()
            
            cont = 0
            
            while not self.reset_event.is_set():
                async with self.video_downloader.alock: 
                    await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.dl_cont.update)
                if self.dl_cont.total_length or self.dl_cont.status in ('complete'):
                    break
                if ((self.dl_cont.status in ('error')) or (time.monotonic() - _tstart > 60)):
                    
                    if self.dl_cont.status in ('error'):
                        _msg_error = self.dl_cont.error_message
                    else:
                        _msg_error = 'timeout 60secs'
                    
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [init][error {cont}] {_msg_error}")
                    
                    await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.remove, [self.dl_cont], clean=True)                   

                    cont += 1
                    if cont > 1: 
                        raise AsyncARIA2CDLErrorFatal("Max init repeat")
                    else:                
                        async with self._decor:
                            self.dl_cont = await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.add_uris, self.uris, self.opts)
                        
                        _tstart = time.monotonic()
                
                await asyncio.sleep(0)                   
                    

            if self.reset_event.is_set():
                return
                
            elif self.dl_cont.status in ('complete'):
                self.status = "done"
                                    
            if self.dl_cont.total_length:
                self.filesize = self.dl_cont.total_length
                            

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if self.sem:
                _msg = f"host: {self._host} proxy: {self._proxy} "
            else: _msg = ""  
            if self.dl_cont.status in ('error'):
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e) 
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch] {_msg}error: {_msg_error}")
            self.status = "error" 
            self.error_message = _msg_error          
       


    async def fetch(self):        

        try: 
            
                            
            if self.dl_cont.status in ('active'):        
                self.status = 'downloading'
                
                while (self.dl_cont.status in ('active') and not self.reset_event.is_set()):                    
                   
                    try:                
                        _incsize = self.dl_cont.completed_length - self.down_size
                        self.down_size = self.dl_cont.completed_length
                        async with self.video_downloader.alock: 
                            self.video_downloader.info_dl['down_size'] += _incsize
                        
                        if self.video_downloader.pause_event.is_set():
                            await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.pause,[self.dl_cont])                        
                            await asyncio.wait([self.video_downloader.resume_event.wait(), self.reset_event.wait()], return_when=asyncio.FIRST_COMPLETED)
                            self.video_downloader.pause_event.clear()
                            self.video_downloader.resume_event.clear()
                            if self.reset_event.is_set():                                
                                return                                 
                                
                            async with self._decor: 
                                await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.resume,[self.dl_cont])                        
                            
                            
                        
                        async with self.video_downloader.alock: 
                            await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.dl_cont.update)

                        await asyncio.sleep(0)
                    except BaseException as e:
                        raise
                
                if self.dl_cont.status == "complete":
                    self.status = "done"
                    return
                if self.dl_cont.status == "error":
                    raise AsyncARIA2CDLError("error")
        
        except BaseException as e:            
            if isinstance(e, KeyboardInterrupt):
                raise                       
            if self.sem:
                _msg = f"host: {self._host} proxy: {self._proxy} "
            else: _msg = ""  
            if self.dl_cont.status in ('error'):
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e) 
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch] {_msg}error: {_msg_error}")
            self.status = "error" 
            self.error_message = _msg_error
            

    async def fetch_async(self):
        
        self.reset_event = asyncio.Event()


        while True:
            
            try:
                
                await self.init()
                if self.status in ("done"):
                    return                
                elif self.reset_event.is_set():
                    self.reset_event.clear()
                    await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.remove, [self.dl_cont], force=False, files=False, clean=True)
                    continue
                elif self.status in ("error"):
                    return                            
                await self.fetch()
                if self.status == "done":
                    return                
                elif self.video_downloader.stop_event.is_set():
                    self.status = "stop"
                    return
                elif self.reset_event.is_set():
                    try:
                        self.reset_event.clear()
                        await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.remove, [self.dl_cont], force=False, files=False, clean=True)
                        continue
                    except BaseException as e:
                        if isinstance(e, KeyboardInterrupt):
                            raise
                elif self.dl_cont.status in ('error'): 
                    return
            
            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    raise
                if self.sem:
                    _msg = f"host: {self._host} proxy: {self._proxy} "
                else: _msg = ""  
                if self.dl_cont.status in ('error'):
                    _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
                else:
                    _msg_error = repr(e) 
                logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch] {_msg}error: {_msg_error}")
                self.status = "error" 
                self.error_message = _msg_error
            finally:
                if self.sem:
                    async with self.video_downloader.master_hosts_alock:
                        self.video_downloader.hosts_dl[self._host]['count'] -= 1
                        if self._proxy:
                            self.video_downloader.hosts_dl[self._host]['queue'].put_nowait(self._proxy)
                            self._proxy = None
                            
                    await asyncio.sleep(0)
                

    def print_hookup(self):
        
        msg = ""
        
        if self.status == "done":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host}] Completed\n"
        elif self.status == "init":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host}] Waiting to DL [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n"       
        elif self.status == "error":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host}] ERROR {naturalsize(self.down_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n"
        elif self.status == "stop":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host}] STOPPED {naturalsize(self.down_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n"
        elif self.status == "downloading":
            
            _temp = copy.deepcopy(self.dl_cont)    #mientras calculamos strings progreso no puede haber update de dl_cont, así que deepcopy de la instancia      
            
            _speed_str = _temp.download_speed_string()
            _progress_str = _temp.progress_string()
            _connections = _temp.connections
            _eta_str = _temp.eta_string()
                       
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host}] CONN[{_connections:2d}/{self.nworkers:2d}] DL[{_speed_str}] PR[{_progress_str}] ETA[{_eta_str}]\n"
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f')}]\n"
            
        return msg
        
               
