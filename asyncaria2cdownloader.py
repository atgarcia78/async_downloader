import asyncio
import copy
import logging
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote, urlparse

import aria2p

from utils import async_ex_in_executor, naturalsize, none_to_cero, try_get, CONFIG_EXTRACTORS


from threading import Lock, Semaphore
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
    
    _CONFIG = copy.deepcopy(CONFIG_EXTRACTORS)    
    _SEM = {}    
    _LOCK = Lock()    
    _EX_ARIA2DL = None
        
    
    def __init__(self, port, video_dict, vid_dl):

        self.info_dict = copy.deepcopy(video_dict)
        self.video_downloader = vid_dl                
        
        self.aria2_client = aria2p.API(aria2p.Client(port=port))
        
        self.ytdl = self.video_downloader.info_dl['ytdl']
        self.ytdl.params['sem'] = AsyncARIA2CDownloader._SEM
                
        proxies = self.ytdl.params.get('proxy', None)
        if proxies:
            self.proxies = f"http://{proxies}"
        
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')        
        self.video_url = self.info_dict.get('url')
        #self._extra_urls = self.info_dict.get('_extra_urls')
        
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
        
        self.nworkers = self.video_downloader.info_dl['n_workers']
        
        with AsyncARIA2CDownloader._LOCK:
            if not AsyncARIA2CDownloader._EX_ARIA2DL:
                AsyncARIA2CDownloader._EX_ARIA2DL = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")
        
        self.init()       
        
        

    def init(self):
        
        
        def transp(func):
            return func
        
        def getter(x):
        
            value, key_text = try_get([(v,kt) for k,v in self._CONFIG.items() if any(x==(kt:=_) for _ in k)], lambda y: y[0]) or ("","") 
            if value:
                return(value['ratelimit'].ratelimit(key_text, delay=True), value['maxsplits'])
        
        _extractor = self.info_dict.get('extractor', '')
        self.auto_pasres = False
        self.sem = False
        if _extractor and _extractor.lower() != 'generic':
            _decor, _nsplits = getter(_extractor) or (transp, self.nworkers)
            if _extractor in ['doodstream', 'vidoza']:
                self.auto_pasres = True
            if _nsplits < 16: self.sem = True
        else: 
            _decor, _nsplits = transp, self.nworkers
            

        self.nworkers = min(_nsplits, self.nworkers)
        #logger.info(f"{_extractor}:{_nsplits}:{self.nworkers}")
        opts_dict = {
            'split': self.nworkers,
            'max-connection-per-server': self.nworkers,
            'header': [f"{key}: {value}" for key,value in self.headers.items()],# if key.lower() not in ['user-agent', 'referer', 'accept-encoding']], #+ ["Sec-Fetch-Dest: video", "Sec-Fetch-Mode: cors", "Sec-Fetch-Site: cross-site", "Pragma: no-cache", "Cache-Control: no-cache"],
            'dir': str(self.download_path),
            'out': self.filename.name,
            #'check-certificate': self.verifycert,
            #'check-certificate': False,              
            #'user-agent': self.video_downloader.args.useragent
        }
        
        #if _referer:=self.headers.get('Referer'):
        #    opts_dict.update({'referer': _referer})
        
        
        opts = self.aria2_client.get_global_options()
        for key,value in opts_dict.items():
            rc = opts.set(key, value)
            if not rc:
                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] options - couldnt set [{key}] to [{value}]")
                

                 
            
        # def _transf(_url):
        #    return(_url.replace("medialatest-cdn.gayforit.eu", "media.gayforit.eu"))
        
        #uris = [unquote(_transf(self.video_url))]
        uris = [unquote(self.video_url)]
        #if self._extra_urls:
        #    for el in self._extra_urls: 
        #        uris.append(unquote(el))
        
        self._host = urlparse(uris[0]).netloc
                
        if self.sem:
            with AsyncARIA2CDownloader._LOCK:
                if not (AsyncARIA2CDownloader._SEM.get(self._host)):
                    #AsyncARIA2CDownloader._SEM.update({self._host: Semaphore()})
                    AsyncARIA2CDownloader._SEM.update({self._host: PriorityLock()})
                
            
        @_decor
        def _throttle_add_uris():
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{_extractor}] init uris[{uris}]")
            #logger.info(opts.get_struct())
            return self.aria2_client.add_uris(uris, opts)
                        
        try:
            
            if self.sem:
                AsyncARIA2CDownloader._SEM[self._host].acquire()
            
            self.dl_cont = _throttle_add_uris()            
        
            cont = 0            
        
            _tstart = time.monotonic()
            
            while True:
                self.dl_cont.update()
                #logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [init]\n{self.dl_cont._struct}")
                if self.dl_cont.total_length or self.dl_cont.status in ('complete'):
                    break
                if self.dl_cont.status in ('error') or (time.monotonic() - _tstart > 15):
                #if (time.monotonic() - _tstart > 15):
                    if self.dl_cont.status in ('error'):
                        _msg_error = self.dl_cont.error_message
                    else:
                        _msg_error = 'timeout 15secs'
                    
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [init][error {cont}] {_msg_error}")
                    
                    self.aria2_client.remove([self.dl_cont], clean=True)                   

                    if self.sem: AsyncARIA2CDownloader._SEM[self._host].release()
                    
                    cont += 1
                    if cont > 1: 
                        raise AsyncARIA2CDLErrorFatal("Max init repeat")
                    else:
                        time.sleep(1)
                        if self.sem: AsyncARIA2CDownloader._SEM[self._host].acquire()
                
                        self.dl_cont = _throttle_add_uris()
                    
                        _tstart = time.monotonic()                   
                    

            if self.dl_cont.status in ('active'):
                self.aria2_client.pause([self.dl_cont])
                
            elif self.dl_cont.status in ('complete'):
                self.status = "done"
                #AsyncARIA2CDownloader._SEM[self._host].release()
                                    
            if self.dl_cont.total_length:
                self.filesize = self.dl_cont.total_length
                            

        except AsyncARIA2CDLErrorFatal as e:
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {repr(e)}")
            self.status = "error"
            #AsyncARIA2CDownloader._SEM[self._host].release()
            raise        
        except Exception as e:                         
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {repr(e)}")
            self.status = "error"
            #AsyncARIA2CDownloader._SEM[self._host].release()
            try:
                self.error_message = self.dl_cont.error_message
            except Exception:
                self.error_message = repr(e)            
            raise AsyncARIA2CDLErrorFatal(self.error_message)
        finally:
            if self.sem: AsyncARIA2CDownloader._SEM[self._host].release()
        
        

    async def fetch_async(self):        

        try: 
            
            if self.sem: await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, AsyncARIA2CDownloader._SEM[self._host].acquire, priority=50)
            
            await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.resume,[self.dl_cont])
            
            while True:
                await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.dl_cont.update)
                #logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [fetch_init]\n{self.dl_cont._struct}")
                if self.dl_cont.status in ('active', 'error'):
                    break                    
                await asyncio.sleep(0)
            
            if self.dl_cont.status in ('active'):        
                self.status = "downloading"  
                
                #timer0 = time.monotonic()
                while self.dl_cont.status in ('active'):                    
                                    
                    _incsize = self.dl_cont.completed_length - self.down_size
                    self.down_size = self.dl_cont.completed_length
                    async with self.video_downloader.lock: 
                        self.video_downloader.info_dl['down_size'] += _incsize
                    if self.video_downloader.stop_event.is_set():
                        await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.remove,[self.dl_cont], force=False, files=False, clean=False)
                        self.status = 'stop'
                        return
                        
                    if self.video_downloader.pause_event.is_set():
                        await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.pause,[self.dl_cont])                        
                        await self.video_downloader.resume_event.wait()
                        await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.resume,[self.dl_cont])                        
                        self.video_downloader.pause_event.clear()
                        self.video_downloader.resume_event.clear()
                        while True:
                            await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.dl_cont.update)
                            if self.dl_cont.status in ('active', 'error', 'complete'):
                                break                    
                            await asyncio.sleep(0)
                        
                    else: 
                        await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, self.dl_cont.update)
                        # if ((timer1:=time.monotonic()) - timer0 > 1):
                        #     logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [fetch_dl]\n{self.dl_cont._struct}")
                        #     timer0 = timer1
                    await asyncio.sleep(0)
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [fetch_findl]\n{self.dl_cont._struct}")
            if self.dl_cont.status in ('complete'): self.status = "done"
            elif self.dl_cont.status in ('error'): 
                self.status = 'error'
                self.error_message = self.dl_cont.error_message

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] {repr(e)}\n{'!!'.join(lines)}")
            self.status = "error"
            self.error_message = repr(e)
        finally:
            if self.sem: await async_ex_in_executor(AsyncARIA2CDownloader._EX_ARIA2DL, AsyncARIA2CDownloader._SEM[self._host].release)
            
            
            
    def print_hookup(self):
        
        msg = ""
        
        if self.status == "done":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Completed\n"
        elif self.status == "init":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Waiting to DL [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n"       
        elif self.status == "error":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]"
        elif self.status == "stop":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: STOPPED {naturalsize(self.down_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]"
        elif self.status == "downloading":
            _temp = copy.deepcopy(self.dl_cont)    #mientras calculamos strings progreso no puede haber update de dl_cont, así que deepcopy de la instancia      
            
            _speed_str = _temp.download_speed_string()
            _progress_str = _temp.progress_string()
            _connections = _temp.connections
            _eta_str = _temp.eta_string()
                       
            msg = f"[ARIA2C][{self.info_dict['format_id']}]:(CONN[{_connections:2d}/{self.nworkers:2d}]) DL[{_speed_str}] PR[{_progress_str}] ETA[{_eta_str}]\n"
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f')}]\n"
            
        return msg
        
               
