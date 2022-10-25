import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote, urlparse, urlunparse
import copy
import random

import aria2p

from functools import partial

from utils import (
    async_ex_in_executor,
    sync_to_async,
    async_wait_time,
    naturalsize, 
    none_to_zero, 
    try_get, 
    CONFIG_EXTRACTORS, 
    traverse_obj, 
    limiter_non, 
    ProxyYTDL, 
    get_format_id,
    get_domain,
    CONF_PROXIES_MAX_N_GR_HOST,
    CONF_PROXIES_N_GR_VIDEO,
    CONF_PROXIES_BASE_PORT,
    CONF_ARIA2C_MIN_SIZE_SPLIT,
    CONF_ARIA2C_SPEED_PER_CONNECTION,
    CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED,
    CONF_ARIA2C_N_CHUNKS_CHECK_SPEED,
    CONF_ARIA2C_TIMEOUT_INIT,
    CONF_INTERVAL_GUI
)

from threading import Lock




logger = logging.getLogger("async_ARIA2C_DL")

class AsyncARIA2CDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncARIA2CDLError(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info

class AsyncARIA2CDownloader:    
    _CONFIG = CONFIG_EXTRACTORS.copy()  
    _EX_ARIA2DL = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")
    
    


    async def async_client(self, func, *args, **kwargs):
        await sync_to_async(getattr(self.aria2_client, func), AsyncARIA2CDownloader._EX_ARIA2DL)(*args, **kwargs)
    

    
    def __init__(self, port, enproxy, video_dict, vid_dl):

        self.info_dict = video_dict.copy()
        self.video_downloader = vid_dl                
        self.enproxy = enproxy
        self.aria2_client = aria2p.API(aria2p.Client(port=port))
        
        self.ytdl = traverse_obj(self.video_downloader.info_dl, ('ytdl'))
       
        self.proxies = [i for i in range(CONF_PROXIES_MAX_N_GR_HOST)]

        self.video_url = self.info_dict.get('url')
        self.uris = [self.video_url]        
        self._host = get_domain(self.video_url)        
        
        self.headers = self.info_dict.get('http_headers')  
        
        self.download_path = self.info_dict['download_path']
        
        self.download_path.mkdir(parents=True, exist_ok=True) 
        if (_filename:=self.info_dict.get('_filename')):            
            
            self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + "aria2."  + self.info_dict['ext'])
        else:
            _filename = self.info_dict.get('filename')            
            self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + "aria2."  + self.info_dict['ext'])

        self.filesize = none_to_zero((self.info_dict.get('filesize', 0)))
        self.down_size = 0
                
        self.status = "init"
        self.error_message = ""
        
        self.nworkers = self.video_downloader.info_dl['n_workers']
        
        self.dl_cont = None
        
        self.count_init = 0
        
        self.last_progress_str = "--"
        
        self.prep_init()
        
    def prep_init(self):        

        def getter(x):
        
            value, key_text = try_get(
                [(v,kt) for k,v in self._CONFIG.items() if any(x==(kt:=_) for _ in k)],
                lambda y: y[0]) or ("","") 
            
            if value:
                return(value['ratelimit'].ratelimit(key_text, delay=True), value['maxsplits'])
        
        _extractor = try_get(self.info_dict.get('extractor_key'), lambda x: x.lower())
        self.auto_pasres = False
        _sem = False
        self._mode = "simple"
        if _extractor and _extractor != 'generic':
            self._decor, self._nsplits = getter(_extractor) or (limiter_non.ratelimit("transp", delay=True), self.nworkers)
            if self._nsplits < 16 or _extractor in ['boyfriendtv']: 
                _sem = True
                if _extractor in ['tubeload', 'redload', 'tubeload+cache', 'highload']:#['doodstream', 'vidoza', 'tubeload', 'redload']:
                    self._mode = "group"

        else: 
            self._decor, self._nsplits = limiter_non.ratelimit("transp", delay=True), self.nworkers
            
        if self.enproxy == 0:
            _sem = False
            self._mode = "simple"
                
        self.nworkers = self._nsplits
        if self.filesize:
            _nsplits = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
            if _nsplits < self.nworkers:
                self.nworkers = _nsplits
                

        opts_dict = {
            'split': self.nworkers,
            'header': [f"{key}: {value}" for key,value in self.headers.items()],
            'dir': str(self.download_path),
            'out': self.filename.name,
            'uri-selector': 'inorder',
            'min-split-size': CONF_ARIA2C_MIN_SIZE_SPLIT
            
        }
        

        self.opts = self.aria2_client.get_global_options()
        for key,value in opts_dict.items():
            rc = self.opts.set(key, value)
            if not rc:
                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] options - couldnt set [{key}] to [{value}]")

        with self.ytdl.params['lock']:
     
            if not (_sem:=traverse_obj(self.ytdl.params, ('sem', self._host))):
                _sem = Lock()
                self.ytdl.params['sem'].update({self._host: _sem})


        if _sem:
            
            
            if _extractor in ['doodstream']:
                self.sem = None
                self.auto_pasres = True   
            else:
                self.sem = True            
            
        else: 
            self.sem = None

        self.block_init = True
                    
    async def init(self):

        try:
            if not self.sem: self._proxy = None
            else:
                
                while not self.video_downloader.stop_event.is_set() and not self.video_downloader.reset_event.is_set():
                    async with self.video_downloader.master_hosts_alock:
                        if not self.video_downloader.hosts_dl.get(self._host):
                            self.video_downloader.hosts_dl.update({self._host: {'count': 1, 'queue': asyncio.Queue()}})
                            for el in random.sample(self.proxies, len(self.proxies)):
                                self.video_downloader.hosts_dl[self._host]['queue'].put_nowait(el)
                            self._proxy = "get_one"
                            break
                            
                        else:
                            if self.video_downloader.hosts_dl[self._host]['count'] < CONF_PROXIES_MAX_N_GR_HOST:
                                self.video_downloader.hosts_dl[self._host]['count'] += 1
                                self._proxy = "get_one"
                                break                        
                            else:
                                self._proxy = "wait_for_one"
                    
                    if self._proxy == "wait_for_one":
                        await asyncio.sleep(0)
                        continue
                
                #await asyncio.sleep(0)
                if self.video_downloader.stop_event.is_set() or self.video_downloader.reset_event.is_set():
                    return
                
               
                done, pending = await asyncio.wait([self.video_downloader.reset_event.wait(),self.video_downloader.stop_event.wait(), self.video_downloader.hosts_dl[self._host]['queue'].get()], return_when=asyncio.FIRST_COMPLETED)
                try_get(list(pending), lambda x: (x[0].cancel(), x[1].cancel()))
                if self.video_downloader.stop_event.is_set() or self.video_downloader.reset_event.is_set():
                    return
                
                self._index = try_get(list(done), lambda x: x[0].result())
                
                if self._mode == "simple":
                    
                    self._proxy = f'http://127.0.0.1:{CONF_PROXIES_BASE_PORT+self._index*100}'
                    self.opts.set("all-proxy", self._proxy) 
                
                    try:
                        _ytdl_opts = self.ytdl.params.copy()
                        
                        async with ProxyYTDL(opts=_ytdl_opts, proxy=self._proxy) as proxy_ytdl:
                            proxy_info = get_format_id(
                                proxy_ytdl.sanitize_info(
                                    await proxy_ytdl.async_extract_info(
                                        AsyncARIA2CDownloader._EX_ARIA2DL,
                                        self.info_dict.get('webpage_url'),
                                        download=False)
                                ), self.info_dict['format_id'])
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] mode simple, proxy ip: {self._proxy} init uri: {proxy_info.get('url')}\n{proxy_info}")
                        self.video_url = proxy_info.get('url')
                        self.uris = [unquote(proxy_info.get('url'))]
                                           
                    except Exception as e:
                        logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] mode simple, proxy ip: {self._proxy} init uri: {repr(e)}")
                        self.uris = [None]                     

                    self.opts.set("split", self.nworkers)
                    await asyncio.sleep(0)
                    
                elif self._mode == "group":
                    
                    self._proxy = f'http://127.0.0.1:{CONF_PROXIES_BASE_PORT+self._index*100+50}'
                    self.opts.set("all-proxy", self._proxy) 
                    
                    _uris = []
                    
                    if self.filesize:
                        _n = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
                        _gr = _n // self._nsplits or 1
                        if _gr > CONF_PROXIES_N_GR_VIDEO:                                
                            _gr = CONF_PROXIES_N_GR_VIDEO
                    else:
                        _gr = CONF_PROXIES_N_GR_VIDEO
                        
                    self.nworkers = _gr*self._nsplits
                    self.opts.set('split', self.nworkers)

                    async def get_uri(i):
                        try:
                            _ytdl_opts = self.ytdl.params.copy()
                            _proxy = f'http://127.0.0.1:{CONF_PROXIES_BASE_PORT+self._index*100+i}'
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] proxy ip{i} {_proxy}")
                            if self.video_downloader.stop_event.is_set() or self.video_downloader.reset_event.is_set():
                                return
                            
                            async with ProxyYTDL(opts=_ytdl_opts, proxy=_proxy) as proxy_ytdl:
                                proxy_info = get_format_id(
                                    proxy_ytdl.sanitize_info(
                                        await proxy_ytdl.async_extract_info(
                                            AsyncARIA2CDownloader._EX_ARIA2DL,
                                            self.info_dict.get('webpage_url'),
                                            download=False)
                                    ), self.info_dict['format_id'])
                            
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] proxy ip{i} {_proxy} uri{i} {proxy_info.get('url')}")
                            _url = unquote(proxy_info.get('url'))
                            _url_as_dict = urlparse(_url)._asdict()
                            _url_as_dict['netloc'] = f"__routing={CONF_PROXIES_BASE_PORT+self._index*100+i}__.{_url_as_dict['netloc']}"
                            return urlunparse(list(_url_as_dict.values()))
                        except Exception as e:
                            _msg = f"host: {self._host} proxy[{i}]: {_proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
                            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] init uris {repr(e)} - {_msg}")
                
                    _tasks_get_uri = {asyncio.create_task(get_uri(i)):i for i in range(1, _gr + 1)}
                    _all_tasks = {"get_uri":asyncio.wait(_tasks_get_uri, return_when=asyncio.ALL_COMPLETED),"reset":asyncio.create_task(self.video_downloader.reset_event.wait()), "stop":asyncio.create_task(self.video_downloader.stop_event.wait())}
                    
                    done, pending = await asyncio.wait(list(_all_tasks.values()), return_when=asyncio.FIRST_COMPLETED)                    
                                            
                    if self.video_downloader.reset_event.is_set(): 
                        
                        _all_tasks["stop"].cancel()
                        await asyncio.wait(pending)
                        return
                    if self.video_downloader.stop_event.is_set():
                        
                        _all_tasks["reset"].cancel()
                        await asyncio.wait(pending)
                        return
                    
                    try_get(list(pending), lambda x: (x[0].cancel(), x[1].cancel())) 
                    for _task in _tasks_get_uri:
                        try:
                            if (_uri:=_task.result()):
                                _uris.append(_uri)
                        except Exception as e:                                
                            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] host: {self._host} init uris proxy[{_tasks_get_uri[_task]}] {repr(e)}")

                    self.uris = _uris * self._nsplits
                    
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] proxy {self._proxy} count_init: {self.count_init} uris:\n{self.uris}")

            async with self._decor: 
                self.dl_cont = await async_ex_in_executor(
                    AsyncARIA2CDownloader._EX_ARIA2DL, self.aria2_client.add_uris, self.uris, self.opts)

                self.async_update = sync_to_async(self.dl_cont.update, AsyncARIA2CDownloader._EX_ARIA2DL)
                self.async_pause = sync_to_async(partial(self.aria2_client.pause, [self.dl_cont]), AsyncARIA2CDownloader._EX_ARIA2DL)
                self.async_resume = sync_to_async(partial(self.aria2_client.resume, [self.dl_cont]), AsyncARIA2CDownloader._EX_ARIA2DL)
            
            _tstart = time.monotonic()

            while True:
                if not await async_wait_time(CONF_INTERVAL_GUI, events=[self.video_downloader.stop_event, self.video_downloader.reset_event]):
                    return
                await self.async_update()
                if self.video_downloader.stop_event.is_set() or self.video_downloader.reset_event.is_set():
                    return
                if self.dl_cont and (self.dl_cont.total_length or self.dl_cont.status == "complete"):
                    break
                if ((self.dl_cont and self.dl_cont.status == "error") or (time.monotonic() - _tstart > CONF_ARIA2C_TIMEOUT_INIT)):
                    if self.dl_cont and self.dl_cont.status == "error":
                        _msg_error = self.dl_cont.error_message
                    else:
                        _msg_error = 'timeout'

                    raise AsyncARIA2CDLError(f"init error {_msg_error}")

            
            if self.video_downloader.stop_event.is_set() or self.video_downloader.reset_event.is_set():
                return
                
            if self.dl_cont and self.dl_cont.status == "complete":
                self.status = "done"
                                    
            elif self.dl_cont and self.dl_cont.total_length:
                if not self.filesize:
                    self.filesize = self.dl_cont.total_length
                    async with self.video_downloader.alock:
                        self.video_downloader.info_dl['filesize'] = self.filesize
                        
                            

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if self.sem:
                _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
            else: _msg = ""  
            if self.dl_cont and self.dl_cont.status == "error":
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e) 

            self.error_message = _msg_error
            self.count_init += 1
                 
            if self.count_init < 3:
                logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][init] {_msg} error: {_msg_error} count_init: {self.count_init}, will RESET")
                if 'estado=403' in _msg_error:
                    if not self.sem:
                        
                        self.sem = True
                        self._index = None
                        async with self.video_downloader.master_hosts_alock:
                            if not self.video_downloader.hosts_dl.get(self._host):
                                self.video_downloader.hosts_dl.update({self._host: {'count': 1, 'queue': asyncio.Queue()}})
                                for el in random.sample(self.proxies, len(self.proxies)):
                                    self.video_downloader.hosts_dl[self._host]['queue'].put_nowait(el)
                        
                self.video_downloader.reset_event.set()                
            else:
                self.status = "error"
                logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][init] {_msg} error: {_msg_error} count_init: {self.count_init}") 
                
    async def fetch(self):        

        def getter(x):
            if x <= 2:
                return x*CONF_ARIA2C_SPEED_PER_CONNECTION*0.2                            
            elif x <= (self.nworkers // 2):
                return x*CONF_ARIA2C_SPEED_PER_CONNECTION*1.25
            else:
                return x*CONF_ARIA2C_SPEED_PER_CONNECTION*1.75

        self.count_init = 0
        _speed = []        
        self.block_init = False
        try: 
                         
            while (self.dl_cont and self.dl_cont.status == "active"):                    
                
                try:                
              
                    if not await async_wait_time(CONF_INTERVAL_GUI, events=[self.video_downloader.stop_event, self.video_downloader.reset_event]):
                        return

                    if self.video_downloader.pause_event.is_set():                       
                        
                        await self.async_pause()  

                        done, pending = await asyncio.wait([self.video_downloader.resume_event.wait(),
                                            self.video_downloader.reset_event.wait(),
                                            self.video_downloader.stop_event.wait()],
                                            return_when=asyncio.FIRST_COMPLETED)
                        
                        try_get(list(pending), lambda x: (x[0].cancel(), x[1].cancel()))
                        
                        self.video_downloader.pause_event.clear()
                        self.video_downloader.resume_event.clear()
                        
                        _speed = []
                        
                        if self.video_downloader.stop_event.is_set() or self.video_downloader.reset_event.is_set():
                            return
                        
                        async with self._decor: 
                            await self.async_resume()

                    await self.async_update()                       
                    
                    if self.video_downloader.stop_event.is_set() or self.video_downloader.reset_event.is_set():
                        return
                   
                    _incsize = self.dl_cont.completed_length - self.down_size
                    self.down_size = self.dl_cont.completed_length
                            
                    async with self.video_downloader.alock: 
                        self.video_downloader.info_dl['down_size'] += _incsize

                    _speed.append((self.dl_cont.connections, self.dl_cont.download_speed)) 
                    
                    if len(_speed) > CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED:
                        
                        
                        if all([el[1] == 0 for el in _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED // 2:]]) or all([el[0] == _speed[-1][0] and el[1] < getter(el[0]) for el in _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED:]]):
                            logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch] speed reset: n_el_speed[{len(_speed)}]\n{_speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED:]}")
                            self.video_downloader.reset_event.set()
                            return
                        else: _speed = _speed[1:]


                except BaseException as e:
                    raise
            
            if self.dl_cont and self.dl_cont.status == "complete":
                self.status = "done"
                return
            elif self.dl_cont and self.dl_cont.status == "error":
                raise AsyncARIA2CDLError("error")
        
        except BaseException as e:            
            if isinstance(e, KeyboardInterrupt):
                raise                       
            if self.sem:
                _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
            else: _msg = ""  
            if self.dl_cont and self.dl_cont.status == "error":
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e) 
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch] {_msg}error: {_msg_error}")
            self.status = "error" 
            self.error_message = _msg_error
            
    async def fetch_async(self):
        
        self.status = "downloading"
        
        while True:
            
            try:
                
                await self.init()                
                if self.status == "done":
                    return
                elif self.status == "error":
                    return
                elif self.video_downloader.stop_event.is_set():
                    self.status = "stop"
                    return                
                elif self.video_downloader.reset_event.is_set():
                    self.video_downloader.reset_event.clear()
                    self.block_init = True
                    if self.dl_cont:
                        await async_ex_in_executor(
                            AsyncARIA2CDownloader._EX_ARIA2DL, 
                            self.aria2_client.remove, [self.dl_cont], clean=False)
                        continue

                await self.fetch()
                if self.status == "done":
                    return
                elif self.status == "error":
                    return
                elif self.video_downloader.stop_event.is_set():
                    self.status = "stop"
                    return                
                elif self.video_downloader.reset_event.is_set():
                    self.video_downloader.reset_event.clear()
                    self.block_init = True
                    if self.dl_cont:
                        await async_ex_in_executor(
                            AsyncARIA2CDownloader._EX_ARIA2DL, 
                            self.aria2_client.remove, [self.dl_cont], clean=False)
                        continue            
            except BaseException as e:
                if isinstance(e, KeyboardInterrupt):
                    raise
                if self.sem:
                    _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
                else: _msg = ""  
                if self.dl_cont and self.dl_cont.status == "error":
                    _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
                else:
                    _msg_error = repr(e) 
                logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch] {_msg} error: {_msg_error}")
                self.status = "error" 
                self.error_message = _msg_error
            finally:
                if self.sem:
                    async with self.video_downloader.master_hosts_alock:
                        self.video_downloader.hosts_dl[self._host]['count'] -= 1
                        if self._index: self.video_downloader.hosts_dl[self._host]['queue'].put_nowait(self._index)
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
            
            if not self.block_init:
                _temp = copy.deepcopy(self.dl_cont)    #mientras calculamos strings progreso no puede haber update de dl_cont, así que deepcopy de la instancia      
                
                _speed_str = f'{naturalsize(_temp.download_speed,binary=True,format_="6.2f")}ps'
                _progress_str = f'{_temp.progress:.0f}%'
                self.last_progress_str = _progress_str
                _connections = _temp.connections
                _eta_str = _temp.eta_string()
                        
                msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}] CONN[{_connections:2d}/{self.nworkers:2d}] DL[{_speed_str}] PR[{_progress_str}] ETA[{_eta_str}]\n"
            
            else:
                
                msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}] INIT DL PR[{self.last_progress_str}]\n"
                
                
                
        elif self.status == "manipulating":  
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f')}]\n"
            
        return msg
        
               
