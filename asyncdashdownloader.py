import asyncio
from concurrent.futures import CancelledError

import httpx
import sys
import traceback
from shutil import rmtree


from pathlib import Path

from natsort import (
    natsorted,
    ns
)

import subprocess
from urllib.parse import urlparse, urljoin
import logging

import random

from utils import (
    print_norm_time,
    naturalsize,
    int_or_none
)

import aiofiles
import time


class AsyncDASHDLErrorFatal(Exception):
    

    def __init__(self, msg):
        
        super(AsyncDASHDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncDASHDLError(Exception):
   

    def __init__(self, msg):
        
        super(AsyncDASHDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception
        
class AsyncDASHDLReset(Exception):
    
    def __init__(self, msg):
        
        super(AsyncDASHDLReset, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncDASHDownloader():

    _CHUNK_SIZE = 1048576
    #_CHUNK_SIZE = 1024
       
    def __init__(self, video_dict, vid_dl):


        self.logger = logging.getLogger("async_DASH_DL")
        
        self._type = "dash"         
        self.info_dict = video_dict
        self.video_downloader = vid_dl
        self.iworkers = vid_dl.info_dl['n_workers'] 
        self.video_url = self.info_dict.get('url')
        self.webpage_url = self.info_dict.get('webpage_url')
        self.fragment_base_url = self.info_dict.get('fragment_base_url')
        

        self.id = self.info_dict['id']
        
        self.ytdl = vid_dl.info_dl['ytdl']
        proxies = self.ytdl.params.get('proxy', None)
        if proxies:
            self.proxies = f"http://{proxies}"
        else: self.proxies = None
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')

        self.timeout = httpx.Timeout(10, connect=30)
        self.limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)
        self.headers = self.info_dict.get('http_headers')
        self.base_download_path = Path(str(self.info_dict['download_path']))
        if (_filename:=self.info_dict.get('_filename')):
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True) 
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + ".ts")
        else:
            _filename = self.info_dict.get('filename')
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + ".ts")

        self.key_cache = dict()
        self.n_reset = 0
        
  

        self.prep_init()        
        self.status = "init"            
                
    
    def prep_init(self):

        self.info_frag = []
        self.frags_to_dl = []
        
        
        self.n_dl_fragments = 0
        self.down_size = 0
        
        self.tbr = self.info_dict.get('tbr', 0) #for audio streams tbr is not present

        for i, fragment in enumerate(self.info_dict['fragments']):
                                
            if not (_url:=fragment.get('url')):
                _url = urljoin(self.fragment_base_url, fragment['path'])
                _file_path =  Path(self.download_path, fragment['path'])              

       
            
            est_size = self.tbr * fragment.get('duration', 0) * 1000 / 8
            
            if _file_path.exists():
                size = _file_path.stat().st_size
            
                self.info_frag.append({"frag" : i+1, "url" : _url, "file" : _file_path, "downloaded" : True, "estsize" : est_size, "headersize" : None, "size": size, "n_retries": 0, "error" : ["AlreadyDL"]})
                
                self.n_dl_fragments += 1
                self.down_size += size
                self.frags_to_dl.append(i+1)
                
            else:
                self.info_frag.append({"frag" : i+1, "url" : _url, "file" : _file_path, "downloaded" : False, "estsize" : est_size, "headersize": None, "size": None, "n_retries": 0, "error" : []})
                self.frags_to_dl.append(i+1)
                
 
                
           

        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: \nFrags DL: {self.fragsdl()}\nFrags not DL: {self.fragsnotdl()}")
        
          
        self.n_total_fragments = len(self.info_dict['fragments'])
        self.calculate_duration() #get total duration
        self.calculate_filesize() #get filesize estimated
        
        if self.filesize == 0: _est_size = "NA"
        else: _est_size = naturalsize(self.filesize, False, False)
        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: total duration {print_norm_time(self.totalduration)} -- estimated filesize {_est_size} -- already downloaded {naturalsize(self.down_size)} -- total fragments {self.n_total_fragments} -- fragments already dl {self.n_dl_fragments}")  
        
        if not self.frags_to_dl:
            self.status = "manipulating"
       

                                         
    def calculate_duration(self):
        self.totalduration = 0
        for fragment in self.info_dict['fragments']:
            self.totalduration += fragment.get('duration', 0)
            
    def calculate_filesize(self):                
        self.filesize = int(self.totalduration * 1000 * self.tbr / 8)
        
    
    def reset(self):         
        #async with self.reslock:
            #ya tenemos toda la info, sólo queremos refrescar la info de los fragmentos
            
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:get video dict: {self.webpage_url}")
        
        try:
                      
            # rc = subprocess.call(f'sudo /Users/antoniotorres/.dotfiles/bin/pywgdown.sh', shell=True)   
            
            info = self.ytdl.extract_info(self.webpage_url, download=False, process=False)
            if not info.get('format_id') and not info.get('requested_formats'):                                
                info_reset = self.ytdl.process_ie_result(info,download=False)
            else:
                info_reset = info
            
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:New info video\{info_reset}")
            
            
        except Exception as e:
            raise AsyncDASHDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:fails no descriptor {e}")

        if not info_reset:
            raise AsyncDASHDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]: fails no descriptor")         

        try: 
            if info_reset.get('requested_formats'):
                info_format = [_info_format for _info_format in info_reset['requested_formats'] if _info_format['format_id'] == self.info_dict['format_id']]
                self.prep_reset(info_format[0])
            else: self.prep_reset(info_reset)
            self.n_reset += 1
        except Exception as e:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]: Exception occurred when reset: {repr(e)}")
            raise AsyncDASHDLErrorFatal("RESET fails: preparation frags failed")
    
    
    def prep_reset(self, info_reset):       
       
        
        self.headers = self.info_dict['http_headers'] = info_reset.get('http_headers')
        self.video_url = self.info_dict['url'] = info_reset.get('url')
        self.info_dict['manifest_url'] = info_reset.get('manifest_url')
        self.fragment_base_url = self.info_dict['fragment_base_url'] = info_reset.get('fragment_base_url')         

        self.frags_to_dl = []

        
        for i, fragment in enumerate(self.info_dict['fragments']):

            
            if not (_url:=fragment.get('url')):
                _url = urljoin(self.fragment_base_url, fragment['path'])
                _file_path =  Path(self.download_path, fragment['path'])            



            if not self.info_frag[i]['downloaded'] or (self.info_frag[i]['downloaded'] and not self.info_frag[i]['headersize']):
                self.frags_to_dl.append(i+1)                        
                self.info_frag[i]['url'] = _url
                self.info_frag[i]['file'] = _file_path
                if not self.info_frag[i]['downloaded'] and self.info_frag[i]['file'].exists(): 
                    self.info_frag[i]['file'].unlink()
                
                self.info_frag[i]['n_retries'] = 0


           
        if not self.frags_to_dl:
            self.status = "manipulating"
        else:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:prep_reset:OK {self.frags_to_dl[0]} .. {self.frags_to_dl[-1]}")
            
 
    
    async def wait_time(self, n):
        _timer = httpx._utils.Timer()
        await _timer.async_start()
        while True:
            _t = await _timer.async_elapsed()
            if _t > n: break
            await asyncio.sleep(0)     
     
    
    async def fetch(self, nco):

        
        try:

            _timer = httpx._utils.Timer()
            client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers) 
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: init worker")
            
            while True:

                q = await self.frags_queue.get()
                
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]")
                
                if q == "KILL":
                    break  
                         
                url = self.info_frag[q - 1]['url']
                filename = Path(self.info_frag[q - 1]['file'])
               
                await asyncio.sleep(0)        
                
                while self.info_frag[q - 1]['n_retries'] < 5:
    
                    try: 
                        
                        async with aiofiles.open(filename, mode='ab') as f:
                            
                            # req = client.build_request("GET",url, headers=headers)
                            
                            # res = await client.send(req, stream=True)
                            
                            async with client.stream("GET", url, timeout=30) as res:
                                                 
                            
                                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag{q}: {res.status_code} {res.reason_phrase}")
                                if res.status_code >= 400:                                                   
                                    raise AsyncDASHDLErrorFatal(f"Frag:{str(q)} resp code:{str(res)}")
                                
                                else:
                                        
                                    _hsize = int_or_none(res.headers.get('content-length'))
                                    if _hsize:
                                        self.info_frag[q-1]['headersize'] = _hsize
                                    else:
                                        raise AsyncDASHDLErrorFatal(f"Frag:{str(q)} _hsize is None")                                    
                                    
                                    if self.info_frag[q-1]['downloaded']:                                    
                                        
                                        if (await asyncio.to_thread(filename.exists)):
                                            _size = self.info_frag[q-1]['size'] = (await asyncio.to_thread(filename.stat)).st_size
                                            if _size and  (_hsize - 5 <= _size <= _hsize + 5):                            
                                    
                                                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag{q}: Already DL with hsize[{_hsize}] and size [{_size}] check[{_hsize - 5 <=_size <= _hsize + 5}]")                                    
                                                break
                                            else:
                                                await asyncio.to_thread(filename.unlink)
                                                self.info_frag[q-1]['downloaded'] = False
                                                async with self.video_downloader.lock:
                                                    self.n_dl_fragments -= 1
                                                    self.down_size -= _size
                                                    self.video_downloader.info_dl['down_size'] -= _size                                                            
                                        else:
                                            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag{q}: frag with mark downloaded but file doesnt exists")
                                            self.info_frag[q-1]['downloaded'] = False
                                            async with self.video_downloader.lock:
                                                    self.n_dl_fragments -= 1
                        
        

                                    if self.info_frag[q-1]['headersize'] < self._CHUNK_SIZE:
                                        _chunk_size = self.info_frag[q-1]['headersize']
                                    else:
                                        _chunk_size = self._CHUNK_SIZE
                                    
                                    num_bytes_downloaded = res.num_bytes_downloaded
                                
                                    await _timer.async_start()
                                    
                                    self.info_frag[q - 1]['time2dlchunks'] = []
                                    
                                    async for chunk in res.aiter_bytes(chunk_size=_chunk_size): 
                                        
                                        _timechunk = await _timer.async_elapsed() 
                                        self.info_frag[q - 1]['time2dlchunks'].append(_timechunk)                             
                                        await asyncio.sleep(0)                                       
                                        await f.write(chunk)                                                       
                                        async with self.video_downloader.lock:
                                            self.down_size += (_iter_bytes:=(res.num_bytes_downloaded - num_bytes_downloaded))                                        
                                            self.video_downloader.info_dl['down_size'] += _iter_bytes 
                                        num_bytes_downloaded = res.num_bytes_downloaded                                        
                                        await _timer.async_start()
                                    
                        _size = (await asyncio.to_thread(filename.stat)).st_size
                        _hsize = self.info_frag[q-1]['headersize']
                        if (_hsize - 5 <= _size <= _hsize + 5):
                            self.info_frag[q - 1]['downloaded'] = True 
                            self.info_frag[q - 1]['size'] = _size
                            async with self.video_downloader.lock:
                                self.n_dl_fragments += 1     
                                
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:frag[{q}] OK DL: total[{self.n_dl_fragments}]\n{self.info_frag[q - 1]}")
                            break
                        else: 
                            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:frag[{q}] end of streaming. Fragment not completed\n{self.info_frag[q - 1]}")                                    
                            raise AsyncDASHDLError(f"Fragment not completed frag[{q}]")     
          
                                                       
                    except AsyncDASHDLErrorFatal as e:
                        self.info_frag[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: fatalError: \n{'!!'.join(lines)}")                                               
                        raise                 
                    except (asyncio.exceptions.CancelledError, asyncio.CancelledError, CancelledError) as e:
                        self.info_frag[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: CancelledError: \n{'!!'.join(lines)}")
                        raise                   
                    except Exception as e:                        
                        self.info_frag[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: error {repr(e)}")
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: frag[{q}]: error {repr(e)} \n{'!!'.join(lines)}")
                        self.info_frag[q - 1]['n_retries'] += 1
                        if (await asyncio.to_thread(filename.exists)):
                            _size = (await asyncio.to_thread(filename.stat)).st_size                            
                            await asyncio.to_thread(filename.unlink)
                        
                            async with self.video_downloader.lock:
                                self.down_size -= _size                                        
                                self.video_downloader.info_dl['down_size'] -= _size
                        
                        if self.info_frag[q - 1]['n_retries'] < 5:
                                                     
                                await res.aclose()
                                await client.aclose()
                                await self.wait_time(random.choice([i for i in range(1,5)]))
                                client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
                                await asyncio.sleep(0)
                                
                                
                        else:
                            self.info_frag[q - 1]['error'].append("MaxLimitRetries")
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:frag[{q}]:MaxLimitRetries")
                            raise AsyncDASHDLErrorFatal(f"MaxLimitretries frag[{q}]")
                    finally:
                        await res.aclose() 
                        

        finally:    
            await client.aclose()
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: bye worker")
    


    
    async def fetch_async(self):
                
        
        self.frags_queue = asyncio.Queue()
        for frag in self.frags_to_dl:
            self.frags_queue.put_nowait(frag)        
        
        for _ in range(self.iworkers):
            self.frags_queue.put_nowait("KILL")
            
        #self.reslock = asyncio.Lock()
        
        #self.channel_queue = asyncio.Queue()
            
        n_frags_dl = 0
                    
        while True:

            self.status = "downloading"            
                
            await asyncio.sleep(0)
            
            
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] TASKS INIT") 
            
            try:
           
                self.tasks = [asyncio.create_task(self.fetch(i), name=f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]") for i in range(self.iworkers)]
                done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)
   
                if pending:
                    #__pending_tasks = [t for t in self.tasks if not t.cancelled() and not t.done()]
                    _pending_tasks = [t for t in pending if not t.cancelled() and not t.done()]
                    
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] PENDING {_pending_tasks}") 
                    for t in _pending_tasks: t.cancel()
                    # for _task in _pending_tasks:
                    #     if not _task.done() and not _task.cancelled(): _task.cancel()
                    await asyncio.gather(*_pending_tasks,return_exceptions=True)
                    await asyncio.sleep(0)
                    
                if done:
                    _res = []
                    for t in done:
                        try:                            
                            t.result()  
                        except Exception as e:
                            _res.append(repr(e))                            
                    
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] DONE [{len(done)}] with exceptions {_res}")
                    for e in _res:
                        if (("AsyncDASHDLErrorFatal" in e) or ("CancelledError" in e)): raise AsyncDASHDLReset(e)
   
            
            except AsyncDASHDLReset as e:
                
                lines = traceback.format_exception(*sys.exc_info())
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:fetch_async:RESET:{repr(e)}\n{'!!'.join(lines)}")

                
                #if (not self.reslock.locked()) and (self.n_reset < 10):
                if self.n_reset < 10:
                
                    n = 0
                    while(n < 5):
                        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{n}]")
                        await asyncio.sleep(0)
                        
                        try:
                            _reset_task = asyncio.create_task(asyncio.to_thread(self.reset))
                            done, pending = await asyncio.wait([_reset_task])
                            await asyncio.sleep(0)
                            for d in done:d.result()
                            self.frags_queue = asyncio.Queue()
                            for frag in self.frags_to_dl: self.frags_queue.put_nowait(frag)
                            for _ in range(self.iworkers): self.frags_queue.put_nowait("KILL")
                            
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{n}]:ERROR:[{repr(e)}]\n{'!!'.join(lines)}")
                            n += 1 
                            if n == 5:
                                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{n}]:ERROR:Max number of attemps in a RESET")                        
    
                                self.status = "error"
                                await self.clean_when_error()
                                await asyncio.sleep(0)
                                raise   
                               
                        else:
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{n}]:OK:Pending frags {len(self.fragsnotdl())}")                            
                            break                    
                   
                else:
                    if self.n_reset == 10:
                        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{n}]:ERROR:Max_number_of_resets")  
                        self.status = "error"
                        await self.clean_when_error()
                        await asyncio.sleep(0)
                        raise       
                
            except Exception as e:
                
                lines = traceback.format_exception(*sys.exc_info())
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:fetch_async:Exception {repr(e)} \n{'!!'.join(lines)}")

            else:    
                if self.frags_queue.empty(): 
                    #todos los fragmentos en local
                    break
                else:                    
                    #raise AsyncDASHDLErrorFatal("reset") 
                    if (inc_frags_dl:=len(self.fragsdl())) > n_frags_dl:
                        
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_frags_dl} -> {inc_frags_dl}] new cycle with no fatal error")
                    else:
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_frags_dl} <-> {inc_frags_dl}] no improvement, lets raise an error")
                        
                        raise AsyncDASHDLError("no changes in number of dl frags in once cycle")                    
        

        
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Frags DL completed")


        self.status = "manipulating"

    
    async def clean_when_error(self):
        
        for f in self.info_frag:
            if f['downloaded'] == False:
                
                if (await asyncio.to_thread(f['file'].exists)):
                    await asyncio.to_thread(f['file'].unlink)
   
    def sync_clean_when_error(self):
            
        for f in self.info_frag:
            if f['downloaded'] == False:
                if f['file'].exists():
                    f['file'].unlink()
                    
    
    def ensamble_file(self):        
       
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: Fragments DL \n{self.fragsdl()}")
        
        try:
            frag_files = natsorted((file for file in self.download_path.iterdir() if file.is_file() and not file.name.startswith('.')), alg=ns.PATH)
            #frag_files = natsorted(self.download_path.iterdir(), alg=ns.PATH)
            if len(frag_files) != len(self.info_frag):
                raise AsyncDASHDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Number of frag files - {len(frag_files)} != frags - {len(self.info_frag)} ")
        
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{self.filename}")
            with open(self.filename, mode='wb') as dest:
                for f in self.info_frag: 
                                        
                    if not f['size']:
                        if f['file'].exists(): f['size'] = f['file'].stat().st_size
                        if f['size'] and (f['headersize'] - 5 <= f['size'] <= f['headersize'] + 5):                
              
                            with open(f['file'], 'rb') as source:
                                dest.write(source.read())
                        else: raise AsyncDASHDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling: {f}")
                    else:
                        with open(f['file'], 'rb') as source:
                                dest.write(source.read())
                        
        
        except Exception as e:
            if self.filename.exists():
                self.filename.unlink()
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Exception ocurred: \n{'!!'.join(lines)}")
            self.status = "error"
            self.sync_clean_when_error() 
            raise
        
        if self.filename.exists():
            rmtree(str(self.download_path),ignore_errors=True)
            self.status = "done" 
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [ensamble_file] file ensambled")               
        else:
            self.status = "error"  
            self.sync_clean_when_error()                        
            raise AsyncDASHDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling parts")

        
     
    def fragsnotdl(self):
        res = []
        for frag in self.info_frag:
            if frag['downloaded'] == False:
                res.append(frag['frag'])
        return res
    
    def fragsdl(self):
        res = []
        for frag in self.info_frag:
            if frag['downloaded'] == True:
                res.append({'frag': frag['frag'], 'headersize': frag['headersize'], 'size': frag['size']})
        return res


    def print_hookup(self): 
        
        if self.filesize == 0: _est_size = "NA"
        else: _est_size = naturalsize(self.filesize)
            
        if self.status == "done":
            return (f"[DASH][{self.info_dict['format_id']}]: Completed \n")
        elif self.status == "init":
            return (f"[DASH][{self.info_dict['format_id']}]: Waiting to DL [{_est_size}][{self.n_dl_fragments} of {self.n_total_fragments}]\n")            
        elif self.status == "error":
            return (f"[DASH][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size)} [{_est_size}][{self.n_dl_fragments} of {self.n_total_fragments}]\n")
        elif self.status == "downloading":             
            return (f"[DASH][{self.info_dict['format_id']}]: Progress {naturalsize(self.down_size)} [{_est_size}][{self.n_dl_fragments} of {self.n_total_fragments}]\n")
        elif self.status == "manipulating":
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            return (f"[DASH][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size)} [{naturalsize(self.filesize)}]\n")
            
