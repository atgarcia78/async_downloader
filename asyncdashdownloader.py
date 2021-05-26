import asyncio
import httpx
import sys
from shutil import (
    rmtree

)

from Crypto.Cipher import AES


from pathlib import Path
from user_agent import generate_user_agent
from datetime import datetime

from natsort import (
    natsorted,
    ns
)


from urllib.parse import urljoin
import logging
from aiotools import TaskGroup



from common_utils import (
    print_norm_time,
    naturalsize,
    foldersize,
    folderfiles
)

import aiofiles

from concurrent.futures import ThreadPoolExecutor



class AsyncDASHDLErrorFatal(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncDASHDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncDASHDLError(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncDASHDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncDASHDownloader():

    _CHUNK_SIZE = 1048576
   # _CHUNK_SIZE = 1024
       
    def __init__(self, video_dict, vid_dl):


        self.logger = logging.getLogger("async_DASH_DL")
        #self.alogger = AsyncLogger(self.logger)
                 
        self.info_dict = video_dict
        self.video_downloader = vid_dl
        self.iworkers = vid_dl.info_dl['n_workers']
        self.video_url = video_dict.get('url')
        self.webpage_url = video_dict.get('webpage_url')
        

        self.videoid = self.info_dict['id']
        
        self.ytdl = vid_dl.info_dl['ytdl']
        proxies = self.ytdl.params.get('proxy', None)
        if proxies:
            self.proxies = f"http://{proxies}"
        else: self.proxies = None
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')

        self.timeout = httpx.Timeout(10, connect=30)
        self.limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)
        self.headers = self.info_dict.get('http_headers')
                
        self.base_download_path = self.info_dict['download_path']
        if (_filename:=self.info_dict.get('_filename')):
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True) 
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
        else:
            
            _filename = self.info_dict.get('filename')
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])

        self.key_cache = dict()
        self.n_reset = 0
  

        self.prep_init()
        
        self.status = "init"            
                
    
    def prep_init(self):

        self.info_frag = []
        self.frags_to_dl = []
        self.n_dl_fragments = 0
        self.down_size = 0

        for i, fragment in enumerate(self.info_dict['fragments']):
                                
            if not (_url:=fragment.get('url')):
                _url = urljoin(self.info_dict['fragment_base_url'], fragment['path'])
                _file_path =  Path(self.download_path,fragment['path'])              


       
            self.tbr = self.info_dict.get('tbr', 0) #for audio streams tbr is not present
            est_size = self.tbr * fragment.get('duration', 0) * 1000 / 8
            if _file_path.exists():
                    size = _file_path.stat().st_size
            else: size = -1
            if not est_size: est_size = size 
            
            if size < 0.90 * est_size:
                if _file_path.exists(): _file_path.unlink()
                self.info_frag.append({"frag" : i+1, "url" : _url, "file" : _file_path, "downloaded" : False, "n_retries": 0, "error" : []})
                self.frags_to_dl.append(i+1)
                

            else:
                self.info_frag.append({"frag" : i+1, "url" : _url, "file" : _file_path, "downloaded" : True, "n_retries": 0, "error" : []})
                self.n_dl_fragments += 1
                self.down_size += size


        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Frags\n {self.info_frag}")
        
          
        self.n_total_fragments = len(self.info_dict['fragments'])
        self.calculate_duration() #get total duration
        self.calculate_filesize() #get filesize estimated        
        #init in case there's part DL
        if self.filesize == 0: _est_size = "NA"
        else: _est_size = naturalsize(self.filesize, False, False)
        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: total duration {print_norm_time(self.totalduration)} -- estimated filesize {_est_size} -- already downloaded {naturalsize(self.down_size)} -- total fragments {self.n_total_fragments} -- fragments already dl {self.n_dl_fragments}")       
        
         
           
        
                                         
    def calculate_duration(self):
        self.totalduration = 0
        for fragment in self.info_dict['fragments']:
            self.totalduration += fragment.get('duration', 0)
            
    def calculate_filesize(self):
                
        self.filesize = int(self.totalduration * 1000 * self.tbr / 8)
        
    async def reset(self):
 
        
        #ya tenemos toda la info, sólo queremos refrescar la info de los fragmentos
        
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET:get video dict: {self.webpage_url}")
        
        try:
            
            info_reset = self.ytdl.extract_info(self.webpage_url, download=False, process=True)
            
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:New info video")
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{info_reset}")
            
        except Exception as e:
            raise AsyncDASHDLErrorFatal("RESET fails: no descriptor")

        if not info_reset:
            raise AsyncDASHDLErrorFatal("RESET fails: no descriptor")           

        try: 
            self.prep_reset(info_reset)
            self.n_reset += 1
        except Exception as e:
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Exception occurred when reset: {str(e)}")
            raise AsyncDASHDLErrorFatal("RESET fails: preparation frags failed")

    
    def prep_reset(self, info_reset):
       

        self.headers = info_reset.get('headers')
        self.video_url = info_reset.get('url')
        

        self.frags_to_dl = []
        
        
        for i, fragment in enumerate(self.info_dict['fragments']):
                                
            if not (_url:=fragment.get('url')):
                _url = urljoin(self.info_dict['fragment_base_url'], fragment['path'])
                _file_path =  Path(self.download_path,fragment['path'])            



            if not self.info_frag[i]['downloaded']:
                self.frags_to_dl.append(i+1)                        
                self.info_frag[i]['url'] = _url
                self.info_frag[i]['file'] = _file_path
                if (self.info_frag[i]['file']).exists(): 
                    (self.info_frag[i]['file']).unlink()
                self.info_frag[i]['n_retries'] = 0
                
        
                
 
    async def fetch(self, nco):
        
        client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: init worker")
        
        
        
        while True:

            q = await self.frags_queue.get()
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][worker{nco}]: frag[{q}] : request to dl")
            if q =="KILL":
                break
            url = self.info_frag[q - 1]['url']
            filename = self.info_frag[q - 1]['file']                                           
            
            
            await asyncio.sleep(0)
            
            while self.info_frag[q - 1]['n_retries'] < 10:
 
                try: 
                    
                    
                    async with client.stream("GET", url) as res:
                    
                   
                        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][worker{nco}]: frag[{q}] : {res.status_code} {res.reason_phrase}")
                        if res.status_code >= 400:
                            await asyncio.sleep(0)                        
                            raise AsyncDASHDLErrorFatal(f"Frag:{str(q)} resp code:{str(res)}")
                        
                        #with open(filename, mode='wb') as f:
                        async with aiofiles.open(filename, mode='wb') as f:
                            num_bytes_downloaded = res.num_bytes_downloaded
                            async for chunk in res.aiter_bytes(chunk_size=self._CHUNK_SIZE):
                                
                                await f.write(chunk)
                                self.down_size += (_iter_bytes:=res.num_bytes_downloaded - num_bytes_downloaded)
                                if self.tbr:
                                    async with self.video_downloader.lock:
                                        self.video_downloader.down_size += _iter_bytes                                
                                num_bytes_downloaded = res.num_bytes_downloaded                                
                            
                    
                        self.n_dl_fragments += 1
                        self.info_frag[q - 1]['downloaded'] = True
                        self.info_frag[q - 1]['error'].append(str(res))
                        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][worker{nco}]: frag[{q}] : Completed")                        
                        break                                                         
                    

                except AsyncDASHDLErrorFatal as e:
                    self.info_frag[q - 1]['error'].append(str(e))
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: frag[{q}] : fatalError: {type(e)}: {url}", exc_info=True)
                    await client.aclose()
                    await asyncio.sleep(0)
                    raise AsyncDASHDLErrorFatal(f"{str(e)}")
                except (AsyncDASHDLError, httpx.CloseError, httpx.ProxyError, httpx.ConnectError, AttributeError, RuntimeError) as e:
                    #await self.alogger.debug(f"Exception ocurred: {e}", exc_info=True)
                    self.info_frag[q - 1]['error'].append(f"{str(e)}")
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: frag[{q}]: error: {type(e)}: {url}", exc_info=True)
                    self.info_frag[q - 1]['n_retries'] += 1  
                    await asyncio.sleep(0)                                     
                except (httpx.ReadTimeout) as e:
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: frag[{q}]: timeout: {type(e)}: {url}", exc_info=True)
                    await client.aclose()
                    client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers) 
                    self.info_frag[q - 1]['error'].append(f"{str(e)}")
                    self.info_frag[q - 1]['n_retries'] += 1  
                    await asyncio.sleep(0)  

            
            if self.info_frag[q - 1]['n_retries'] == 10:
                self.info_frag[q - 1]['error'].append("MaxLimitRetries")
                await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: (MaxLimitRetries)Frag: {q}")
                await client.aclose()
                raise AsyncDASHDLErrorFatal(f"MaxLimitretries Frag: {q}")
        
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: bye worker")
        await client.aclose()


    async def fetch_async(self):        
        
        cont = 5
        self.frags_queue = asyncio.Queue()
        for frag in self.frags_to_dl:
            self.frags_queue.put_nowait(frag)        
        for _ in range(self.iworkers):
            self.frags_queue.put_nowait("KILL")
        
        while (cont > 0):

            self.status = "downloading"
            try:
                
                worker_dl_tasks = [asyncio.create_task(self.fetch(i)) for i in range(self.iworkers)]
                
                await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: bucle while cont [{cont}] Workers lanzados [{len(worker_dl_tasks)}]") 
                
                done_tasks, pending_tasks = await asyncio.wait(worker_dl_tasks,return_when=asyncio.FIRST_EXCEPTION)

                
                await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: fuera del wait con Done tasks [{len(done_tasks)}] Pending [{len(pending_tasks)}")  
                            
                if pending_tasks:
                    for ptask in pending_tasks:
                        try:
                            ptask.cancel()                        
                        except Exception as e:
                            await self.alogger.debug(f"{self.webpage_url}: {str(e)}")               
                   
                             
                #recorro done para lanzar las excepciones, si hay un Fatal no lo caapturo en el try except
                #del bucle para q llegue al otro except y lance el reset
                fatal_errors = []
                if done_tasks:
                    for done in done_tasks:
                        try:
                            done.result()
                        except AsyncDASHDLErrorFatal as e:
                            await self.alogger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:rec excepciones DASH Fatal de las finalizadas:{str(e)}", exc_info=True)
                            fatal_errors.append(e)
                        except (httpx.HTTPError, httpx.StreamError, AsyncDASHDLError) as e:
                            await self.alogger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:rec excepciones comunes de las finalizadas :{str(e)}", exc_info=True)

                if fatal_errors: raise AsyncDASHDLErrorFatal("reset")
                else: 
                    if not (_notdl:=self.fragsnotdl()):
                        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: ALL fragments DL")
                        break
                    else:
                        cont -= 1
                        if cont == 0: raise AsyncDASHDLErrorFatal("cont is zero")    


            except AsyncDASHDLErrorFatal as e:
                await self.alogger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Break of while loop due to an exception: {type(e)}", exc_info=True)
                
                if cont == 0: 
                    self.status = "error"
                    #await self.client.aclose()                    
                    raise

                if self.n_reset < 5:
                    cont = 5
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET)")
                    _reset_task = [asyncio.create_task(self.reset())]
                    await asyncio.wait(_reset_task)
                    self.frags_queue = asyncio.Queue()
                    for frag in self.frags_to_dl:
                        self.frags_queue.put_nowait(frag)
                    for _ in range(self.iworkers):
                        self.frags_queue.put_nowait("KILL")                   
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET COMPLETED: Pending frags {len(self.fragsnotdl())}")
                else:
                    await self.alogger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:ERROR:Max_number_of_resets {str(e)}")
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Pending frags {len(self.fragsnotdl())}")
                    self.status = "error"
                    #await self.client.aclose()   
                    raise
            except Exception as e:
                await self.alogger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Break of while loop due to an exception: {type(e)}", exc_info=True)
                

        #vemos en disco si están todos los fragmentos
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:while break fetch_async")
        
        if (_notdl:=self.fragsnotdl()):
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: frags not dl \n {_notdl}")
            raise AsyncDASHDLErrorFatal(f"Fragments pending: {len(_notdl)}")        

        try:
            loop = asyncio.get_running_loop()
            with ThreadPoolExecutor(max_workers=1) as ex:
                _blocking_task = [loop.run_in_executor(ex, self.ensamble_file())]
                done, pending = await asyncio.wait(_blocking_task)
            if done:
                 
                rc = done[0].result()
                await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: return code ffmpeg {rc}")             
                       
        except (AsyncDASHDLError,AsyncDASHDLErrorFatal) as e:
            await self.alogger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Exception ocurred when ensambling frags: {str(e)}", exc_info=True) 
            self.status = "error"
            raise
        
        if self.filename.exists():
            if self.filesize:
                if self.filename.stat().st_size in range(self.filesize - 100, self.filesize + 100): self.status == "done"
                else: self.status = "error"
            else:
                if self.filename.stat().st_size > 0: self.status = "done"
                else: self.status = "error"
        else:
            self.status = "error"
         
        if self.status == "done" : rmtree(str(self.download_path),ignore_errors=True)    
        elif self.status == "error": raise AsyncDASHDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling parts")       
        
       
    
    def ensamble_file(self):        
       
        
        try:
            frag_files = natsorted((file for file in self.download_path.iterdir() if file.is_file() and not file.name.startswith('.')), alg=ns.PATH)
            #frag_files = natsorted(self.download_path.iterdir(), alg=ns.PATH)
            if len(frag_files) != len(self.info_frag):
                raise AsyncDASHDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Number of frag files - {len(frag_files)} != frags - {len(self.info_frag)} ")
        
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{self.filename}")
            with open(self.filename, mode='wb') as dest:
                for f in self.info_frag:                    
                    with open(f['file'], 'rb') as source:
                        dest.write(source.read())
        
        except Exception as e:
            if self.filename.exists():
                self.filename.unlink()
            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Exception ocurred: {str(e)}")
            raise

        
     
    def fragsnotdl(self):
        res = []
        for frag in self.info_frag:
            if frag['downloaded'] == False:
                res.append(frag['frag'])
        return res


    # def videoexits(self):
    #     return self.filename.exists()

    # def remove(self):        
    #     rmtree(str(self.base_download_path),ignore_errors=True)
        
   
    
    def print_hookup(self): 
        
        if self.filesize == 0: _est_size = "NA"
        else: _est_size = naturalsize(self.filesize)
            
        if self.status == "done":
            return (f"[DASH][{self.info_dict['format_id']}]: Completed \n")
        elif self.status == "init":
            return (f"[DASH][{self.info_dict['format_id']}]: Waiting to enter in the pool [{_est_size}][{self.n_dl_fragments} of {self.n_total_fragments}]\n")            
        elif self.status == "error":
            return (f"[DASH][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size)} [{_est_size}][{self.n_dl_fragments} of {self.n_total_fragments}]\n")
        elif self.status == "downloading":            
            return (f"[DASH][{self.info_dict['format_id']}]: Progress {naturalsize(self.down_size)} [{_est_size}][{self.n_dl_fragments} of {self.n_total_fragments}]\n")
        elif self.status == "manipulating":
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0
            return (f"[DASH][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size)} [{naturalsize(self.filesize)}]\n")
            
            
