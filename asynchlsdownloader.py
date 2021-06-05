import asyncio
from httpcore import ConnectTimeout
import httpx
import sys
import traceback
from shutil import rmtree
import m3u8
import binascii
from Crypto.Cipher import AES


from pathlib import Path

from natsort import (
    natsorted,
    ns
)


from urllib.parse import urlparse
import logging

from aiotools import TaskGroup

import time

from common_utils import (
    print_norm_time,
    naturalsize,
    int_or_none
)

import aiofiles



class AsyncHLSDLErrorFatal(Exception):
    

    def __init__(self, msg):
        
        super(AsyncHLSDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncHLSDLError(Exception):
   

    def __init__(self, msg):
        
        super(AsyncHLSDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception
        
class AsyncHLSDLReset(Exception):
    
    def __init__(self, msg):
        
        super(AsyncHLSDLReset, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncHLSDownloader():

    _CHUNK_SIZE = 1048576
    #_CHUNK_SIZE = 1024
       
    def __init__(self, video_dict, vid_dl):


        self.logger = logging.getLogger("async_HLS_DL")
        
                 
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

        self.info_seg = []
        self.segs_to_dl = []
        
        try:            

            res = httpx.get(self.video_url,headers=self.headers)
            m3u8_file = res.text  
            self.m3u8_obj = m3u8.loads(m3u8_file,uri=self.video_url)
 
        except Exception as e:
            self.logger.warning(f"No hay descriptor: {e}", exc_info=True)
            raise AsyncHLSDLErrorFatal("no hay descriptor")
           
        part = 0
        uri_ant = ""
        byte_range = {}            
        
        self.n_dl_segments = 0
        self.down_size = 0

        for i, segment in enumerate(self.m3u8_obj.segments):
                                
            if segment.byterange:
                if segment.uri == uri_ant:
                    part += 1
                else:
                    part = 1
                    
                uri_ant = segment.uri
                file_path =  Path(self.download_path, f"{Path(urlparse(segment.uri).path).name}_part_{part}")
                splitted_byte_range = segment.byterange.split('@')
                sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                byte_range = {
                    'start': sub_range_start,
                    'end': sub_range_start + int(splitted_byte_range[0]),
                }

            else:
                file_path =  Path(self.download_path,Path(urlparse(segment.uri).path).name)
                byte_range = {}
       
            self.tbr = self.info_dict.get('tbr', 0) #for audio streams tbr is not present
            est_size = self.tbr * segment.duration * 1000 / 8
            if file_path.exists():
                size = file_path.stat().st_size
            else: size = -1
            if not est_size: est_size = size            

            
            if  est_size*0.9 <= size <= est_size*1.1:
                self.info_seg.append({"seg" : i+1, "url" : segment.absolute_uri, "key": segment.key, "file" : file_path, "byterange" : byte_range, "downloaded" : True, "estsize" : est_size, "headersize" : None, "size": size, "n_retries": 0, "error" : ["AlreadyDL"]})
                
                self.n_dl_segments += 1
                self.down_size += size
                self.segs_to_dl.append(i+1)
                
            else:
                if file_path.exists(): file_path.unlink()
                self.info_seg.append({"seg" : i+1, "url" : segment.absolute_uri, "key": segment.key, "file" : file_path, "byterange" : byte_range, "downloaded" : False, "estsize" : est_size, "headersize": None, "size": None, "n_retries": 0, "error" : []})
                self.segs_to_dl.append(i+1)
                if segment.key is not None and segment.key.method == 'AES-128':
                    if segment.key.absolute_uri not in self.key_cache:
                        self.key_cache.update({segment.key.absolute_uri : httpx.get(segment.key.absolute_uri, headers=self.headers).content})
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{self.key_cache[segment.key.absolute_uri]}")
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{segment.key.iv}")




        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: \nSegs DL: {self.segsdl()}\nSegs not DL: {self.segsnotdl()}")
        
          
        self.n_total_segments = len(self.m3u8_obj.segments)
        self.calculate_duration() #get total duration
        self.calculate_filesize() #get filesize estimated
        
        if self.filesize == 0: _est_size = "NA"
        else: _est_size = naturalsize(self.filesize, False, False)
        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: total duration {print_norm_time(self.totalduration)} -- estimated filesize {_est_size} -- already downloaded {naturalsize(self.down_size)} -- total segments {self.n_total_segments} -- segments already dl {self.n_dl_segments}")  
        
        if not self.segs_to_dl:
            self.status = "manipulating"
       

                                         
    def calculate_duration(self):
        self.totalduration = 0
        for segment in self.m3u8_obj.segments:
            self.totalduration += segment.duration
            
    def calculate_filesize(self):
                
        self.filesize = int(self.totalduration * 1000 * self.tbr / 8)
        
    async def reset(self): 
        
        async with self.reslock:

            #ya tenemos toda la info, sólo queremos refrescar la info de los segmentos
            
            self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:get video dict: {self.webpage_url}")
            
            try:
                
                info = self.ytdl.extract_info(self.webpage_url, download=False, process=False)
                if not info.get('format_id') and not info.get('requested_formats'):                                
                    info_reset = self.ytdl.process_ie_result(info,download=False)
                else:
                    info_reset = info
                
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:New info video\{info_reset}")
               
                
            except Exception as e:
                raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:fails no descriptor {e}")

            if not info_reset:
                raise AsyncHLSDLErrorFatal(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]: fails no descriptor")         

            try: 
                if info_reset.get('requested_formats'):
                    info_format = [_info_format for _info_format in info_reset['requested_formats'] if _info_format['format_id'] == self.info_dict['format_id']]
                    self.prep_reset(info_format[0])
                else: self.prep_reset(info_reset)
                self.n_reset += 1
            except Exception as e:
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]: Exception occurred when reset: {repr(e)}")
                raise AsyncHLSDLErrorFatal("RESET fails: preparation segs failed")
    
    def prep_reset(self, info_reset):
       
       
 
        self.headers = info_reset.get('headers')
        self.video_url = info_reset.get('url')        

        self.segs_to_dl = []


        try:
            #self.m3u8_obj.append(m3u8.loads(httpx.get(self.video_url, headers=self.headers).text,uri=self.video_url))
            m3u8_file = httpx.get(self.video_url,headers=self.headers).text
            #self.logger.debug(m3u8_file)
            self.m3u8_obj = m3u8.loads(m3u8_file,uri=self.video_url)
        except Exception as e:
            raise AsyncHLSDLErrorFatal("no video descriptor")

        part = 0
        uri_ant = ""
        byte_range = {}            
        
        for i, segment in enumerate(self.m3u8_obj.segments):

            
            if segment.byterange:
                if segment.uri == uri_ant:
                    part += 1
                else:
                    part = 1
                    
                uri_ant = segment.uri
                file_path =  Path(self.download_path, f"{Path(urlparse(segment.uri).path).name}_part_{part}")
                splitted_byte_range = segment.byterange.split('@')
                sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                byte_range = {
                    'start': sub_range_start,
                    'end': sub_range_start + int(splitted_byte_range[0]),
                }

            else:
                file_path =  Path(self.download_path, Path(urlparse(segment.uri).path).name)
                byte_range = {}



            if not self.info_seg[i]['downloaded'] or (self.info_seg[i]['downloaded'] and not self.info_seg[i]['headersize']):
                self.segs_to_dl.append(i+1)                        
                self.info_seg[i]['url'] = segment.absolute_uri
                self.info_seg[i]['file'] = file_path
                if not self.info_seg[i]['downloaded'] and self.info_seg[i]['file'].exists(): 
                    self.info_seg[i]['file'].unlink()
                
                self.info_seg[i]['n_retries'] = 0
                self.info_seg[i]['byterange'] = byte_range
                self.info_seg[i]['key'] = segment.key
                if segment.key is not None and segment.key.method == 'AES-128':
                    if segment.key.absolute_uri not in self.key_cache:
                        self.key_cache[segment.key.absolute_uri] = httpx.get(segment.key.absolute_uri, headers=self.headers).content
                        
        if not self.segs_to_dl:
            self.status = "manipulating"
        else:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:prep_reset:OK {self.segs_to_dl[0]} .. {self.segs_to_dl[-1]}")
            self.segs_queue = asyncio.Queue()
            for seg in self.segs_to_dl: self.segs_queue.put_nowait(seg)
            for _ in range(self.iworkers): self.segs_queue.put_nowait("KILL")
 
    
    async def await_time(self, n):
        res = await asyncio.to_thread(time.sleep, n)
        
    async def await_cancel_group(self, tg):
        res = await asyncio.to_thread(tg._abort)
       
    
    async def fetch(self, nco):

        try:

            client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers) 
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: init worker")
            
            while True:

                q = await self.segs_queue.get()
                
                if q =="KILL":
                    break  
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg[{q}]: request to dl \n{self.info_seg[q-1]}")          
                url = self.info_seg[q - 1]['url']
                filename = self.info_seg[q - 1]['file']
                key = self.info_seg[q - 1]['key']
                cipher = None
                if key is not None and key.method == 'AES-128':
                    iv = binascii.unhexlify(key.iv[2:])
                    cipher = AES.new(self.key_cache[key.absolute_uri], AES.MODE_CBC, iv)
                byte_range = self.info_seg[q - 1]['byterange']
                headers = {}
                if byte_range:
                    headers['range'] = f"bytes={byte_range['start']}-{byte_range['end'] - 1}"

                await asyncio.sleep(0)        
                
                while self.info_seg[q - 1]['n_retries'] < 5:
    
                    try: 
                        
                        async with aiofiles.open(filename, mode='wb') as f:
                            
                            async with client.stream("GET", url, headers=headers) as res:                        
                            
                                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg{q}: {res.status_code} {res.reason_phrase}")
                                if res.status_code >= 400:                                                   
                                    raise AsyncHLSDLErrorFatal(f"Frag:{str(q)} resp code:{str(res)}")
                                
                                else:
                                        
                                    _hsize = int_or_none(res.headers.get('content-length'))
                                    if _hsize:
                                        self.info_seg[q-1]['headersize'] = _hsize
                                    else:
                                        raise AsyncHLSDLErrorFatal(f"Frag:{str(q)} _hsize is None")
                                        
                                    
                                    if self.info_seg[q-1]['downloaded']:
                                        
                                        
                                         _size = self.info_seg[q-1]['size']
                                         if _size and  (_hsize - 5 <=_size <= _hsize + 5):                            
                                    
                                             self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg{q}: Already DL with hsize[{_hsize} and size [{_size}] check[{_hsize - 5 <=_size <= _hsize + 5}]")                                    
                                             break
                                         else:
                                            if filename.exists(): filename.unlink()
                                            self.info_seg[q-1]['downloaded'] = False
                                            async with self.video_downloader.lock:
                                                self.n_dl_segments -= 1
                                                self.down_size -= _size
                                                self.video_downloader.info_dl['down_size'] -= _size
                                                                
                                
                                
                                
                                    num_bytes_downloaded = res.num_bytes_downloaded
                                    async for chunk in res.aiter_bytes(chunk_size=self._CHUNK_SIZE):
                                        if chunk:
                                            if cipher: data = cipher.decrypt(chunk)
                                            else: data = chunk 
                                            #f.write(data)
                                            await f.write(data)
                                            async with self.video_downloader.lock:
                                                self.down_size += (_iter_bytes:=res.num_bytes_downloaded - num_bytes_downloaded)
                                                self.video_downloader.info_dl['down_size'] += _iter_bytes                                
                                            
                                            num_bytes_downloaded = res.num_bytes_downloaded
                                        await asyncio.sleep(0)
                                
                        async with self.video_downloader.lock:
                            self.n_dl_segments += 1
                                
                        self.info_seg[q - 1]['error'].append(str(res))
                        self.info_seg[q - 1]['size'] = filename.stat().st_size
                        self.info_seg[q - 1]['downloaded'] = True
                                
                        await asyncio.sleep(0)
                        break                                                         
                            

                    except AsyncHLSDLErrorFatal as e:
                        self.info_seg[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg[{q}]: fatalError: \n{'!!'.join(lines)}")
                        #await client.aclose()
                        await asyncio.sleep(0)
                        self.channel_queue.put_nowait(("ERROR", asyncio.current_task(), repr(e)))
                        raise                 
                    except asyncio.exceptions.CancelledError as e:
                        self.info_seg[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg[{q}]: CancelledError: \n{'!!'.join(lines)}")
                        #await client.aclose()
                        raise                   
                    except Exception as e:                        
                        self.info_seg[q - 1]['error'].append(repr(e))
                        lines = traceback.format_exception(*sys.exc_info())
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg[{q}]: error {repr(e)} \n{'!!'.join(lines)}")
                        self.info_seg[q - 1]['n_retries'] += 1
                        if "httpx" in str(e.__class__):                            
                            _t = asyncio.create_task(self.await_time(1))
                            await asyncio.wait([_t])
                            await client.aclose()
                            client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)                    
                            await asyncio.sleep(0)                       
                           
                
                if (self.info_seg[q - 1]['n_retries'] == 5): 
                    self.info_seg[q - 1]['error'].append("MaxLimitRetries")
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:seg[{q}]:MaxLimitRetries")
                    await asyncio.sleep(0)
                    await self.channel_queue.put_nowait(("ERROR", asyncio.current_task(), self.info_seg[q - 1]['error']))
                    raise AsyncHLSDLErrorFatal(f"MaxLimitretries seg[{q}]")
                elif self.info_seg[q - 1]['downloaded']:
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:seg[{q}]:DL OK")
                elif not self.info_seg[q - 1]['downloaded']:
                    self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]:seq[{q}]:option ILLOGICAL:n_retries[{self.info_seg[q - 1]['n_retries']}]:DL[{self.info_seg[q - 1]['downloaded']}]")
 
        except Exception as e:
            raise
        else:
            self.channel_queue.put_nowait(("DONE", asyncio.current_task()))
        finally:    
            await client.aclose()
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: bye worker")
                                              
        
        
            
            
    async def monitor(self): 
        #para detectar cuándo hay un primer ERROR fatal para cancelar el resto y hacr RESET; si no hay error fatal sale cuando recibe los DONE de
        #de los fetch y los suma
        
        self.channel_queue._init(0)
        count = 0 
        while True:
            msg = await self.channel_queue.get()
            self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[monitor] {msg} - count[{count}]")
            if "ERROR" in msg[0]:
                self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[monitor] starts cancel every pending task")
                if not msg[1].done() and not msg[1].cancelled():
                    await msg[1]                                
                for _task in self.tasks:
                    _tasks = []
                    if not _task.done() and not _task.cancelled():
                        _task.cancel()
                        #_tasks.append(_task)
                await asyncio.sleep(0)
                #if _tasks: await asyncio.wait(_tasks)                
                self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[monitor] ends cancel every pending task")
                break
            
            elif "DONE" in msg[0]:
                
                count += 1
                if count == self.iworkers:
                    break
                await asyncio.sleep(0)
    
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: BYE MONITOR")
        
    
    async def fetch_async(self):
                
        
        self.segs_queue = asyncio.Queue()
        for seg in self.segs_to_dl:
            self.segs_queue.put_nowait(seg)        
        
        for _ in range(self.iworkers):
            self.segs_queue.put_nowait("KILL")
            
        self.reslock = asyncio.Lock()
        
        self.channel_queue = asyncio.Queue()
            
        n_segs_dl = 0
                    
        while True:

            self.status = "downloading"            
                
            await asyncio.sleep(0)
            
            
            self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] TASKS INIT") 
            
            try:
            #task_mon = asyncio.create_task(self.monitor())
                self.tasks = [asyncio.create_task(self.fetch(i), name=f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][{i}]") for i in range(self.iworkers)]
                try:
                    await asyncio.gather(*self.tasks)
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:gather exception: {repr(e)} \n{'!!'.join(lines)}")
                    _pending_tasks = [t for t in asyncio.all_tasks() if t.get_name().startswith(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]")]
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] pending task list: \n{_pending_tasks}") 
                    for _task in _pending_tasks:
                        if not _task.done() and not _task.cancelled(): _task.cancel()
                    results = await asyncio.gather(*_pending_tasks, return_exceptions=True)
                    if (("AsyncHLSDLErrorFatal" in repr(e)) or ("CancelledError" in repr(e))): raise AsyncHLSDLReset(repr(e))
   
            
            except AsyncHLSDLReset as e:
                
                lines = traceback.format_exception(*sys.exc_info())
                self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:fetch_async:Exception fatal: {repr(e)} \n{'!!'.join(lines)}")

                
                if (not self.reslock.locked()) and (self.n_reset < 10):
                    
                    n = 0
                    while(n < 5):
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:COUNT[{n+1}]")
                        
                        try:
                            _reset_task = asyncio.create_task(self.reset())
                            done, pending = await asyncio.wait([_reset_task])
                            for d in done:d.result()
                            
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:error:[{repr(e)}]\n{'!!'.join(lines)}")
                            n += 1                                
                        else:
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:ok:Pending segs {len(self.segsnotdl())}")                            
                            break
                    
                    if n == 5:
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:error:Max number of attemps in a RESET")                        
    
                        self.status = "error"
                        self.clean_when_error()
                        await asyncio.sleep(0)
                        raise   

                else:
                    if self.n_reset == 10:
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]:error:Max_number_of_resets")  
                        self.status = "error"
                        self.clean_when_error()
                        await asyncio.sleep(0)
                        raise       
                
            except Exception as e:
                
                lines = traceback.format_exception(*sys.exc_info())
                self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:fetch_async:Exception {repr(e)} \n{'!!'.join(lines)}")

            else:    
                if self.segs_queue.empty(): 
                    #todos los segmentos en local
                    break
                else:                    
                    #raise AsyncHLSDLErrorFatal("reset") 
                    if (inc_segs_dl:=len(self.segsdl())) > n_segs_dl:
                        
                        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_segs_dl} -> {inc_segs_dl}] new cycle with no fatal error")
                    else:
                        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [{n_segs_dl} <-> {inc_segs_dl}] no improvement, lets raise an error")
                        
                        raise AsyncHLSDLError("no changes in number of dl segs in once cycle")                    
        

        
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Frags DL completed")


        self.status = "manipulating"

    
    def clean_when_error(self):
        
        for f in self.info_seg:
            if f['downloaded'] == False:
                if f['file'].exists():
                    f['file'].unlink()
   
    
    def ensamble_file(self):        
       
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: Segments DL \n{self.segsdl()}")
        
        try:
            seg_files = natsorted((file for file in self.download_path.iterdir() if file.is_file() and not file.name.startswith('.')), alg=ns.PATH)
            #seg_files = natsorted(self.download_path.iterdir(), alg=ns.PATH)
            if len(seg_files) != len(self.info_seg):
                raise AsyncHLSDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Number of seg files - {len(seg_files)} != segs - {len(self.info_seg)} ")
        
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{self.filename}")
            with open(self.filename, mode='wb') as dest:
                for f in self.info_seg: 
                                        
                    if not f['size']:
                        if f['file'].exists(): f['size'] = f['file'].stat().st_size
                        if f['size'] and (f['headersize'] - 5 <= f['size'] <= f['headersize'] + 5):                
              
                            with open(f['file'], 'rb') as source:
                                dest.write(source.read())
                        else: raise AsyncHLSDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling: {f}")
                    else:
                        with open(f['file'], 'rb') as source:
                                dest.write(source.read())
                        
        
        except Exception as e:
            if self.filename.exists():
                self.filename.unlink()
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Exception ocurred: \n{'!!'.join(lines)}")
            self.status = "error"
            self.clean_when_error() 
            raise
        
        if self.filename.exists():
            rmtree(str(self.download_path),ignore_errors=True)
            self.status = "done" 
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: [ensamble_file] file ensambled")               
        else:
            self.status = "error"  
            self.clean_when_error()                        
            raise AsyncHLSDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling parts")

        
     
    def segsnotdl(self):
        res = []
        for seg in self.info_seg:
            if seg['downloaded'] == False:
                res.append(seg['seg'])
        return res
    
    def segsdl(self):
        res = []
        for seg in self.info_seg:
            if seg['downloaded'] == True:
                res.append({'seg': seg['seg'], 'headersize': seg['headersize'], 'size': seg['size']})
        return res


    def print_hookup(self): 
        
        if self.filesize == 0: _est_size = "NA"
        else: _est_size = naturalsize(self.filesize)
            
        if self.status == "done":
            return (f"[HLS][{self.info_dict['format_id']}]: Completed \n")
        elif self.status == "init":
            return (f"[HLS][{self.info_dict['format_id']}]: Waiting to DL [{_est_size}][{self.n_dl_segments} of {self.n_total_segments}]\n")            
        elif self.status == "error":
            return (f"[HLS][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size)} [{_est_size}][{self.n_dl_segments} of {self.n_total_segments}]\n")
        elif self.status == "downloading":             
            return (f"[HLS][{self.info_dict['format_id']}]: Progress {naturalsize(self.down_size)} [{_est_size}][{self.n_dl_segments} of {self.n_total_segments}]\n")
        elif self.status == "manipulating":
            if self.filename.exists(): _size = self.filename.stat().st_size
            else: _size = 0         
            return (f"[HLS][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size)} [{naturalsize(self.filesize)}]\n")
            
