import asyncio
from concurrent.futures import ThreadPoolExecutor
import httpx
import sys
import traceback
from shutil import (
    rmtree

)
import m3u8
import binascii
from Crypto.Cipher import AES


from pathlib import Path

from natsort import (
    natsorted,
    ns
)


from collections import deque
from urllib.parse import urlparse
import logging

from asyncio_pool import AioPool



import time

from common_utils import (
    print_norm_time,
    naturalsize,
    int_or_none
)

import aiofiles

import concurrent.futures

from asynclogger import AsyncLogger

class AsyncHLSDLErrorFatal(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncHLSDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncHLSDLError(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncHLSDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncHLSDownloader():

    _CHUNK_SIZE = 1048576
    #_CHUNK_SIZE = 1024
       
    def __init__(self, video_dict, vid_dl):


        self.logger = logging.getLogger("async_HLS_DL")
        self.alogger = AsyncLogger(self.logger)
                 
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
        #self.client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
        #self.cl = httpx.Client(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
        
        self.base_download_path = self.info_dict['download_path']
        if (_filename:=self.info_dict.get('_filename')):
            self.download_path = Path(self.base_download_path, self.info_dict['format_id'])
            self.download_path.mkdir(parents=True, exist_ok=True) 
            self.filename = Path(self.base_download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
        else:
            self.download_path = self.base_download_path
            self.filename = self.info_dict.get('filename')

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
            
            
            #hsize = int_or_none(httpx.get(segment.absolute_uri,headers=self.headers).headers.get('content-length'))
            
            #self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: seg[{i}] size[{size}] est_size[{est_size}] hsize[{hsize}]")
            
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

        #ya tenemos toda la info, sólo queremos refrescar la info de los segmentos
        
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET:get video dict: {self.webpage_url}")
        
        try:
            
            info_reset = self.ytdl.extract_info(self.webpage_url, download=False, process=True)
            
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:New info video")
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:{info_reset}")
            
        except Exception as e:
            raise AsyncHLSDLErrorFatal("RESET fails: no descriptor")

        if not info_reset:
            raise AsyncHLSDLErrorFatal("RESET fails: no descriptor")           

        try: 
            info_format = [_info_format for _info_format in info_reset['requested_formats'] if _info_format['format_id'] == self.info_dict['format_id']]
            self.prep_reset(info_format[0])
            self.n_reset += 1
        except Exception as e:
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Exception occurred when reset: {str(e)}")
            raise AsyncHLSDLErrorFatal("RESET fails: preparation segs failed")
    
    def prep_reset(self, info_reset):
       
       
 
        self.headers = info_reset.get('headers')
        self.video_url = info_reset.get('url')
        
        # self.client = httpx.AsyncClient(headers=self.headers, http2=False, timeout=timeout, limits=limits, verify=self.verifycert, proxies=self.proxies)

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
 
    
    async def await_time(self, n):
        ex = ThreadPoolExecutor(max_workers=1)
        loop = asyncio.get_running_loop()
        
        blocking_task = [loop.run_in_executor(ex, time.sleep, n)]
        await asyncio.wait(blocking_task)
       
    
    async def fetch(self, nco):
        
        client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)

        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: init worker")
        
        while True:

            q = await self.segs_queue.get()
            
            if q =="KILL":
                break  
            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: seg[{q}]: request to dl \n{self.info_seg[q-1]}")          
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
                    
                    async with client.stream("GET", url, headers=headers) as res:
                    
                        #res = await self.client.get(url,headers=headers)
                        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg{q}: {res.status_code} {res.reason_phrase}")
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
                            
                                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg{q}: Already DL with hsize[{_hsize} and size [{_size}] check[{_hsize - 5 <=_size <= _hsize + 5}]")                                    
                                    break
                                else:
                                    filename.unlink()
                                    self.info_seg[q-1]['downloaded'] = False
                                    async with self.video_downloader.lock:
                                        self.n_dl_segments -= 1
                                        self.down_size -= _size
                                        self.video_downloader.info_dl['down_size'] -= _size
                                                            
                            
                            
                            async with aiofiles.open(filename, mode='wb') as f:
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
                            
                            async with self.video_downloader.lock:
                                self.n_dl_segments += 1
                            self.info_seg[q - 1]['downloaded'] = True
                            self.info_seg[q - 1]['error'].append(str(res))
                            self.info_seg[q - 1]['size'] = filename.stat().st_size
                            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg[{q}]: DL OK")
                            await asyncio.sleep(0)
                            break                                                         
                        

                except AsyncHLSDLErrorFatal as e:
                    self.info_seg[q - 1]['error'].append(f'{type(e)}')
                    lines = traceback.format_exception(*sys.exc_info())
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg[{q}]: fatalError: \n{'!!'.join(lines)}")
                    await client.aclose()
                    await asyncio.sleep(0)
                    raise AsyncHLSDLErrorFatal(f"{e}")                
                
                except httpx.ReadTimeout as e:
                    self.info_seg[q - 1]['error'].append(f'{type(e)}')
                    lines = traceback.format_exception(*sys.exc_info())
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg[{q}]: timeout error: \n{'!!'.join(lines)}")
                    self.info_seg[q - 1]['n_retries'] += 1
                    await client.aclose()
                    await self.await_time(1)
                    client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)                    
                    await asyncio.sleep(0)                    
                except Exception as e:
                    #await self.alogger.debug(f"Exception ocurred: {e}", exc_info=True)
                    self.info_seg[q - 1]['error'].append(f'{type(e)}')
                    lines = traceback.format_exception(*sys.exc_info())
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seg[{q}]: error: \n{'!!'.join(lines)}")
                    self.info_seg[q - 1]['n_retries'] += 1
                    await client.aclose()
                    client = httpx.AsyncClient(limits=self.limits, timeout=self.timeout, verify=self.verifycert, proxies=self.proxies, headers=self.headers)
                    await asyncio.sleep(0)
            
            if self.info_seg[q - 1]['n_retries'] == 5:
                self.info_seg[q - 1]['error'].append("MaxLimitRetries")
                await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker-{nco}]: seq[{q}]: MaxLimitRetries")
                await client.aclose()
                await asyncio.sleep(0)
                raise AsyncHLSDLErrorFatal(f"MaxLimitretries seq[{q}]")
            
        
        await client.aclose()
        
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:[worker{nco}]: bye worker")
            
            

    async def fetch_async(self):
                
        
        self.segs_queue = asyncio.Queue()
        for seg in self.segs_to_dl:
            self.segs_queue.put_nowait(seg)        
        for _ in range(self.iworkers):
            self.segs_queue.put_nowait("KILL")
           
        cont = 5
                    
        while True:

            self.status = "downloading"
            try:
                
                await asyncio.sleep(0)
                
                async with AioPool(size=self.iworkers) as pool:

                    futures = [pool.spawn_n(self.fetch(i)) for i in range(self.iworkers)]
    
                    done_tasks, pending_tasks = await asyncio.wait(futures, return_when=asyncio.FIRST_EXCEPTION)    

                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Fuera del wait tasks, vamos a cancelar pending tasks")           
                    if pending_tasks:
                        try:
                            await pool.cancel(pending_tasks)
                            await self.alogger.debug(f"{self.webpage_url}: {len(pending_tasks)} tasks pending cancelled")
                        except Exception as e:
                            await self.alogger.debug(f"{self.webpage_url}: {str(e)}")
                    
                        await asyncio.gather(*pending_tasks, return_exceptions=True)
                               
                            
                #recorro done para lanzar las excepciones, si hay un Fatal no lo caapturo en el try except
                #del bucle para q llegue al otro except y lance el reset
                fatal_errors = []
                if done_tasks:
                    for done in done_tasks:
                        try:
                            done.result()
                        except AsyncHLSDLErrorFatal as e:
                            fatal_errors.append(e)
                            lines = traceback.format_exception(*sys.exc_info())
                            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:rec excepciones: \n{'!!'.join(lines)}")
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:rec excepciones: \n{'!!'.join(lines)}")

                if fatal_errors:
                    raise AsyncHLSDLErrorFatal("reset")
                else: 
                    if self.segs_queue.empty():
                        break
                    else:
                        cont -= 1
                        if (cont == 0): raise AsyncHLSDLErrorFatal("reset") 
                        await asyncio.sleep(0) 

            except AsyncHLSDLErrorFatal as e:
                lines = traceback.format_exception(*sys.exc_info())
                await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:fetch_async:Exception fatal curred: \n{'!!'.join(lines)}")
                if cont == 0: 
                    self.status = "error"
                    self.clean_when_error()
                    await asyncio.sleep(0)
                    raise

                if self.n_reset < 5:
                    cont = 5
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET[{self.n_reset}]")
                    #await asyncio.wait_for(self.reset(), None) 
                    _reset_task = [asyncio.create_task(self.reset())]
                    await asyncio.wait(_reset_task)
                    self.segs_queue = asyncio.Queue()
                    for seg in self.segs_to_dl:
                        self.segs_queue.put_nowait(seg)
                    for _ in range(self.iworkers):
                        self.segs_queue.put_nowait("KILL")
                                       
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:RESET:Pending segs {len(self.segsnotdl())}")
                    await asyncio.sleep(0)
                else:
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:ERROR:Max_number_of_resets {type(e)}")
                    await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Pending segs {len(self.segsnotdl())}")
    
                    self.status = "error"
                    self.clean_when_error()
                    await asyncio.sleep(0)
                    raise
            except Exception as e:
                lines = traceback.format_exception(*sys.exc_info())
                await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:fetch_async:Exception ocurred: \n{'!!'.join(lines)}")

       
        
        await self.alogger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Frags DL completed")


        self.status = "manipulating"
        
        # try:
        #     loop = asyncio.get_running_loop()
        #     ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        #     blocking_task = [loop.run_in_executor(ex, self.ensamble_file)]
        #     completed, pending = await asyncio.wait(blocking_task)
        #     for c in completed:
        #         c.result()
                
        #     await asyncio.sleep(0)
                    
        # except AsyncHLSDLError as e:
        #     lines = traceback.format_exception(*sys.exc_info())
        #     await self.alogger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]:Exception ocurred when ensambling segs: \n{'!!'.join(lines)}") 
        #     self.status = "error"
        #     self.clean_when_error()
        #     await asyncio.sleep(0)
        #     raise
        
        # if self.filename.exists():
        #     self.status == "done"  
        #     rmtree(str(self.download_path),ignore_errors=True)      
        # else:
        #     self.status = "error"    
        #     self.clean_when_error()
        #     raise AsyncHLSDLError(f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]: error when ensambling parts")       
        
        

    
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
                #if f['file'].exists():
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
            
