import asyncio
from asyncio.exceptions import (
    CancelledError,
    InvalidStateError
)
import httpx
import sys
from shutil import (
    rmtree

)
import m3u8
import binascii
from Crypto.Cipher import AES


from pathlib import Path
from user_agent import generate_user_agent
from datetime import datetime

from natsort import (
    natsorted,
    ns
)
from youtube_dl.utils import sanitize_filename

from collections import deque
from urllib.parse import urlparse
import logging

from asyncio_pool import AioPool

import hashlib

from common_utils import (
    print_norm_time,
    naturalsize,
    foldersize,
    folderfiles
)

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

       
    def __init__(self, ie_result, ytdl, workers):


        self.logger = logging.getLogger("async_HLS_DL")
                 
        self.info_dict = ie_result
        self.ytdl = ytdl
        self.proxies = ytdl.params.get('proxy', None)
        if self.proxies:
            self.proxies = f"http://{self.proxies}"
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')
        self.webpage_url = self.info_dict.get('webpage_url')

        self.date_file = datetime.now().strftime("%Y%m%d")
        
        self.videoid = self.info_dict.get('id', None)
        if not self.videoid:
            self.videoid = int(hashlib.sha256(b"{self.webpage_url}").hexdigest(),16) % 10**8

        self.base_download_path = Path(Path.home(),"testing", self.date_file, str(self.videoid))
        
        #self.filename = Path(Path.home(),"testing", self.date_file, self.date_file + "_" + \
        #    str(self.videoid) + "_" + sanitize_filename(self.info_dict['title'], restricted=True) + "." + self.info_dict['ext'])
        
        self.filename = Path(Path.home(),"testing", self.date_file, str(self.videoid) + "_" + sanitize_filename(self.info_dict['title'], restricted=True) + "." + self.info_dict['ext'])

        if self.info_dict.get('requested_formats', None): #multi stream
            self.n_streams = 2
        else:
            self.n_streams = 1

        self.n_reset = 0
  
        self.iworkers = workers

        self.prep_init()
        
        self.status = "init"            
                
    
    def prep_init(self):

           
        #self.frag_queue = []
        self.headers = []
        self.stream_url = []        
        self.m3u8_obj = []
        self.info_frag = []
        self.key_cache = dict()
        self.client = []
        
        self.download_path = []       
        self.filename_stream = []

        
        

        #timeout = httpx.Timeout(20, connect=60)
        timeout = None
        limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)

        if self.n_streams == 2:

            self.headers.append(self.info_dict['requested_formats'][0]['http_headers'])
            self.headers.append(self.info_dict['requested_formats'][1]['http_headers'])
            self.stream_url.append(self.info_dict['requested_formats'][0]['url'])
            self.stream_url.append(self.info_dict['requested_formats'][1]['url'])

            self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, limits=limits, timeout=timeout, verify=self.verifycert, proxies=self.proxies))
            self.client.append(httpx.AsyncClient(headers=self.headers[1], http2=False, limits=limits, timeout=timeout, verify=self.verifycert, proxies=self.proxies))

            self.download_path.append(Path(self.base_download_path, "video"))
            self.download_path.append(Path(self.base_download_path, "audio"))
            self.download_path[0].mkdir(parents=True, exist_ok=True)
            self.download_path[1].mkdir(parents=True, exist_ok=True)

            self.filename_stream.append(Path(self.download_path[0].parent, self.date_file + "_" + \
                str(self.videoid) + "_" + self.info_dict['title'] + "_video" + "." + \
                self.info_dict['requested_formats'][0]['ext']))
            self.filename_stream.append(Path(self.download_path[1].parent, self.date_file + "_" + \
                str(self.videoid) + "_" + self.info_dict['title'] + "_audio" + "." + \
                self.info_dict['requested_formats'][1]['ext']))
            
            self.info_frag.append(list())
            self.info_frag.append(list())

        else: #single stream
            self.headers.append(self.info_dict['http_headers'])
            self.stream_url.append(self.info_dict['url'])

            self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, limits=limits, timeout=timeout, verify=self.verifycert, proxies=self.proxies))
            self.download_path.append(self.base_download_path)
            self.download_path[0].mkdir(parents=True, exist_ok=True)
            self.filename_stream.append(self.filename)
            
            self.info_frag.append(list())
            

        self.frag_queue = deque()
        
        if self.n_streams == 1:
            tbr = self.info_dict.get('tbr', 0)
        else:
            tbr = self.info_dict['requested_formats'][0].get('tbr', 0)
        
        for j in range(self.n_streams):

            try:
                #self.m3u8_obj.append(m3u8.loads(httpx.get(self.stream_url[j], headers=self.headers[j]).text,uri=self.stream_url[j]))
                #self.logger.debug("HEADERS")
                #self.logger.debug(self.headers[j])
                res = httpx.get(self.stream_url[j],headers=self.headers[j])
                #self.logger.debug(res.request)
                #self.logger.debug(res.request.headers)
                m3u8_file = res.text
                self.logger.debug("M3U8 file")
                #self.logger.debug(m3u8_file)
                self.m3u8_obj.append(m3u8.loads(m3u8_file,uri=self.stream_url[j]))
                self.logger.debug(self.m3u8_obj[j].data)
            except Exception as e:
                self.logger.warning(f"No hay descriptor: {e}", exc_info=True)
                raise AsyncHLSDLErrorFatal("no hay descriptor")


            #self.logger.debug((self.m3u8_obj[j].data))
        
            part = 0
            uri_ant = ""
            byte_range = {}            
            

            for i, segment in enumerate(self.m3u8_obj[j].segments):
                                 
                if segment.byterange:
                    if segment.uri == uri_ant:
                        part += 1
                    else:
                        part = 1
                        
                    uri_ant = segment.uri
                    file_path =  Path(self.download_path[j], f"{Path(urlparse(segment.uri).path).name}_part_{part}")
                    splitted_byte_range = segment.byterange.split('@')
                    sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                    byte_range = {
                        'start': sub_range_start,
                        'end': sub_range_start + int(splitted_byte_range[0]),
                    }

                else:
                    file_path =  Path(self.download_path[j],Path(urlparse(segment.uri).path).name)
                    byte_range = {}


                # size = 0
                # try:
                #     size = file_path.stat().st_size
                # except Exception as e:
                #     pass

                # if size == 0:
                est_size = tbr * segment.duration * 1000 / 8
                if file_path.exists():
                    size = file_path.stat().st_size
                else: size = -1
                
                if size < 0.90 * est_size:
                    if size != -1: file_path.unlink()
                    self.info_frag[j].append({"frag" : i+1, "url" : segment.absolute_uri, "key": segment.key, "file" : file_path, "byterange" : byte_range, "downloaded" : False, "n_retries": 0, "error" : []})
                    self.frag_queue.append((j, i+1))
                    if segment.key is not None and segment.key.method == 'AES-128':
                        if segment.key.absolute_uri not in self.key_cache:
                            self.key_cache[segment.key.absolute_uri] = httpx.get(segment.key.absolute_uri, headers=self.headers[j]).content
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{self.key_cache[segment.key.absolute_uri]}")
                            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{segment.key.iv}")

                else:
                    self.info_frag[j].append({"frag" : i+1, "url" : segment.absolute_uri, "file" : file_path, "byterange" : byte_range, "downloaded" : True, "n_retries": 0, "error" : "AlreadyDL"})
 

            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Frags for stream {str(j)}")
            self.logger.debug(self.info_frag[j])
            
        self.get_number_segments()
        self.calculate_duration() #get total duration
        self.calculate_filesize() #get filesize estimated
        self.n_dl_segments = folderfiles(str(self.base_download_path))
        self.down_size = foldersize(str(self.base_download_path)) #init in case there's part DL
        
        self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: total duration {print_norm_time(self.totalduration)} -- estimated filesize {naturalsize(self.filesize, False, False)} -- already downloaded {naturalsize(self.down_size)} -- total segments {self.n_total_segments} -- segments already dl {self.n_dl_segments}")       
        
         
    def get_number_segments(self):
        self.n_total_segments = len(self.m3u8_obj[0].segments)
        if self.n_streams == 2:
            self.n_total_segments += len(self.m3u8_obj[1].segments)        
        
                                         
    def calculate_duration(self):
        self.totalduration = 0
        for segment in self.m3u8_obj[0].segments:
            self.totalduration += segment.duration
            
    def calculate_filesize(self):
        if self.n_streams == 1:
            tbr = self.info_dict.get('tbr', 0)
        else:
            tbr = self.info_dict['requested_formats'][0].get('tbr', 0)
        
        self.filesize = self.totalduration * 1000 * tbr / 8 #bytes  
        
    async def reset(self):
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:RESET:close clients")
        
        try:
            await self.client[0].aclose()
            if self.n_streams == 2:
                await self.client[1].aclose()
        except Exception as e:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Exception ocurred when closing client: {str(e)}", exc_info=True)    
 
        
        #ya tenemos toda la info, sólo queremos refrescar la info de los fragmentos
        
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:RESET:get video dict: {self.webpage_url}")
        
        try:
            
            info = self.ytdl.process_ie_result({'_type':'url_transparent', 'url': self.webpage_url, 'title': self.info_dict['title'], 'ie_key': self.info_dict['extractor_key']}, download=False)
            if info.get('playlist'):
                info_reset = info['entries'][self.info_dict['playlist_index']]
            else:
                info_reset = info
            
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:New info video")
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{info_reset}")
            
        except Exception as e:
            raise AsyncHLSDLErrorFatal("RESET fails: no descriptor")

        if not info_reset:
            raise AsyncHLSDLErrorFatal("RESET fails: no descriptor")           

        try: 
            self.prep_reset(info_reset)
            self.n_reset += 1
        except Exception as e:
            self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Exception occurred when reset: {str(e)}")
            raise AsyncHLSDLErrorFatal("RESET fails: preparation frags failed")

    
    def prep_reset(self, info_reset):
       
        #self.frag_queue = []
        self.headers = []
        self.stream_url = []        
        self.m3u8_obj = []
        self.key_cache = dict()
        self.client = []

        #timeout = httpx.Timeout(20, connect=60)
        timeout = None

        if self.n_streams == 2:
            
 
            self.headers.append(info_reset['requested_formats'][0]['http_headers'])
            self.headers.append(info_reset['requested_formats'][1]['http_headers'])
            self.stream_url.append(info_reset['requested_formats'][0]['url'])
            self.stream_url.append(info_reset['requested_formats'][1]['url'])
           

            self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))

            self.client.append(httpx.AsyncClient(headers=self.headers[1], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))



        else: #single stream
            self.headers.append(info_reset['http_headers'])
            self.stream_url.append(info_reset['url'])


            self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))



        

        self.frag_queue = deque()

        for j in range(self.n_streams):

            try:
                #self.m3u8_obj.append(m3u8.loads(httpx.get(self.stream_url[j], headers=self.headers[j]).text,uri=self.stream_url[j]))
                m3u8_file = httpx.get(self.stream_url[j],headers=self.headers[j]).text
                #self.logger.debug(m3u8_file)
                self.m3u8_obj.append(m3u8.loads(m3u8_file,uri=self.stream_url[j]))
            except Exception as e:
                raise AsyncHLSDLErrorFatal("no video descriptor")

            part = 0
            uri_ant = ""
            byte_range = {}            
            
            for i, segment in enumerate(self.m3u8_obj[j].segments):

                
                if segment.byterange:
                    if segment.uri == uri_ant:
                        part += 1
                    else:
                        part = 1
                        
                    uri_ant = segment.uri
                    file_path =  Path(self.download_path[j], f"{Path(urlparse(segment.uri).path).name}_part_{part}")
                    splitted_byte_range = segment.byterange.split('@')
                    sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                    byte_range = {
                        'start': sub_range_start,
                        'end': sub_range_start + int(splitted_byte_range[0]),
                    }

                else:
                    file_path =  Path(self.download_path[j], Path(urlparse(segment.uri).path).name)
                    byte_range = {}



                if not self.info_frag[j][i]['downloaded']:
                    self.frag_queue.append((j, i+1))                        
                    self.info_frag[j][i]['url'] = segment.absolute_uri
                    self.info_frag[j][i]['file'] = file_path
                    if (self.info_frag[j][i]['file']).exists(): 
                        (self.info_frag[j][i]['file']).unlink()
                    self.info_frag[j][i]['n_retries'] = 0
                    self.info_frag[j][i]['byterange'] = byte_range
                    self.info_frag[j][i]['key'] = segment.key
                    if segment.key is not None and segment.key.method == 'AES-128':
                        if segment.key.absolute_uri not in self.key_cache:
                            self.key_cache[segment.key.absolute_uri] = httpx.get(segment.key.absolute_uri, headers=self.headers[j]).content
 
    async def fetch(self, nco):
        
        while self.frag_queue:

            j, q = self.frag_queue.popleft()
            url = self.info_frag[j][q - 1]['url']
            filename = self.info_frag[j][q - 1]['file']
            key = self.info_frag[j][q - 1]['key']
            cipher = None
            if key is not None and key.method == 'AES-128':
                iv = binascii.unhexlify(key.iv[2:])
                cipher = AES.new(self.key_cache[key.absolute_uri], AES.MODE_CBC, iv)
            byte_range = self.info_frag[j][q - 1]['byterange']
            headers = {}
            if byte_range:
                headers['range'] = f"bytes={byte_range['start']}-{byte_range['end'] - 1}"

            #self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:[{nco}]{j}:{q}:{url}")                       
            
            while self.info_frag[j][q - 1]['n_retries'] < 5:
 
                try: 
                    
                    async with self.client[j].stream("GET", url, headers=headers) as res:
                    
                        #res = await self.client[j].get(url,headers=headers)
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}] : [worker{nco}]: stream{j} : frag{q} : {res.http_version} {res.status_code} {res.reason_phrase} : {filename.name}")
                        if res.status_code >= 400:                        
                            raise AsyncHLSDLErrorFatal(f"Frag:{str(q)} resp code:{str(res)}")
                        
                        with open(filename, mode='wb') as f:
                            num_bytes_downloaded = res.num_bytes_downloaded
                            async for chunk in res.aiter_bytes(chunk_size=1024):
                                if cipher: data = cipher.decrypt(chunk)
                                else: data = chunk 
                                f.write(data)
                                self.down_size += res.num_bytes_downloaded - num_bytes_downloaded
                                num_bytes_downloaded = res.num_bytes_downloaded
                        
                        self.n_dl_segments += 1
                        self.info_frag[j][q - 1]['downloaded'] = True
                        self.info_frag[j][q - 1]['error'].append(str(res))
                        break                                                         
                        

                except (AsyncHLSDLErrorFatal, httpx.ConnectError) as e:
                    self.info_frag[j][q - 1]['error'].append(str(e))
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:[{nco}]stream[{j}]:frag[{q}]:fatalError:{e}:{url}")
                    raise AsyncHLSDLErrorFatal(f"{e}")
                except (AsyncHLSDLError, httpx.HTTPError, httpx.CloseError, httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ProxyError, AttributeError, RuntimeError) as e:
                    #self.logger.debug(f"Exception ocurred: {e}", exc_info=True)
                    self.info_frag[j][q - 1]['error'].append(f"{str(e)}")
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:[{nco}]stream[{j}]:frag[{q}]:error:{e}:{url}")
                    self.info_frag[j][q - 1]['n_retries'] += 1
                    continue

            
            if self.info_frag[j][q - 1]['n_retries'] == 5:
                self.info_frag[j][q - 1]['error'].append("MaxLimitRetries")
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:[{nco}]{j}:(MaxLimitRetries)Frag: {q}")
                raise AsyncHLSDLErrorFatal(f"MaxLimitretries Frag: {q}")

        else:
            return 0

    async def fetch_async(self):        
        cont = 5
        while (len(self.fragsnotdl()) != 0):

            self.status = "downloading"
            try:
                
                async with AioPool(size=self.iworkers) as pool:

                    futures = [pool.spawn_n(self.fetch(i)) for i in range(self.iworkers)]
    
                    done_tasks, pending_tasks = await asyncio.wait(futures, return_when=asyncio.FIRST_EXCEPTION)    

                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Fuera del wait tasks, vamos a cancelar pending tasks")           
                    if pending_tasks:
                        try:
                            await pool.cancel(pending_tasks)
                            self.logger.debug(f"{self.webpage_url}: {len(pending_tasks)} tasks pending cancelled")
                        except Exception as e:
                            self.logger.debug(f"{self.webpage_url}: {e}")
                    
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
                            except (CancelledError, InvalidStateError, httpx.HTTPError, httpx.StreamError) as e:
                                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:rec excepciones:{str(e)}")

                    if fatal_errors: raise AsyncHLSDLErrorFatal("reset")
                    else: 
                        cont -= 1
                        if cont == 0:
                            if len(self.fragsnotdl()) != 0:  raise AsyncHLSDLErrorFatal("count to zero")
                            else: break
                   


            except AsyncHLSDLErrorFatal as e:
                self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Exception ocurred: {str(e)}")
                if cont == 0: 
                    self.status = "error"
                    raise

                if self.n_reset < 5:
                    cont = 5
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:RESET)")
                    await asyncio.wait_for(self.reset(), None)                    
                    for i in range(self.n_streams):
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:stream[{i}]:RESET:Pending frags {len(self.not_dl(i))}")
                else:
                    self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]:ERROR:Max_number_of_resets {str(e)}")
                    for i in range(self.n_streams):
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:stream[{i}]:E:Pending frags {len(self.not_dl(i))}")
                        #self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:stream[{i}]:{self.info_frag[i]}")
                    try:
                        await self.client[0].aclose()
                        if self.n_streams == 2:
                            await self.client[1].aclose()             
                    except Exception as e:
                        self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Exception ocurred when closing clients: {str(e)}") 
                    self.status = "error"
                    raise

        #vemos en disco si están todos los fragmentos
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Frags DL completed")
        try:            
            await self.client[0].aclose()
            if self.n_streams == 2:
                await self.client[1].aclose() 
        except Exception as e:
            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Exception ocurred when closing clients: {str(e)}")

        try:   
            self.ensamblfrags()            
        except AsyncHLSDLError as e:
            self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Exception ocurred when ensambling frags: {str(e)}") 
            self.status = "error"
            raise             

              
        if self.n_streams == 2:
            rc = await self.merge()
            if rc == 0 and self.filename.exists():
                self.filename_stream[0].unlink()
                self.filename_stream[1].unlink()
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Streams merged for: {self.filename}")
        
            else:
                self.status = "error"
                raise AsyncHLSDLErrorFatal(f"error merge, ffmpeg error: {rc}")

        self.status = "done"
        
        rmtree(str(self.base_download_path),ignore_errors=True)
        
        
        return (f"{self.webpage_url}: {self.status}")

    
    def ensamblfrags(self):        
        for j in range(self.n_streams):
        
            try:
                
                frag_files = natsorted(self.download_path[j].iterdir(), alg=ns.PATH)
                if len(frag_files) != len(self.info_frag[j]):
                    raise AsyncHLSDLError(f"Stream({j}):{self.filename_stream[j]}:Number of frag files < frags")
            
                self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{self.filename_stream[j]}")
                with open(self.filename_stream[j], mode='wb') as dest:
                    for f in frag_files:                    
                        with open(f, 'rb') as source:
                            dest.write(source.read())
            
            except Exception as e:
                if self.filename_stream[j].exists():
                    self.filename_stream[j].unlink()
                self.logger.warning(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Exception ocurred: {str(e)}")
                raise

        
     
    def fragsnotdl(self):
        res = []
        for stream in range(self.n_streams):
            for frag in self.info_frag[stream]:
                if frag['downloaded'] == False:
                    res.append(frag['frag'])
        return res


    def not_dl(self, j):        
        res = []
        for frag in self.info_frag[j]:
            if frag['downloaded'] == False:
                res.append(frag['frag'])
        
        return res
    
    async def read_stream(self, stream):
        msg = ""
        while (self.proc is not None and not self.proc.returncode):
            try:
                line = await stream.readline()
            except (asyncio.LimitOverrunError, ValueError):
                continue
            if line: 
                line = line.decode('utf-8').strip()
                msg = f"{msg}{line}\n"                                                            
            else:
                break
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{msg}")

    
    async def merge(self):        
        cmd = f"ffmpeg -y -loglevel repeat+info -i 'file:{str(self.filename_stream[0])}' -i \'file:{str(self.filename_stream[1])}' -c copy -map 0:v:0 -map 1:a:0 'file:{str(self.filename)}'"
        
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{cmd}")
        
        self.proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=1024 * 1024) 
        
        await asyncio.gather(self.read_stream(self.proc.stdout), self.read_stream(self.proc.stderr), self.proc.wait())
        
        return self.proc.returncode

    def videoexits(self):
        return self.filename.exists()

    def remove(self):        
        rmtree(str(self.base_download_path),ignore_errors=True)
        
   
    
    def print_hookup(self):
        if self.status == "done":
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: Completed [{naturalsize(self.filename.stat().st_size)}][{self.n_dl_segments} of {self.n_total_segments}]\n")
        elif self.status == "init":
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: Waiting to enter in the pool [{naturalsize(self.filesize)}][{self.n_dl_segments} of {self.n_total_segments}]\n")            
        elif self.status == "error":
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: ERROR {naturalsize(self.down_size)} [{naturalsize(self.filesize)}][{self.n_dl_segments} of {self.n_total_segments}]\n")
        else:            
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: Progress {naturalsize(self.down_size)} [{naturalsize(self.filesize)}][{self.n_dl_segments} of {self.n_total_segments}]\n")
            
        