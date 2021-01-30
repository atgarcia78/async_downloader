import asyncio
from asyncio.exceptions import (
    CancelledError,
    InvalidStateError
)
import httpx
import aiofile
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

from collections import deque
import logging

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
        self.proxies = ytdl.params.get('proxy', None)
        if self.proxies:
            self.proxies = f"http://{self.proxies}"
        self.verifycert = not self.ytdl.params.get('nocheckcertificate')
        self.video_url = self.info_dict.get('webpage_url')

        self.date_file = datetime.now().strftime("%Y%m%d")

        self.filename = Path(Path.home(),"testing", self.date_file, self.date_file + "_" + \
            str(self.info_dict['id']) + "_" + self.info_dict['title'] + "." + self.info_dict['ext'])

        if self.info_dict.get('requested_formats', None): #multi stream
            self.n_streams = 2
        else:
            self.n_streams = 1

        self.n_reset = 0
  
        self.iworkers = workers

        self.prep_init()            
                
    
    def prep_init(self):

           
        self.frag_queue = []
        self.headers = []
        self.stream_url = []        
        self.m3u8_obj = []
        self.info_frag = []
        self.key_cache = dict()
        self.client = []
        #self.proxies = f"fttp://atgarcia:ID4KrSc6mo6aiy8@{get_ip_proxy()}:6060"
        
        self.download_path = []       
        self.filename_stream = []

        self.base_download_path = Path(Path.home(),"testing", self.date_file, str(self.info_dict['id']))
        

        #timeout = httpx.Timeout(20, connect=60)
        timeout = None

        if self.n_streams == 2:

            self.headers.append(self.info_dict['requested_formats'][0]['http_headers'])
            self.headers.append(self.info_dict['requested_formats'][1]['http_headers'])
            self.stream_url.append(self.info_dict['requested_formats'][0]['url'])
            self.stream_url.append(self.info_dict['requested_formats'][1]['url'])
           
            #self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, proxies=self.proxies))
            #self.client.append(httpx.AsyncClient(headers=self.headers[1], http2=False, proxies=self.proxies))
            self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))

            self.client.append(httpx.AsyncClient(headers=self.headers[1], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))

            self.download_path.append(Path(self.base_download_path, "video"))
            self.download_path.append(Path(self.base_download_path, "audio"))
            self.download_path[0].mkdir(parents=True, exist_ok=True)
            self.download_path[1].mkdir(parents=True, exist_ok=True)

            self.filename_stream.append(Path(self.download_path[0].parent, self.date_file + "_" + \
                str(self.info_dict['id']) + "_" + self.info_dict['title'] + "_video" + "." + \
                self.info_dict['requested_formats'][0]['ext']))
            self.filename_stream.append(Path(self.download_path[1].parent, self.date_file + "_" + \
                str(self.info_dict['id']) + "_" + self.info_dict['title'] + "_audio" + "." + \
                self.info_dict['requested_formats'][1]['ext']))
            
            self.frag_queue.append(deque())
            self.frag_queue.append(deque())

            self.info_frag.append(list())
            self.info_frag.append(list())

        else: #single stream
            self.headers.append(self.info_dict['http_headers'])
            self.stream_url.append(self.info_dict['url'])


            self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))
            #self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, proxies=self.proxies))
            self.download_path.append(self.base_download_path)
            self.download_path[0].mkdir(parents=True, exist_ok=True)
            self.filename_stream.append(self.filename)
            self.frag_queue.append(deque())
            self.info_frag.append(list())
            

        for j in range(self.n_streams):

            try:
                #self.m3u8_obj.append(m3u8.loads(httpx.get(self.stream_url[j], headers=self.headers[j]).text,uri=self.stream_url[j]))
                self.logger.debug("HEADERS")
                self.logger.debug(self.headers[j])
                res = httpx.get(self.stream_url[j],headers=self.headers[j])
                self.logger.debug(res.request)
                self.logger.debug(res.request.headers)
                m3u8_file = res.text
                #m3u8_file = self.ytdl.urlopen(self.stream_url[j]).read()
                self.logger.debug("M3U8 file")
                self.logger.debug(m3u8_file)
                self.m3u8_obj.append(m3u8.loads(m3u8_file,uri=self.stream_url[j]))
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
                    file_path =  Path(self.download_path[j], f"{Path(segment.uri).name}_part_{part}")
                    splitted_byte_range = segment.byterange.split('@')
                    sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                    byte_range = {
                        'start': sub_range_start,
                        'end': sub_range_start + int(splitted_byte_range[0]),
                    }

                else:
                    file_path =  Path(self.download_path[j],Path(segment.uri).name)
                    byte_range = {}


                size = 0
                try:
                    size = file_path.stat().st_size
                except Exception as e:
                    pass

                if size == 0:
                    self.info_frag[j].append({"frag" : i+1, "url" : segment.absolute_uri, "key": segment.key, "file" : file_path, "byterange" : byte_range, "downloaded" : False, "n_retries": 0, "error" : []})
                    self.frag_queue[j].append(i+1)
                    if segment.key is not None and segment.key.method == 'AES-128':
                        if segment.key.absolute_uri not in self.key_cache:
                            self.key_cache[segment.key.absolute_uri] = httpx.get(segment.key.absolute_uri, headers=self.headers[j]).content
                            self.logger.debug(f"{self.info_dict['title']}:{self.key_cache[segment.key.absolute_uri]}")
                            self.logger.debug(f"{self.info_dict['title']}:{segment.key.iv}")

                else:
                    self.info_frag[j].append({"frag" : i+1, "url" : segment.absolute_uri, "file" : file_path, "byterange" : byte_range, "downloaded" : True, "n_retries": 0, "error" : "AlreadyDL"})
 

            self.logger.debug(f"{self.info_dict['title']}:Frags for stream {str(j)}")
            self.logger.debug(self.info_frag[j])
            #self.logger.debug(self.not_dl(j))
        
   
        
    async def reset(self):

        self.logger.debug(f"{self.info_dict['title']}:RESET:close clients")
        
        try:
            await self.client[0].aclose()
            if self.n_streams == 2:
                await self.client[1].aclose()
        except Exception as e:
            self.logger.debug(f"{self.info_dict['title']}:Exception ocurred when closing client: {str(e)}", exc_info=True)    
 
        self.logger.debug(f"{self.info_dict['title']}:Get info video dict again")

        #ya tenemos toda la info, sólo queremos refrescar la info de los fragmentos
        
        self.logger.debug(f"{self.info_dict['title']}:RESET:get video dict: {self.video_url}")
        
        try:
            
            info = self.ytdl.process_ie_result({'_type':'url_transparent', 'url': self.video_url, 'title': self.info_dict['title'], 'ie_key': self.info_dict['extractor_key']}, download=False)
            if info.get('playlist'):
                info_reset = info['entries'][self.info_dict['playlist_index']]
            else:
                info_reset = info
            
            self.logger.debug(f"{self.info_dict['title']}:{info_reset}")
            
        except Exception as e:
            raise AsyncHLSDLErrorFatal("no hay descriptor")

        if not info_reset:
            raise AsyncHLSDLErrorFatal("no hay descriptor")           

        try: 
            self.prep_reset(info_reset)
            self.n_reset += 1
        except Exception as e:
            self.logger.debug(f"{self.info_dict['title']}:Exception occurred when reset: {str(e)}")

    
    def prep_reset(self, info_reset):
       
        self.frag_queue = []
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
           
            #self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, proxies=self.proxies))
            #self.client.append(httpx.AsyncClient(headers=self.headers[1], http2=False, proxies=self.proxies))
            self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))

            self.client.append(httpx.AsyncClient(headers=self.headers[1], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))


            self.frag_queue.append(deque())
            self.frag_queue.append(deque())

        else: #single stream
            self.headers.append(info_reset['http_headers'])
            self.stream_url.append(info_reset['url'])


            self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, timeout=timeout, verify=self.verifycert, proxies=self.proxies))
            #self.client.append(httpx.AsyncClient(headers=self.headers[0], http2=False, proxies=self.proxies))

            self.frag_queue.append(deque())
        


        for j in range(self.n_streams):

            try:
                #self.m3u8_obj.append(m3u8.loads(httpx.get(self.stream_url[j], headers=self.headers[j]).text,uri=self.stream_url[j]))
                m3u8_file = httpx.get(self.stream_url[j],headers=self.headers[j]).text
                #self.logger.debug(m3u8_file)
                self.m3u8_obj.append(m3u8.loads(m3u8_file,uri=self.stream_url[j]))
            except Exception as e:
                raise AsyncHLSDLErrorFatal("no hay descriptor")

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
                    file_path =  Path(self.download_path[j], f"{Path(segment.uri).name}_part_{part}")
                    splitted_byte_range = segment.byterange.split('@')
                    sub_range_start = int(splitted_byte_range[1]) if len(splitted_byte_range) == 2 else byte_range['end']
                    byte_range = {
                        'start': sub_range_start,
                        'end': sub_range_start + int(splitted_byte_range[0]),
                    }

                else:
                    file_path =  Path(self.download_path[j],Path(segment.uri).name)
                    byte_range = {}



                if not self.info_frag[j][i]['downloaded']:
                    self.frag_queue[j].append(i+1)                        
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
 
    async def fetch(self, nco, j):
        
        while self.frag_queue[j]:

            q = self.frag_queue[j].popleft()
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

            #self.logger.debug(f"{self.info_dict['title']}:[{nco}]{j}:{q}:{url}")                       
            
            while self.info_frag[j][q - 1]['n_retries'] < 5:
 
                try: 
                    res = await self.client[j].get(url,headers=headers)
                    self.logger.debug(f"[{self.info_dict['title']}] : [worker{nco}]: stream{j} : frag{q} : {res.http_version} {res.status_code} {res.reason_phrase} : {filename.name}")
                    if res.status_code >= 400:                        
                        raise AsyncHLSDLErrorFatal(f"Frag:{str(q)} resp code:{str(res)}")
                    if res.content:
                        if cipher:
                            data = cipher.decrypt(res.content)
                        else:
                            data = res.content 
                        async with aiofile.async_open(filename, mode='wb') as f:
                            await f.write(data)

                        self.info_frag[j][q - 1]['downloaded'] = True
                        self.info_frag[j][q - 1]['error'].append(str(res))
                        break                                   
                        

                except (AsyncHLSDLErrorFatal, httpx.ConnectError) as e:
                    self.info_frag[j][q - 1]['error'].append(str(e))
                    self.logger.debug(f"{self.info_dict['title']}:[{nco}]stream[{j}]:frag[{q}]:fatalError:{e}:{url}", exc_info=True)
                    raise AsyncHLSDLErrorFatal(f"{e}")
                except (AsyncHLSDLError, httpx.HTTPError, httpx.CloseError, httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ProxyError, AttributeError, RuntimeError) as e:
                    #self.logger.debug(f"Exception ocurred: {e}", exc_info=True)
                    self.info_frag[j][q - 1]['error'].append(f"{str(e)}")
                    self.logger.debug(f"{self.info_dict['title']}:[{nco}]stream[{j}]:frag[{q}]:error:{e}:{url}", exc_info=True)
                    self.info_frag[j][q - 1]['n_retries'] += 1
                    continue

            
            if self.info_frag[j][q - 1]['n_retries'] == 5:
                self.info_frag[j][q - 1]['error'].append("MaxLimitRetries")
                self.logger.debug(f"{self.info_dict['title']}:[{nco}]{j}:(MaxLimitRetries)Frag: {q}")
                raise AsyncHLSDLErrorFatal(f"MaxLimitretries Frag: {q}")

        else:
            return 0

    async def fetch_async(self):
        
        while True:

            try:

                self.tasks = []    
                for j in range(self.n_streams):
                    for i in range(self.iworkers):
                        task = asyncio.create_task(self.fetch(i,j))
                        self.tasks.append(task)   

                self.logger.debug(f"{self.info_dict['title']}:WAITINGFORTASKS")
                done_tasks, pending_tasks = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)    

                self.logger.debug(f"{self.info_dict['title']}:Fuera del wait tasks, vamos a cancelar pending tasks")           
                if pending_tasks:
                    for pending in pending_tasks:
                            try:
                                pending.cancel()
                                self.logger.debug(f"{self.info_dict['title']}:task cancelled")
                            except (CancelledError, InvalidStateError) as e:
                                self.logger.debug(f"{self.info_dict['title']}:{e}")
                    
                    self.logger.debug(f"{self.info_dict['title']}:esperar a las pending tasks")
                    group = await asyncio.gather(*pending_tasks, return_exceptions=True)
                    self.logger.debug(f"{self.info_dict['title']}:fin de esperar a las pending tasks")
                               
                if self.n_streams == 1:
                    if not self.not_dl(0):
                        break
                else:
                    if not self.not_dl(0) and not self.not_dl(1):
                        break
        
                #recorro done para lanzar las excepciones, si hay un Fatal no lo caapturo en el try except
                #del bucle para q llegue al otro except y lance el reset
                if done_tasks:
                    for done in done_tasks:
                        try:
                            done.result()
                        except AsyncHLSDLErrorFatal as e:
                            raise
                        except (CancelledError, InvalidStateError, httpx.HTTPError, httpx.StreamError) as e:
                            self.logger.debug(f"{self.info_dict['title']}:rec excepciones:{str(e)}")


            except AsyncHLSDLErrorFatal as e:
                self.logger.warning(f"{self.info_dict['title']}:Exception ocurred: {str(e)}")
                if self.n_reset < 5:
                    self.logger.debug(f"{self.info_dict['title']}:RESET)")
                    await asyncio.wait_for(self.reset(), None)                    
                    for i in range(self.n_streams):
                        self.logger.debug(f"{self.info_dict['title']}:stream[{i}]:RESET:Pending frags {len(self.not_dl(i))}")
                else:
                    self.logger.warning(f"{self.info_dict['title']}:ERROR:Max_number_of_resets {str(e)}")
                    for i in range(self.n_streams):
                        self.logger.debug(f"{self.info_dict['title']}:stream[{i}]:E:Pending frags {len(self.not_dl(i))}")
                        #self.logger.debug(f"{self.info_dict['title']}:stream[{i}]:{self.info_frag[i]}")
                    try:
                        await self.client[0].aclose()
                        if self.n_streams == 2:
                            await self.client[1].aclose()             
                    except AsyncHLSDLError as e:
                        self.logger.warning(f"{self.info_dict['title']}:Exception ocurred when closing clients: {str(e)}") 
                        raise

        #vemos en disco si están todos los fragmentos
        self.logger.debug(f"{self.info_dict['title']}:Frags DL completed")
        try:
            self.ensamblfrags()
            await self.client[0].aclose()
            if self.n_streams == 2:
                await self.client[1].aclose()             
        except AsyncHLSDLError as e:
            self.logger.warning(f"{self.info_dict['title']}:Exception ocurred when ensambling frags: {str(e)}") 
            raise             

              
        if self.n_streams == 2:
            rc = await self.merge()
            if self.filename.exists():
                self.filename_stream[0].unlink()
                self.filename_stream[1].unlink()
                self.logger.debug(f"{self.info_dict['title']}:Streams merged for: {self.filename}")
        
            else:
                raise AsyncHLSDLErrorFatal(f"error merge, ffmpeg error: {rc}")

        rmtree(str(self.base_download_path),ignore_errors=True)
            

        return 0

    
    def ensamblfrags(self):
        
        
        for j in range(self.n_streams):

        
            try:
                
                frag_files = natsorted(self.download_path[j].iterdir(), alg=ns.PATH)
                if len(frag_files) != len(self.info_frag[j]):
                    raise AsyncHLSDLError(f"Stream({j}):{self.filename_stream[j]}:Number of frag files < frags")
            
                self.logger.debug(f"{self.info_dict['title']}:{self.filename_stream[j]}")
                with open(self.filename_stream[j], mode='wb') as dest:
                    for f in frag_files:                    
                        with open(f, 'rb') as source:
                            dest.write(source.read())
            
            except Exception as e:
                if self.filename_stream[j].exists():
                    self.filename_stream[j].unlink()
                self.logger.warning(f"{self.info_dict['title']}:Exception ocurred: {str(e)}")
                raise

        
     


    def not_dl(self, j):
        
        res = []
        for frag in self.info_frag[j]:
            if frag['downloaded'] == False:
                res.append(frag['frag'])
        
        return res
        

    
    async def merge(self):
        
        cmd = f"ffmpeg -y -loglevel repeat+info -i 'file:{str(self.filename_stream[0])}' -i \'file:{str(self.filename_stream[1])}' -c copy -map 0:v:0 -map 1:a:0 'file:{str(self.filename)}'"
        
        self.logger.debug(f"{self.info_dict['title']}:{cmd}")
        
        task = await asyncio.create_subprocess_shell(cmd)

        await task.wait()
        
        # self.logger(f"ffmpeg RC={task.returncode}")

        # return task.returncode

    def videoexits(self):
        return self.filename.exists()

    def remove(self):
        
        rmtree(str(self.base_download_path),ignore_errors=True)