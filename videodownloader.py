
import logging
import sys
import traceback
import asyncio
from pathlib import Path
import shutil
from asynchttpdownloader import (
    AsyncHTTPDownloader
)
from asynchlsdownloader import (
    AsyncHLSDownloader
)
from asyncdashdownloader import (
    AsyncDASHDownloader    
)
from common_utils import ( 
    naturalsize,
)
from concurrent.futures import (
    ThreadPoolExecutor  
)
from youtube_dl.utils import sanitize_filename, determine_protocol
from datetime import datetime
import hashlib
from shutil import rmtree, move
import functools


class VideoDownloader():
    
    def __init__(self, video_dict, ytdl, n_workers, dlpath=None):
        
        self.logger = logging.getLogger("video_DL")
        #self.alogger = AsyncLogger(self.logger)
        
        # self.proxies = "http://atgarcia:ID4KrSc6mo6aiy8@proxy.torguard.org:6060"
        # #self.proxies = "http://192.168.1.133:5555"
        
        #self.proxies = f"http://atgarcia:ID4KrSc6mo6aiy8@{get_ip_proxy()}:6060"
                
        self.info_dict = dict(video_dict)
        
        if not self.info_dict.get('id'):
            _video_id = str(int(hashlib.sha256(b"{video_dict.get('webpage_url')}").hexdigest(),16) % 10**8)
        else: _video_id = str(self.info_dict['id'])
        self.info_dict.update({'id': _video_id[:8] if len(_video_id) > 8 else _video_id})
        
        _date_file = datetime.now().strftime("%Y%m%d")
        _download_path = Path(Path.home(),"testing", _date_file, self.info_dict['id']) if not dlpath else Path(dlpath, self.info_dict['id'])
        
        self.info_dl = dict({
            
            'videoid': self.info_dict['id'],
            'n_workers': n_workers,
            'webpage_url': self.info_dict.get('webpage_url'),
            'title': self.info_dict.get('title'),
            'ytdl': ytdl,
            'date_file': _date_file,
            'download_path': _download_path,
            'filename': Path(_download_path.parent, str(self.info_dict['id']) + "_" + sanitize_filename(self.info_dict['title'], restricted=True)  + "." + self.info_dict.get('ext', 'mp4')),
        })        
        
               
        self.info_dl['download_path'].mkdir(parents=True, exist_ok=True)  
        
        downloaders = []
        if not (_requested_formats:=self.info_dict.get('requested_formats')):
            _new_info_dict = dict(self.info_dict)
            _new_info_dict.update({'filename': self.info_dl['filename'], 'download_path': self.info_dl['download_path']})
            downloaders.append(self._get_dl(_new_info_dict))
        else:
            for f in _requested_formats:
                _new_info_dict = dict(f)                
                _new_info_dict.update({'id': self.info_dl['videoid'], 'title': self.info_dl['title'], '_filename': self.info_dl['filename'], 'download_path': self.info_dl['download_path'], 'webpage_url': self.info_dl['webpage_url']})
                downloaders.append(self._get_dl(_new_info_dict))        
        
            
        self.info_dl.update({
            'downloaders': downloaders,
            'filesize': sum([dl.filesize for dl in downloaders]),
            'down_size': sum([dl.down_size for dl in downloaders]),
            'status': "init_manipulating" if ((len(res:=set([dl.status for dl in downloaders])) == 1) and (res.pop() == "manipulating")) else "init"             
        })
                
    
    def _get_dl(self, info):
        
        protocol = determine_protocol(info)
        if protocol in ('http', 'https'):
            dl = AsyncHTTPDownloader(info, self)
        elif protocol in ('m3u8', 'm3u8_native'):
            dl = AsyncHLSDownloader(info, self)
        elif protocol in ('http_dash_segments'):
            #dl = AsyncDASHDownloader(info, self) 
            raise NotImplementedError("dl dash not supported")           
            
        else:
            self.logger.error(f"[{info['id']}][{info['title']}]: protocol not supported")
            raise NotImplementedError("protocol not supported")
        
        return dl
    
    async def run_dl(self):
        
        self.lock = asyncio.Lock()
        
        self.info_dl['status'] = "downloading"
        tasks_run = [asyncio.create_task(dl.fetch_async()) for dl in self.info_dl['downloaders'] if dl.status != "manipulating"]
        done, _ = await asyncio.wait(tasks_run, return_when=asyncio.ALL_COMPLETED)
        
    
        if done:
            for d in done:
                try:                        
                    d.result()  
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())                
                    self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: result de dl.fetch_async\n{'!!'.join(lines)}")
                    
      
            
        self.info_dl['status'] = "init_manipulating" if ((len(res:=set([dl.status for dl in self.info_dl['downloaders']])) == 1) and (res.pop() == "manipulating")) else "error" 
        
    async def run_manip(self):
        
        try:
            self.lock = asyncio.Lock()
            
            self.info_dl['status'] = "creating"
            loop = asyncio.get_running_loop()
            ex = ThreadPoolExecutor(max_workers=len(self.info_dl['downloaders']))
            blocking_tasks = [loop.run_in_executor(ex, dl.ensamble_file) for dl in self.info_dl['downloaders']]
            #await asyncio.sleep(0)
            done, pending = await asyncio.wait(blocking_tasks, return_when=asyncio.ALL_COMPLETED)
            for t in done:
                
                try:
                    t.result()
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())                
                    self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: result de dl.ensamble_file\n{'!!'.join(lines)}")
        
            res = True
            for dl in self.info_dl['downloaders']:
                self.logger.info(f"{dl.filename} exists: [{dl.filename.exists()}] status: [{dl.status}]")
                res = res and (dl.filename.exists() and dl.status == "done")
            
            if res:    
                self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}] ensambled OK")            
        
                if len(self.info_dl['downloaders']) == 1:
                    
                    rc = -1
                    
                    if "ts" in self.info_dl['downloaders'][0].filename.suffix: #usamos ffmpeg para cambiar contenedor ts del DL de HLS de un sólo stream a mp4
                    
                        cmd = f"ffmpeg  -y -loglevel repeat+info -i 'file:{self.info_dl['downloaders'][0].filename}' -c copy 'file:{str(self.info_dl['filename'])}'"
                        rc = await self._postffmpeg(cmd)
                        
                    else:
                        
                        res = await asyncio.to_thread(shutil.move, self.info_dl['downloaders'][0].filename, self.info_dl['filename'])
                        if res == self.info_dl['filename']: rc = 0                   
          
                    
                    if rc == 0 and self.info_dl['filename'].exists():                        
                        self.info_dl['status'] = "done"
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error move file: {rc}")
                        
                else:
                    
                    cmd = f"ffmpeg -y -loglevel repeat+info -i 'file:{str(self.info_dl['downloaders'][0].filename)}' -i \'file:{str(self.info_dl['downloaders'][1].filename)}' -c copy -map 0:v:0 -map 1:a:0 'file:{str(self.info_dl['filename'])}'"
                            
                    rc = await self._postffmpeg(cmd)
                    if rc == 0 and self.info_dl['filename'].exists():
                        self.info_dl['status'] = "done"
                        for dl in self.info_dl['downloaders']:
                            dl.filename.unlink()
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:Streams merged for: {self.info_dl['filename']}")
                    
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error merge, ffmpeg error: {rc}")
                
                    
                if self.info_dl['status'] == "done":
                    await asyncio.to_thread(functools.partial(rmtree, self.info_dl['download_path'], ignore_errors=True))
                    
                 
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when manipulating\n{'!!'.join(lines)}")
            
        
    async def _postffmpeg(self, cmd):        
        
        
        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{cmd}")
        
        proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=1024 * 1024) 
        
        async def read_stream(stream):
            msg = ""
            while (proc is not None and not proc.returncode):
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
            
        await asyncio.gather(read_stream(proc.stdout), read_stream(proc.stderr), proc.wait())
        
        return proc.returncode
    
     
    
    def print_hookup(self):        
        
        msg = ""
        for dl in self.info_dl['downloaders']:
            msg += f"\t{dl.print_hookup()}"
        msg += "\n" 
        if self.info_dl['status'] == "done":
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: Completed [{naturalsize(self.info_dl['filename'].stat().st_size)}]\n")
        elif self.info_dl['status'] == "init":
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: Waiting to DL [{naturalsize(self.info_dl['filesize'])}]\n {msg}\n")  
        elif self.info_dl['status'] == "init_manipulating":
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: Waiting to create file [{naturalsize(self.info_dl['filesize'])}]\n")           
        elif self.info_dl['status'] == "error":
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: ERROR {naturalsize(self.info_dl['down_size'])} [{naturalsize(self.info_dl['filesize'])}]\n {msg}\n")
        elif self.info_dl['status'] == "downloading":            
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]: Downloading {naturalsize(self.info_dl['down_size'])} [{naturalsize(self.info_dl['filesize'])}]\n {msg}\n")
        elif self.info_dl['status'] == "creating": 
            if self.info_dl['filename'].exists(): _size = self.info_dl['filename'].stat().st_size
            else: _size = 0
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]:  Ensambling/Merging {naturalsize(_size)} [{naturalsize(self.info_dl['filesize'])}]\n {msg}\n")
        
        