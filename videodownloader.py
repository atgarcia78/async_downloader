
import logging
import sys
import traceback
import asyncio
from pathlib import Path
from aiopath import AsyncPath
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
from asyncaria2cdownloader import (
     AsyncARIA2CDownloader    
)

from utils import ( 
    naturalsize,
)
from concurrent.futures import (
    ThreadPoolExecutor  
)
from yt_dlp.utils import sanitize_filename, determine_protocol
from datetime import datetime
import hashlib
from shutil import rmtree, move
import functools
import httpx
from pycaption import detect_format, SRTWriter
import subprocess
import copy

class VideoDownloader():
    
    def __init__(self, video_dict, ytdl, n_workers, dlpath=None, aria2c=None):
        
        self.logger = logging.getLogger("video_DL")
        #self.alogger = AsyncLogger(self.logger)
        
        # self.proxies = "http://atgarcia:ID4KrSc6mo6aiy8@proxy.torguard.org:6060"
        # #self.proxies = "http://192.168.1.133:5555"
        
        #self.proxies = f"http://atgarcia:ID4KrSc6mo6aiy8@{get_ip_proxy()}:6060"
                
        self.info_dict = copy.deepcopy(video_dict)
    
        #_video_id = str(self.info_dict['id'])
        #self.info_dict.update({'id': _video_id[:10] if len(_video_id) > 10 else _video_id})
        
        _date_file = datetime.now().strftime("%Y%m%d")
        _download_path = Path(Path.home(),"testing", _date_file, self.info_dict['id']) if not dlpath else Path(dlpath, self.info_dict['id'])
        
               
        self.info_dl = {
            
            'id': self.info_dict['id'],
            'n_workers': n_workers,
            'aria2c': aria2c,
            'webpage_url': self.info_dict.get('webpage_url'),
            'title': self.info_dict.get('title'),
            'ytdl': ytdl,
            'date_file': _date_file,
            'download_path': _download_path,
            'filename': Path(_download_path.parent, str(self.info_dict['id']) + "_" + sanitize_filename(self.info_dict['title'], restricted=True)  + "." + self.info_dict.get('ext', 'mp4')),
        } 
        
       
        
               
        self.info_dl['download_path'].mkdir(parents=True, exist_ok=True)  
        
        downloaders = []
        if not (_requested_formats:=self.info_dict.get('requested_formats')):
            _new_info_dict = copy.deepcopy(self.info_dict)
            _new_info_dict.update({'filename': self.info_dl['filename'], 'download_path': self.info_dl['download_path']})
            downloaders.append(self._get_dl(_new_info_dict))
        else:
            for f in _requested_formats:
                _new_info_dict = copy.deepcopy(f)                
                _new_info_dict.update({'id': self.info_dl['id'], 'title': self.info_dl['title'], '_filename': self.info_dl['filename'], 'download_path': self.info_dl['download_path'], 'webpage_url': self.info_dl['webpage_url']})
                downloaders.append(self._get_dl(_new_info_dict))        
        
               
        
        
        res = sorted(list(set([dl.status for dl in downloaders])))    
        self.info_dl.update({
            'downloaders': downloaders,
            'requested_subtitles': copy.deepcopy(_req_sub) if (_req_sub:=self.info_dict.get('requested_subtitles')) else {},
            'filesize': sum([dl.filesize for dl in downloaders if dl.filesize]),
            'down_size': sum([dl.down_size for dl in downloaders]),
            'status': "init_manipulating" if (res == ["init_manipulating"] or res == ["done"] or res == ["done", "init_manipulating"]) else "init"             
        })
        
        #print(self.info_dl)

    def _get_dl(self, info):
        
        protocol = determine_protocol(info)
        if protocol in ('http', 'https'):
            if not self.info_dl['aria2c']: dl = AsyncHTTPDownloader(info, self)
            else: dl = AsyncARIA2CDownloader(info, self)           
        elif protocol in ('m3u8', 'm3u8_native'):
            dl = AsyncHLSDownloader(info, self)            
        elif protocol in ('http_dash_segments', 'dash'):
            dl = AsyncDASHDownloader(info, self)             
            #raise NotImplementedError("dl dash not supported")           
            
        else:
            self.logger.error(f"[{info['id']}][{info['title']}]: protocol not supported")
            raise NotImplementedError("protocol not supported")
        
        return dl
    
    async def run_dl(self):
        
        self.lock = asyncio.Lock()
        
        self.info_dl['status'] = "downloading"
        tasks_run = [asyncio.create_task(dl.fetch_async()) for dl in self.info_dl['downloaders'] if dl.status not in ("init_manipulating", "done")]
        done, _ = await asyncio.wait(tasks_run, return_when=asyncio.ALL_COMPLETED)
        
    
        if done:
            for d in done:
                try:                        
                    d.result()  
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())                
                    self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] error ftch_async: {repr(e)}\n{'!!'.join(lines)}")
                    
      
        res = sorted(list(set([dl.status for dl in self.info_dl['downloaders']]))) 
            
        self.info_dl['status'] = "init_manipulating" if (res == ["init_manipulating"] or res == ["done"] or res == ["done", "init_manipulating"]) else "error"
        
    def _get_subs_files(self):
     
        for key, value in self.info_dl['requested_subtitles'].items():
            try:
                res = httpx.get(value['url'])
                reader = detect_format(res.text)
                _srt = SRTWriter().write(reader().read(res.text))
                _file_subs = Path(self.info_dl['filename'].parent, f"{self.info_dl['filename'].stem}.{key}.srt")
                value['file'] = _file_subs
                with open(_file_subs, "w") as f:
                    f.write(_srt)
                
                self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: subs file for [{key}] downloaded and converted to srt format")
                    
            except Exception as e:
                lines = traceback.format_exception(*sys.exc_info())                
                self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when downloading subs file\n{'!!'.join(lines)}")
           
   
    @staticmethod
    def _syncpostffmpeg(cmd):
        
        res = subprocess.run(cmd.split(' '), encoding='utf-8', capture_output=True)
        return res
    
    @staticmethod
    def embed_subs(_file_subs_en, _filename, _logger, _menslogger):
        
        
        logger = _logger if _logger else logging.getLogger('embed_subs')
        mens = _menslogger if _menslogger else ""
         
        
        if Path(_file_subs_en).exists():
            
            _temp = Path(Path(_filename).parent, f"_temp_{_filename.name}")
            cmd = f"ffmpeg -y -loglevel repeat+info -i file:{_filename} -i file:{_file_subs_en} -c copy -map 0 -dn -map -0:s -map -0:d -c:s mov_text -map 1:0 -metadata:s:s:0 language=eng file:{_temp}" 
            
            res = VideoDownloader._syncpostffmpeg(cmd)
            logger.debug(f"{mens} ffmpeg rc[{res.returncode}]\n{res.stdout}\n{res.stderr}")
                                
            if (rc:=res.returncode) == 0:
                if _temp.exists():
                
                    _filename.unlink
                    res2 = shutil.move(_temp, _filename)
                    if (res2 != _filename) or not _filename.exists():
                        rc = 1                                    
                
            if rc == 1: logger.error(f"{mens}: error when embedding subs with ffmpeg")
            else: logger.info(f"{mens}: subs embedded OK")
            
            return rc
            
        else:
            logger.error(f"{mens} couldnt find file with subs")
            return 1    
    
    async def run_manip(self):
        
        try:
            self.lock = asyncio.Lock()
            
            self.info_dl['status'] = "manipulating"
            for dl in self.info_dl['downloaders']: 
                if dl.status == 'init_manipulating':
                    dl.status = 'manipulating'
                   
            
            loop = asyncio.get_running_loop()
            ex = ThreadPoolExecutor(max_workers=len(self.info_dl['downloaders']))
            blocking_tasks = [loop.run_in_executor(ex, dl.ensamble_file) for dl in self.info_dl['downloaders'] if (dl._type != 'aria2c' and dl.status == 'manipulating')]
            if self.info_dl.get('requested_subtitles'):
                blocking_tasks += [loop.run_in_executor(ex, self._get_subs_files)]
            await asyncio.sleep(0)
            if blocking_tasks:
                done, pending = await asyncio.wait(blocking_tasks, return_when=asyncio.ALL_COMPLETED)
            
                for t in done:
                    
                    try:
                        t.result()
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())                
                        self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_manip] result de dl.ensamble_file: {repr(e)}\n{'!!'.join(lines)}")
            
            res = True
            for dl in self.info_dl['downloaders']:
                res = res and (_exists:= await asyncio.to_thread(dl.filename.exists)) and dl.status == "done"
                self.logger.info(f"{dl.filename} exists: [{_exists}] status: [{dl.status}]")
                if not res: break
                
            
            if res:    
                self.logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}] ensambled OK")            
        
                if len(self.info_dl['downloaders']) == 1:
                    
                    rc = -1
                    
                    if "ts" in self.info_dl['downloaders'][0].filename.suffix: #usamos ffmpeg para cambiar contenedor ts del DL de HLS de un sólo stream a mp4
                    
                        cmd = f"ffmpeg -y -loglevel repeat+info -i file:{str(self.info_dl['downloaders'][0].filename)} -c copy file:{str(self.info_dl['filename'])}"
                        
                        res = await asyncio.to_thread(self._syncpostffmpeg, cmd)
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {cmd}\n[rc] {res.returncode}\n[stdout]\n{res.stdout}\n[stderr]{res.stderr}")
                        rc = res.returncode
                        
                    else:                        
               
                        rc = -1
                        try:
                            
                            res = await asyncio.to_thread(shutil.move, self.info_dl['downloaders'][0].filename, self.info_dl['filename'])
                            if (res == self.info_dl['filename']): rc = 0
                            
                            
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())                
                            self.logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when manipulating\n{'!!'.join(lines)}")
                            
                    if rc == 0 and (await asyncio.to_thread(self.info_dl['filename'].exists)):
                    
                        
                        if (_file_subs_en:=self.info_dl.get('requested_subtitles', {}).get('en', {}).get('file')):
                            
                            rc = await asyncio.to_thread(self.embed_subs, _file_subs_en, self.info_dl['filename'], self.logger, f"[{self.info_dict['id']}][{self.info_dict['title']}]")
                                                                 
                        self.info_dl['status'] = "done"
                        
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error move file: {rc}")
                        
                else:
                    
                    cmd = f"ffmpeg -y -loglevel repeat+info -i file:{str(self.info_dl['downloaders'][0].filename)} -i file:{str(self.info_dl['downloaders'][1].filename)} -c copy -map 0:v:0 -map 1:a:0 file:{str(self.info_dl['filename'])}"
                    
                    rc = -1
                    
                    res = await asyncio.to_thread(self._syncpostffmpeg, cmd)
                    self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: ffmpeg rc[{res.returncode}]\n{res.stdout}")
                    rc = res.returncode
                    
                    if rc == 0 and (await asyncio.to_thread(self.info_dl['filename'].exists)):
                        
                        if (_file_subs_en:=self.info_dl['requested_subtitles'].get('en', {}).get('file')):
                            
                            rc = await asyncio.to_thread(self.embed_subs, _file_subs_en, self.info_dl['filename'], self.logger,f"[{self.info_dict['id']}][{self.info_dict['title']}]")
                                                
                        self.info_dl['status'] = "done"          
                        for dl in self.info_dl['downloaders']:
                            
                            await asyncio.to_thread(dl.filename.unlink)
                            
                        self.logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: Streams merged for: {self.info_dl['filename']}")
                    
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error merge, ffmpeg error: {rc}")
                
                    
                if self.info_dl['status'] == "done":
                    await asyncio.to_thread(functools.partial(rmtree, self.info_dl['download_path'], ignore_errors=True))
                    
            
            else: self.info_dl['status'] = "error"
                               
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
        elif self.info_dl['status'] == "manipulating": 
            if self.info_dl['filename'].exists(): _size = self.info_dl['filename'].stat().st_size
            else: _size = 0
            return (f"[{self.info_dict['id']}][{self.info_dict['title']}]:  Ensambling/Merging {naturalsize(_size)} [{naturalsize(self.info_dl['filesize'])}]\n {msg}\n")
        
        