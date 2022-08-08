
import asyncio
import copy
import logging
import shutil
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path
from shutil import rmtree

import httpx
#from pycaption import (DFXPReader, SAMIReader, SCCReader, SRTReader,
#                       WebVTTReader, SRTWriter, detect_format)
from yt_dlp.utils import determine_protocol, sanitize_filename, try_get

from asyncaria2cdownloader import AsyncARIA2CDownloader
from asyncdashdownloader import AsyncDASHDownloader
from asynchlsdownloader import AsyncHLSDownloader
from asynchttpdownloader import AsyncHTTPDownloader
from utils import async_ex_in_executor, naturalsize, try_get

# SUPPORTED_EXT = {
#     DFXPReader: 'ttml', WebVTTReader: 'vtt', SAMIReader: 'sami', SRTReader: 'srt', SCCReader: 'scc'
    
# }

import os
from concurrent.futures import ThreadPoolExecutor




logger = logging.getLogger("video_DL")
class VideoDownloader():
    
    def __init__(self, video_dict, ytdl, args): 
        
        try:
        
            self.args = args
            self.info_dict = copy.deepcopy(video_dict) 
            
            _date_file = datetime.now().strftime("%Y%m%d")
            _download_path = Path(Path.home(), "testing", _date_file, self.info_dict['id']) if not self.args.path else Path(self.args.path, self.info_dict['id'])
            
                
            self.info_dl = {
                
                'id': self.info_dict['id'],
                'n_workers': self.args.parts,
                'rpcport': self.args.rpcport, #será None si no hemos querido usar aria2c si es DL HTTP
                'auto_pasres': False,
                'webpage_url': self.info_dict.get('webpage_url'),
                'title': self.info_dict.get('title'),
                'ytdl': ytdl,
                'date_file': _date_file,
                'download_path': _download_path,
                'filename': Path(_download_path.parent, str(self.info_dict['id']) + "_" + sanitize_filename(self.info_dict['title'], restricted=True)  + "." + self.info_dict.get('ext', 'mp4')),
                'backup_http': self.args.use_http_failover,
            } 
                
            self.info_dl['download_path'].mkdir(parents=True, exist_ok=True)  
            
            downloaders = []
            if not (_requested_formats:=self.info_dict.get('requested_formats')):
                _new_info_dict = copy.deepcopy(self.info_dict)
                _new_info_dict.update({'filename': self.info_dl['filename'], 'download_path': self.info_dl['download_path']})
                downloaders.append(self._get_dl(_new_info_dict))
            else:
                #logger.info(_requested_formats)
                for f in _requested_formats:
                    #logger.info(f)
                    _new_info_dict = copy.deepcopy(f)                
                    _new_info_dict.update({'id': self.info_dl['id'], 'title': self.info_dl['title'], '_filename': self.info_dl['filename'], 'download_path': self.info_dl['download_path'], 'webpage_url': self.info_dl['webpage_url'], 'extractor_key': self.info_dict.get('extractor_key')})
                    #logger.info(_new_info_dict)
                    dl = self._get_dl(_new_info_dict)
                    #logger.info(dl)
                    downloaders.append(dl)        

            res = sorted(list(set([dl.status for dl in downloaders])))
            if (res == ["init_manipulating"] or res == ["done"] or res == ["done", "init_manipulating"]):
                _status = "init_manipulating"
            elif "error" in res:
                _status = "error"
            else:
                _status = "init"
                
            self.info_dl.update({
                'downloaders': downloaders,
                'requested_subtitles': copy.deepcopy(_req_sub) if (_req_sub:=self.info_dict.get('requested_subtitles')) else {},
                'filesize': sum([dl.filesize for dl in downloaders if dl.filesize]),
                'down_size': sum([dl.down_size for dl in downloaders]),
                'status': _status,
                'error_message': ""             
            })
            
            self.pause_event = None
            self.resume_event = None
            self.stop_event = None
            self.lock = None
            
            self.ex_videodl = ThreadPoolExecutor(thread_name_prefix="ex_videodl")
            
            
        except Exception as e:            
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}] DL constructor failed")
            self.info_dl['status'] = "error"
        finally:
            if self.info_dl['status'] == "error":
                rmtree(self.info_dl['download_path'], ignore_errors=True)
                
            

    def _get_dl(self, info):
        
        protocol = determine_protocol(info)
                    
        if protocol in ('http', 'https'):
            if self.info_dl['rpcport']: 
                
                try:
                    dl = None
                    dl = AsyncARIA2CDownloader(self.info_dl['rpcport'], info, self)
                    logger.info(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type ARIA2C")
                    if dl.auto_pasres: self.info_dl.update({'auto_pasres': True})
                except Exception as e:
                    if self.info_dl['backup_http']:
                        logger.warning(f"[{info['id']}][{info['title']}][{info['format_id']}][{info.get('extractor')}]: aria2c init failed, swap to HTTP DL")
                        #if dl and dl.auto_pasres: self.info_dl.update({'auto_pasres': True})
                        dl = AsyncHTTPDownloader(info, self)
                        logger.info(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type HTTP")
                        if dl.auto_pasres: self.info_dl.update({'auto_pasres': True}) 
                    else: raise
            else: 
                dl = AsyncHTTPDownloader(info, self)
                logger.info(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type HTTP")                   
        elif protocol in ('m3u8', 'm3u8_native'):
            dl = AsyncHLSDownloader(info, self)
            logger.info(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type HLS")
                        
        elif protocol in ('http_dash_segments', 'dash'):
            dl = AsyncDASHDownloader(info, self)
            logger.info(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type DASH")
        else:
            logger.error(f"[{info['id']}][{info['title']}][{info['format_id']}]: protocol not supported")
            raise NotImplementedError("protocol not supported")
        
                        
        return dl

    def reset(self):
        for dl in self.info_dl['downloaders']:
            if 'hls' in str(type(dl)).lower():
                if dl.status == "downloading":
                    if dl.reset_event: dl.reset_event.set()
                
                logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: event reset")
    
    def change_numvidworkers(self, n):
        for dl in self.info_dl['downloaders']:
            if 'hls' in str(type(dl)).lower():
                
                dl.n_workers = n
                if dl.status == "downloading":
                    if dl.reset_event: dl.reset_event.set()
                logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: workers set to {n}")        
    
    def stop(self):
        if self.stop_event:
            self.stop_event.set()
            logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: event stop")
        
    def pause(self):
        if self.pause_event:
            self.pause_event.set()
            #logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: event pause")
        
    def resume(self):
        if self.resume_event:
            if self.pause_event.is_set(): 
                self.resume_event.set()
                #logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: event resume")
    async def run_dl(self):

        self.info_dl['status'] = "downloading"
        self.pause_event = asyncio.Event()
        self.resume_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.reset_event = asyncio.Event()
        self.lock = asyncio.Lock()
        
        try:
            
            tasks_run = [asyncio.create_task(dl.fetch_async()) for dl in self.info_dl['downloaders'] if dl.status not in ("init_manipulating", "done")]
            done, _ = await asyncio.wait(tasks_run, return_when=asyncio.ALL_COMPLETED)
            
        
            if done:
                for d in done:
                    try:                        
                        d.result()  
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())                
                        logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] error ftch_async: {repr(e)}\n{'!!'.join(lines)}")
                        
            if self.stop_event.is_set():
                self.info_dl['status'] = "stop"
            
            else:
                
                res = sorted(list(set([dl.status for dl in self.info_dl['downloaders']]))) 

                if 'error' in res:
                    self.info_dl['status'] = 'error'
                    self.info_dl['error_message'] = '\n'.join([dl.error_message for dl in self.info_dl['downloaders']])
                
                else: 
                    self.info_dl['status'] = "init_manipulating"
        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]:[run_dl] error when DL\n{'!!'.join(lines)}")
        finally:    
            for t in tasks_run: t.cancel()
            await asyncio.wait(tasks_run)
             
    def _get_subs_files(self):
     
        key = None
        for _el in (_keys:=list(self.info_dl['requested_subtitles'].keys())):
            if _el.startswith('es'): 
                key = _el
                break
            if _el.startswith('en'):
                key= _el
        
        if not key: return            
      
        subtitles = self.info_dl['requested_subtitles'][key]
        
        for value in subtitles:
        
            try:
                if (_ext:=value.get('ext') in ('srt', 'vtt')):
                    
                    _content = httpx.get(value['url']).text
                    #reader = detect_format(_srt)
                
                    #_ext = SUPPORTED_EXT[reader]
                    _subs_file = f"{self.info_dl['filename'].parent}/{self.info_dl['filename'].stem}.{key}.{_ext}"
                
                # with open(f'{_subs_file_stem}.{_ext}', "wb") as f:
                #     f.write(res.content)
                    
                # if reader is not SRTReader: 
                
                #     _srt = SRTWriter().write(reader().read(_srt))
                #     _ext = 'srt'                  
                    
                    with open(_subs_file, "w") as f:
                        f.write(_content)
                        
                
                #value['file'] = f'{_subs_file_stem}.{_ext}' #the srt format will be embed to the video file
                    
                    logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: subs file for [{key}] downloaded in {_ext} format")
                    
            except Exception as e:
                lines = traceback.format_exception(*sys.exc_info())                
                logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when downloading subs file\n{'!!'.join(lines)}")
           
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
            if not self.lock:
                self.lock = asyncio.Lock()
            
            self.info_dl['status'] = "manipulating"
            for dl in self.info_dl['downloaders']: 
                if dl.status == 'init_manipulating':
                    dl.status = 'manipulating'
                

            blocking_tasks = [async_ex_in_executor(self.ex_videodl, dl.ensamble_file) for dl in self.info_dl['downloaders'] if (not 'aria2' in str(type(dl)).lower() and dl.status == 'manipulating')]
            if self.info_dl.get('requested_subtitles'):
                blocking_tasks += [async_ex_in_executor(self.ex_videodl, self._get_subs_files)]
            await asyncio.sleep(0)
            if blocking_tasks:
                done, _ = await asyncio.wait(blocking_tasks)
            
                for d in done:
                    
                    try:
                        d.result()
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())                
                        logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_manip] result de dl.ensamble_file: {repr(e)}\n{'!!'.join(lines)}")
            
            res = True
            for dl in self.info_dl['downloaders']:
                res = res and (_exists:= await async_ex_in_executor(self.ex_videodl, dl.filename.exists)) and dl.status == "done"
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}] {dl.filename} exists: [{_exists}] status: [{dl.status}]")
                
                
            
            if res:    
                           
        
                if len(self.info_dl['downloaders']) == 1:
                    
                    rc = -1
                    
                    if "ts" in self.info_dl['downloaders'][0].filename.suffix: #usamos ffmpeg para cambiar contenedor ts del DL de HLS de un sólo stream a mp4
                    
                        cmd = f"ffmpeg -y -probesize max -loglevel repeat+info -i file:{str(self.info_dl['downloaders'][0].filename)} -c copy -map 0 -dn -f mp4 -bsf:a aac_adtstoasc file:{str(self.info_dl['filename'])}"
                        
                        res = await async_ex_in_executor(self.ex_videodl, self._syncpostffmpeg, cmd)
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {cmd}\n[rc] {res.returncode}\n[stdout]\n{res.stdout}\n[stderr]{res.stderr}")
                        rc = res.returncode
                        
                    else:                        
            
                        rc = -1
                        try:
                            
                            res = await async_ex_in_executor(self.ex_videodl, shutil.move, self.info_dl['downloaders'][0].filename, self.info_dl['filename'])
                            if (res == self.info_dl['filename']): rc = 0
                            
                            
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())                
                            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when manipulating\n{'!!'.join(lines)}")
                            
                    if rc == 0 and (await async_ex_in_executor(self.ex_videodl, self.info_dl['filename'].exists)):
                    
                        
                        # if (_file_subs_en:=self.info_dl.get('requested_subtitles', {}).get('en', {}).get('file')):
                            
                        #     rc = await asyncio.to_thread(self.embed_subs, _file_subs_en, self.info_dl['filename'], logger, f"[{self.info_dict['id']}][{self.info_dict['title']}]")
                                                                
                        self.info_dl['status'] = "done"
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: DL video file OK")
                        
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error move file: {rc}")
                        
                else:
                    
                    cmd = f"ffmpeg -y -loglevel repeat+info -i file:{str(self.info_dl['downloaders'][0].filename)} -i file:{str(self.info_dl['downloaders'][1].filename)} -c copy -map 0:v:0 -map 1:a:0 file:{str(self.info_dl['filename'])}"
                    
                    rc = -1
                    
                    res = await async_ex_in_executor(self.ex_videodl, self._syncpostffmpeg, cmd)
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: ffmpeg rc[{res.returncode}]\n{res.stdout}")
                    rc = res.returncode
                    
                    if rc == 0 and (await async_ex_in_executor(self.ex_videodl, self.info_dl['filename'].exists)):
                        
                        # if (_file_subs_en:=self.info_dl['requested_subtitles'].get('en', {}).get('file')):
                            
                        #     rc = await asyncio.to_thread(self.embed_subs, _file_subs_en, self.info_dl['filename'], logger,f"[{self.info_dict['id']}][{self.info_dict['title']}]")
                                                
                        self.info_dl['status'] = "done"          
                        for dl in self.info_dl['downloaders']:
                            
                            await async_ex_in_executor(self.ex_videodl, dl.filename.unlink)
                            
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: Streams merged for: {self.info_dl['filename']}")
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: DL video file OK")
                    
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error merge, ffmpeg error: {rc}")
                
                    
                if self.info_dl['status'] == "done":
                    #await asyncio.to_thread(functools.partial(rmtree, self.info_dl['download_path'], ignore_errors=True))
                    await async_ex_in_executor(self.ex_videodl, rmtree, self.info_dl['download_path'], ignore_errors=True)
                    if (mtime:=self.info_dict.get("release_timestamp")):
                        await async_ex_in_executor(self.ex_videodl, os.utime, self.info_dl['filename'], (int(datetime.now().timestamp()), mtime))
                        

            else: self.info_dl['status'] = "error"
                               
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when manipulating\n{'!!'.join(lines)}")
            for t in blocking_tasks: t.cancel()
            await asyncio.wait(blocking_tasks)
            raise 
            
    async def _postffmpeg(self, cmd):        
        
        
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{cmd}")
        
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
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{msg}")
            
        await asyncio.gather(read_stream(proc.stdout), read_stream(proc.stderr), proc.wait())
        
        return proc.returncode
    
    @staticmethod
    def _syncpostffmpeg(cmd):
        
        res = subprocess.run(cmd.split(' '), encoding='utf-8', capture_output=True)
        return res
    
    
    def print_hookup(self):        
        
        msg = ""
        for dl in self.info_dl['downloaders']:
            msg += f"  {dl.print_hookup()}"
        msg += "\n" 
        
        if self.info_dl['status'] == "done":
            return (f"[{self.info_dict['id']}][{self.info_dict['title'][:40]}]: Completed [{naturalsize(self.info_dl['filename'].stat().st_size, format_='.2f')}]\n")
        elif self.info_dl['status'] == "init":
            return (f"[{self.info_dict['id']}][{self.info_dict['title'][:40]}]: Waiting to DL [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")  
        elif self.info_dl['status'] == "init_manipulating":
            return (f"[{self.info_dict['id']}][{self.info_dict['title'][:40]}]: Waiting to create file [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n")           
        elif self.info_dl['status'] == "error":
            return (f"[{self.info_dict['id']}][{self.info_dict['title'][:40]}]: ERROR {naturalsize(self.info_dl['down_size'], format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "stop":
            return (f"[{self.info_dict['id']}][{self.info_dict['title'][:40]}]: STOPPED {naturalsize(self.info_dl['down_size'], format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "downloading":
            if self.pause_event and self.pause_event.is_set(): status = "PAUSED"
            else: status ="Downloading"            
            return (f"[{self.info_dict['id']}][{self.info_dict['title'][:40]}]: {status} [{naturalsize(self.info_dl['down_size'])}/{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "manipulating": 
            if self.info_dl['filename'].exists(): _size = self.info_dl['filename'].stat().st_size
            else: _size = 0
            return (f"[{self.info_dict['id']}][{self.info_dict['title'][:40]}]:  Ensambling/Merging {naturalsize(_size, format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
        
        