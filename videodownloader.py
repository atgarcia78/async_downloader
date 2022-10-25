
import asyncio
import logging
import shutil
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path
from shutil import rmtree

import httpx

from yt_dlp.utils import determine_protocol, sanitize_filename

from asyncffmpegdownloader import AsyncFFMPEGDownloader
from asyncaria2cdownloader import AsyncARIA2CDownloader
from asyncdashdownloader import AsyncDASHDownloader
from asynchlsdownloader import AsyncHLSDownloader
from asynchttpdownloader import AsyncHTTPDownloader
from utils import async_ex_in_executor, naturalsize, traverse_obj, try_get

import os
from concurrent.futures import ThreadPoolExecutor

FORCE_TO_HTTP = [''] #['doodstream']


logger = logging.getLogger("video_DL")

class VideoDownloader:
    
    def __init__(self, window_root, video_dict, ytdl, args, hosts_dl, alock, hosts_alock): 
        
        try:
            
            self.hosts_dl = hosts_dl
            self.master_alock = alock
            self.master_hosts_alock = hosts_alock
            self.args = args
            
            self._index = None #for printing
            self.window_root = window_root
            
            self.info_dict = video_dict
            
            _date_file = datetime.now().strftime("%Y%m%d")
            
            if not self.args.path:
                _download_path = Path(Path.home(), "testing", _date_file, self.info_dict['id']) 
            else:
                _download_path = Path(self.args.path, self.info_dict['id'])
            
                
            self.info_dl = {
                
                'id': self.info_dict['id'],
                'n_workers': self.args.parts,
                'rpcport': self.args.rpcport, 
                'auto_pasres': False,
                'webpage_url': self.info_dict.get('webpage_url'),
                'title': self.info_dict.get('title'),
                'ytdl': ytdl,
                'date_file': _date_file,
                'download_path': _download_path,
                'filename': Path(_download_path.parent, str(self.info_dict['id']) + 
                                 "_" + sanitize_filename(self.info_dict['title'], restricted=True) + 
                                 "." + self.info_dict.get('ext', 'mp4')),
                'backup_http': self.args.use_http_failover
            } 
                
            self.info_dl['download_path'].mkdir(parents=True, exist_ok=True)  
            
            downloaders = []
            
            _new_info_dict = self.info_dict.copy()
            _new_info_dict.update({'filename': self.info_dl['filename'], 
                              'download_path': self.info_dl['download_path']})
            
            if (dl:=self._check_if_apple(_new_info_dict)):
                downloaders.append(dl)
            else:
                dl = self._get_dl(_new_info_dict)
                if isinstance(dl, list): downloaders.extend(dl)
                else: 
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
                'requested_subtitles': _req_sub.copy() if (_req_sub:=self.info_dict.get('requested_subtitles')) else {},
                'filesize': sum([dl.filesize for dl in downloaders if dl.filesize]),
                'down_size': sum([dl.down_size for dl in downloaders]),
                'status': _status,
                'error_message': ""             
            })
            

            self.pause_event = None
            self.resume_event = None
            self.stop_event = None
            self.alock = None
            
            
            
            
        except Exception as e:            
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}] DL constructor failed {repr(e)}")
            self.info_dl['status'] = "error"
        finally:
            if self.info_dl['status'] == "error":
                rmtree(self.info_dl['download_path'], ignore_errors=True)
    
    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value
      
    @index.deleter
    def index(self):
        del self._index 

    def _check_if_apple(self, info):
        
        try:
        
            if not (_info:=info.get('requested_formats')):
                #_info = [info]
                return
                
            prots, urls = list(map(list, zip(*[(determine_protocol(f), f['url']) for f in _info])))
            
            if any("dash" in _ for _ in prots):
                #return(AsyncFFMPEGDownloader(info, self))
                pass
            elif all("m3u8" in _ for _ in prots):
                if any("dash" in _ for _ in urls):
                    return(AsyncFFMPEGDownloader(info, self))
                else:
                    res = [self.syncpostffmpeg(f"ffmpeg -i {_url}").stderr for _url in urls]
                    if any(".mp4" in _ for _ in res):
                        return(AsyncFFMPEGDownloader(info, self))

        except Exception as e:
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]check if apple failed - {repr(e)}\n{info}")
            
            
    def _get_dl(self, info): 
        
        try:
        
            if not (_info:=info.get('requested_formats')):
                _info = [info]
            else:
                for f in _info:
                    f.update({'id': self.info_dl['id'], 'title': self.info_dl['title'],
                                '_filename': self.info_dl['filename'], 'download_path': self.info_dl['download_path'],
                                'webpage_url': self.info_dl['webpage_url'], 'extractor_key': self.info_dict.get('extractor_key')}) 
            res_dl = []
            
            for info in _info:        
            
                dl = None
                
                protocol = determine_protocol(info)
                            
                if protocol in ('http', 'https'):
                    if self.info_dl['rpcport'] and (info.get('extractor_key').lower() not in FORCE_TO_HTTP):
                                        
                        try:
                                        
                            dl = AsyncARIA2CDownloader(self.info_dl['rpcport'], self.args.proxy, info, self)
                            logger.debug(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type ARIA2C")
                            if dl.auto_pasres: self.info_dl.update({'auto_pasres': True})
                        except Exception as e:
                            if self.info_dl['backup_http']:
                                logger.warning(f"[{info['id']}][{info['title']}][{info['format_id']}][{info.get('extractor_key').lower()}]: aria2c init failed, swap to HTTP DL")
                                dl = AsyncHTTPDownloader(info, self)
                                logger.debug(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type HTTP")
                                if dl.auto_pasres: self.info_dl.update({'auto_pasres': True}) 
                            else: raise
                    
                    else: #--aria2c 0 or extractor is doodstream
                        
                        dl = AsyncHTTPDownloader(info, self)
                        logger.debug(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type HTTP")
                        if dl.auto_pasres: self.info_dl.update({'auto_pasres': True}) 
                                        
                elif protocol in ('m3u8', 'm3u8_native'):
                    dl = AsyncHLSDownloader(self.args.proxy, info, self)
                    logger.debug(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type HLS")
                                
                elif protocol in ('http_dash_segments', 'dash'):
                    dl = AsyncDASHDownloader(info, self)
                    logger.debug(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type DASH")
                else:
                    logger.error(f"[{info['id']}][{info['title']}][{info['format_id']}]: protocol not supported")
                    raise NotImplementedError("protocol not supported")
                                
                if dl:
                    res_dl.append(dl)
        
            return res_dl 

        except Exception as e:
            logger.exception(repr(e))

    def change_numvidworkers(self, n):
        _reset = False
        for dl in self.info_dl['downloaders']:
            if any([_ in str(type(dl)).lower() for _ in ('hls', 'dash')]):                
                dl.n_workers = n
                _reset = True
        if self.info_dl['status']  == "downloading" and _reset and self.reset_event:
            self.reset_event.set()
        
        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: workers set to {n}")        
    
    def reset(self):
        if self.reset_event:
            self.resume()
            self.reset_event.set()                
            logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: event reset")
    
    def stop(self):
        self.info_dl['status'] = "stop"
        #try_get(self.info_dl['ytdl'].params.get('stop'), lambda x: x.set())
        if self.stop_event:
            self.resume()
            self.stop_event.set()
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: stop")

    def pause(self):
        if self.pause_event:
            self.pause_event.set()

    def resume(self):
        if self.resume_event:
            if self.pause_event.is_set(): 
                self.resume_event.set()

    
    def write_window(self):
        if self.info_dl['status'] not in ("init", "downloading", "manipulating", "init_manipulating"):
            mens = {self.index: self.print_hookup()}
            self.window_root.write_event_value(self.info_dl['status'], mens)
        

    async def run_dl(self):

        
        self.pause_event = asyncio.Event()
        self.resume_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.reset_event = asyncio.Event()
        self.alock = asyncio.Lock()
        
        try:
            tasks_run = []
            if self.info_dl['status'] != "stop":
                self.info_dl['status'] = "downloading"
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] status {[dl.status for dl in self.info_dl['downloaders']]}")
                tasks_run = [asyncio.create_task(dl.fetch_async()) for dl in self.info_dl['downloaders'] if dl.status not in ("init_manipulating", "done")]
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] tasks run {len(tasks_run)}")
            
                if tasks_run:
                    
                    done, _ = await asyncio.wait(tasks_run, return_when=asyncio.ALL_COMPLETED)
                            
                    if done:
                        for d in done:
                            try:                        
                                d.result()  
                            except Exception as e:
                                lines = traceback.format_exception(*sys.exc_info())                
                                logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] error fetch_async: {repr(e)}\n{'!!'.join(lines)}")
                                
            
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
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]:[run_dl] error when DL {repr(e)}")
            self.info_dl['status'] = 'error'
        finally:  
            self.write_window()  
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
      
        subtitles = traverse_obj(self.info_dl, ('requested_subtitles', key))
        
        if isinstance(subtitles, dict):
            subtitles = [subtitles]
        
        for value in subtitles:
        
            try:
                if ((_ext:=value.get('ext')) in ('srt', 'vtt', 'ttml')):
                    
                    _content = httpx.get(value['url']).text

                    _subs_file = f"{self.info_dl['filename'].parent}/{self.info_dl['filename'].stem}.{key}.{_ext}"               
                    
                    with open(_subs_file, "w") as f:
                        f.write(_content)

                    
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
            
            res = VideoDownloader.syncpostffmpeg(cmd)
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

        ex_videodl = ThreadPoolExecutor(thread_name_prefix="ex_videodl")
        
        try:
            if not self.alock:
                self.alock = asyncio.Lock()
            
            self.info_dl['status'] = "manipulating"
            for dl in self.info_dl['downloaders']: 
                if dl.status == 'init_manipulating':
                    dl.status = 'manipulating'
                
            self.write_window()
            blocking_tasks = [asyncio.create_task(async_ex_in_executor(ex_videodl, dl.ensamble_file)) for dl in self.info_dl['downloaders'] if (not any(_ in str(type(dl)).lower() for _ in ('aria2', 'ffmpeg')) and dl.status == 'manipulating')]
            if self.info_dl.get('requested_subtitles'):
                blocking_tasks += [asyncio.create_task(async_ex_in_executor(ex_videodl, self._get_subs_files))]
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
                res = res and (_exists:= await async_ex_in_executor(ex_videodl, dl.filename.exists)) and dl.status == "done"
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}] {dl.filename} exists: [{_exists}] status: [{dl.status}]")

            if res:    

                if len(self.info_dl['downloaders']) == 1:
                    
                    rc = -1

                    if "ts" in self.info_dl['downloaders'][0].filename.suffix: #usamos ffmpeg para cambiar contenedor ts del DL de HLS de un sólo stream a mp4
                    
                        cmd = f"ffmpeg -y -probesize max -loglevel repeat+info -i file:{str(self.info_dl['downloaders'][0].filename)} -c copy -map 0 -dn -f mp4 -bsf:a aac_adtstoasc file:{str(self.info_dl['filename'])}"

                        res = await async_ex_in_executor(ex_videodl, self.syncpostffmpeg, cmd)
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {cmd}\n[rc] {res.returncode}\n[stdout]\n{res.stdout}\n[stderr]{res.stderr}")
                        rc = res.returncode
                        
                    else:                         
            
                        rc = -1
                        try:
                            
                            res = await async_ex_in_executor(ex_videodl, shutil.move, self.info_dl['downloaders'][0].filename, self.info_dl['filename'])
                            if (res == self.info_dl['filename']): rc = 0
                            
                            
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())                
                            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when manipulating\n{'!!'.join(lines)}")
                            
                    if rc == 0 and (await async_ex_in_executor(ex_videodl, self.info_dl['filename'].exists)):
                                                                
                        self.info_dl['status'] = "done"
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: DL video file OK")
                        
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error move file: {rc}")
                        
                else:
                    
                    cmd = f"ffmpeg -y -loglevel repeat+info -i file:{str(self.info_dl['downloaders'][0].filename)} -i file:{str(self.info_dl['downloaders'][1].filename)} -c copy -map 0:v:0 -map 1:a:0 -bsf:a:0 aac_adtstoasc -movflags +faststart file:{str(self.info_dl['filename'])}"
                    
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {cmd}")
                    
                    
                    rc = -1
                    
                    res = await async_ex_in_executor(ex_videodl, self.syncpostffmpeg, cmd)
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: ffmpeg rc[{res.returncode}]\n{res.stdout}")
                    rc = res.returncode
                    
                    if rc == 0 and (await async_ex_in_executor(ex_videodl, self.info_dl['filename'].exists)):
                                                
                        self.info_dl['status'] = "done"          
                        for dl in self.info_dl['downloaders']:
                            
                            await async_ex_in_executor(ex_videodl, dl.filename.unlink)
                            
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: Streams merged for: {self.info_dl['filename']}")
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: DL video file OK")
                    
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error merge, ffmpeg error: {rc}")
                
                    
                if self.info_dl['status'] == "done":
                    await async_ex_in_executor(ex_videodl, rmtree, self.info_dl['download_path'], ignore_errors=True)
                    if (mtime:=self.info_dict.get("release_timestamp")):
                        await async_ex_in_executor(ex_videodl, os.utime, self.info_dl['filename'], (int(datetime.now().timestamp()), mtime))
                        

            else: self.info_dl['status'] = "error"
                               
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when manipulating\n{'!!'.join(lines)}")
            for t in blocking_tasks: t.cancel()
            await asyncio.wait(blocking_tasks)
            raise
        finally:
            self.write_window()
            ex_videodl.shutdown(wait=False,cancel_futures=True)
            await asyncio.sleep(0) 

    @staticmethod
    def syncpostffmpeg(cmd):
        
        res = subprocess.run(cmd.split(' '), encoding='utf-8', capture_output=True)
        return res

    def print_hookup(self):        
        
        msg = ""
        for dl in self.info_dl['downloaders']:
            msg += f"  {dl.print_hookup()}"
        msg += "\n" 
        _title = self.info_dict['title'] if ((_len:=len(self.info_dict['title'])) >= 40) else self.info_dict['title'] + ' '*(40 - _len) 

        if self.info_dl['status'] == "done":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:40]}]: Completed [{naturalsize(self.info_dl['filename'].stat().st_size, format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "init":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:40]}]: Waiting to DL [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")  
        elif self.info_dl['status'] == "init_manipulating":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:40]}]: Waiting to create file [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")           
        elif self.info_dl['status'] == "error":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:40]}]: ERROR {naturalsize(self.info_dl['down_size'], format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "stop":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:40]}]: STOPPED {naturalsize(self.info_dl['down_size'], format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "downloading":
            if self.pause_event and self.pause_event.is_set(): status = "PAUSED"
            else: status ="Downloading"            
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:40]}]: {status} [{naturalsize(self.info_dl['down_size'], format_='6.2f')}/{naturalsize(self.info_dl['filesize'], format_='6.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "manipulating": 
            if self.info_dl['filename'].exists(): _size = self.info_dl['filename'].stat().st_size
            else: _size = 0
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:40]}]:  Ensambling/Merging {naturalsize(_size, format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
