import asyncio
import logging
import shlex
import shutil
import subprocess
import sys
import traceback
import urllib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from queue import Queue

import aiofiles
import aiofiles.os as os
import httpx
import m3u8
from yt_dlp.utils import determine_protocol, sanitize_filename

from asyncaria2cdownloader import AsyncARIA2CDownloader
from asyncdashdownloader import AsyncDASHDownloader
from asyncffmpegdownloader import AsyncFFMPEGDownloader
from asynchlsdownloader import AsyncHLSDownloader
from asynchttpdownloader import AsyncHTTPDownloader
from utils import (async_ex_in_executor, naturalsize, prepend_extension,
                   sync_to_async, traverse_obj, try_get, MyAsyncioEvent)

FORCE_TO_HTTP = [''] 

logger = logging.getLogger("video_DL")


class VideoDownloader:

    _PLNS = {}
    
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
                'backup_http': self.args.use_http_failover,
                'fromplns': VideoDownloader._PLNS
                
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
            if any([_ in res for _ in ("done", "init_manipulating")]):#(res == ["init_manipulating"] or res == ["done"] or res == ["done", "init_manipulating"]):
                _status = "init_manipulating"
            elif "error" in res:
                _status = "error"
            else:
                _status = "init"
                
            self.info_dl.update({
                'downloaders': downloaders,
                'downloaded_subtitles': {},
                'filesize': sum([dl.filesize for dl in downloaders if dl.filesize]),
                'down_size': sum([dl.down_size for dl in downloaders]),
                'status': _status,
                'error_message': ""
            })
            

            self.pause_event = None
            self.resume_event = None
            self.stop_event = None
            self.reset_event = None
            self.end_tasks = None
            self.alock = None
            self.awrite_window  = None

            self.ex_videodl = ThreadPoolExecutor(thread_name_prefix="ex_videodl")

        except Exception as e:            
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}] DL constructor failed {repr(e)}")
            self.info_dl['status'] = "error"
    
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
                return
                
            prots, urls = list(map(list, zip(*[(determine_protocol(f), f['url']) for f in _info])))
            
            if all("m3u8" in _ for _ in prots):
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
                _streams = False
            else:
                for f in _info:
                    f.update({'id': self.info_dl['id'], 'title': self.info_dl['title'],
                                '_filename': self.info_dl['filename'], 'download_path': self.info_dl['download_path'],
                                'webpage_url': self.info_dl['webpage_url'], 'extractor_key': self.info_dict.get('extractor_key')}) 
                _streams = True
            
            res_dl = []

            for n, info in enumerate(_info):        
            
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
                    if dl.auto_pasres: self.info_dl.update({'auto_pasres': True})
                                
                elif protocol in ('http_dash_segments', 'dash'):
                    if _streams: _str = n
                    else: _str = None
                    dl = AsyncDASHDownloader(info, self, stream=_str)
                    logger.debug(f"[{info['id']}][{info['title']}][{info['format_id']}][get_dl] DL type DASH")
                else:
                    logger.error(f"[{info['id']}][{info['title']}][{info['format_id']}]: protocol not supported")
                    raise NotImplementedError("protocol not supported")
                                
                if dl:
                    res_dl.append(dl)
        
            return res_dl 

        except Exception as e:
            logger.exception(repr(e))

    async def change_numvidworkers(self, n):
       
        for dl in self.info_dl['downloaders']:
            dl.n_workers = n
            if 'aria2' in str(type(dl)).lower():
                dl.opts.set('split', dl.n_workers)
            
        if self.info_dl['status']  == "downloading": await self.reset("manual")
        
        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: workers set to {n}")        
    
    async def reset_from_console(self, cause=None):
        if 'hls' in str(type(self.info_dl['downloaders'][0])).lower():
            if (_plns:=self.info_dl['downloaders'][0].fromplns):
                await self.reset_plns(cause)
                return
        await self.reset(cause)

    
    async def reset(self, cause=None):
        if self.reset_event:
            _wait_tasks = []
            if not self.reset_event.is_set():
                if self.pause_event.is_set():
                    self.resume_event.set()
                self.reset_event.set(cause)                      
                await asyncio.sleep(0)                
                for dl in self.info_dl['downloaders']:
                    if 'hls' in str(type(dl)).lower():
                        _wait_tasks.extend(try_get([_task for _task in dl.tasks if not _task.done() and not _task.cancelled() and _task not in [asyncio.current_task()]], lambda x: [_t for _t in x if any([(_res:=_t.cancel()), not _res])]))
                
            else:
                self.reset_event.set(cause)

            return _wait_tasks

    async def reset_plns(self, cause="403"):
        self.info_dl['fromplns']['ALL']['reset'].clear()
         
        plid_total = self.info_dl['fromplns']['ALL']['downloading']

        _wait_all_tasks = []
                
        for plid in plid_total:

            dict_dl = traverse_obj(self.info_dl['fromplns'], (plid, 'downloaders'))
            list_dl = traverse_obj(self.info_dl['fromplns'], (plid, 'downloading'))
            list_reset = traverse_obj(self.info_dl['fromplns'], (plid, 'in_reset'))
            
            if list_dl and dict_dl:
                self.info_dl['fromplns']['ALL']['in_reset'].add(plid)
                self.info_dl['fromplns'][plid]['reset'].clear()               
                plns = [dl for key,dl in dict_dl.items() if key in list_dl]
                for dl,key in zip(plns, list_dl):
                    _wait_all_tasks.extend(await dl.reset(cause))
                    list_reset.add(key)
        return _wait_all_tasks
        
    async def back_from_reset_plns(self, logger, premsg):
        _tasks_all = []
        for plid in  self.info_dl['fromplns']['ALL']['in_reset']:
            dict_dl = traverse_obj(self.info_dl['fromplns'], (plid, 'downloaders'))
            list_reset = traverse_obj(self.info_dl['fromplns'], (plid, 'in_reset'))        
            if list_reset and dict_dl:
                plns = [dl for key,dl in dict_dl.items() if key in list_reset]
                _tasks_all.extend([asyncio.create_task(dl.end_tasks.wait()) for dl in plns])
                
        logger.debug(f"{premsg} endtasks {_tasks_all}")
        if _tasks_all:
            _2tasks = {asyncio.wait(_tasks_all): 'tasks',  asyncio.create_task(self.stop_event.wait()): 'stop'}
            done, pending = await asyncio.wait(_2tasks, return_when=asyncio.FIRST_COMPLETED)
            for _el in pending: 
                if _2tasks.get(_el) == 'tasks':
                    for _t in _tasks_all: _t.cancel()
                else: _el.cancel()

    async def stop(self):
        self.info_dl['status'] = "stop"
        for dl in self.info_dl['downloaders']:
            dl.status = "stop"
        if self.stop_event:
            if self.pause_event.is_set():
                self.resume_event.set()            
            await self.awrite_window()
            self.stop_event.set()
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: stop")

    async def pause(self):
        if self.pause_event:            
            self.pause_event.set()

    async def resume(self):
        if self.resume_event:
            self.resume_event.set()
    
    def write_window(self):
        if self.info_dl['status'] not in ("init", "downloading", "manipulating", "init_manipulating"):
            mens = {self.index: self.print_hookup()}
            if self.window_root:
                self.window_root.write_event_value(self.info_dl['status'], mens)
        
    async def run_dl(self):
        
        self.pause_event = asyncio.Event()
        self.resume_event = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.end_tasks = asyncio.Event()
        self.info_dl['ytdl'].params['stop_dl'][str(self.index)] = self.stop_event
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] [stop_dl] {self.info_dl['ytdl'].params['stop_dl']}")
        self.reset_event = MyAsyncioEvent()
        self.alock = asyncio.Lock()
        

        self.awrite_window = sync_to_async(self.write_window, self.ex_videodl)
        
        try:
            
            if self.info_dl['status'] != "stop":
                self.info_dl['status'] = "downloading"
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] status {[dl.status for dl in self.info_dl['downloaders']]}")
                tasks_run_0 = [asyncio.create_task(dl.fetch_async()) for i, dl in enumerate(self.info_dl['downloaders']) if i == 0 and dl.status not in ("init_manipulating", "done")]
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] tasks run {len(tasks_run_0)}")
            
                if tasks_run_0:
                    
                    done, _ = await asyncio.wait(tasks_run_0)
                    
                    if len(self.info_dl['downloaders']) > 1:
                        tasks_run_1 = [asyncio.create_task(dl.fetch_async()) for i, dl in enumerate(self.info_dl['downloaders']) if i == 1 and dl.status not in ("init_manipulating", "done")]
                        if tasks_run_1:
                            done1, _ = await asyncio.wait(tasks_run_1)
                            done = done.union(done1)
                            
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

                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] salida tasks {res}")

                    if 'error' in res:
                        self.info_dl['status'] = 'error'
                        self.info_dl['error_message'] = '\n'.join([dl.error_message for dl in self.info_dl['downloaders']])
                    
                    else: 
                        self.info_dl['status'] = "init_manipulating"

        except Exception as e:
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]:[run_dl] error when DL {repr(e)}")
            self.info_dl['status'] = 'error'
        finally:  
            await self.awrite_window()  
             
    def _get_subts_files(self):
     
        
        def _dl_subt(url):

            subt_url = url
            if '.m3u8' in url:
                m3u8obj = m3u8.load(url, headers = self.info_dict.get('http_headers'))
                subt_url = urllib.parse.urljoin(m3u8obj.segments[0]._base_uri, m3u8obj.segments[0].uri)
        
            return(httpx.get(subt_url, headers = self.info_dict.get('http_headers')).text)
        
        _subts = self.info_dict.get('subtitles') or self.info_dict.get('requested_subtitles')

        if not _subts: return

        _langs = {}
        
        for key in list(_subts.keys()):
            if key.startswith('es'): 
                _langs['es'] = key

        _final_lang = 'es'

        if not _langs:

            for key in list(_subts.keys()):
                if key.startswith('en'): 
                    _langs['en'] = key

            if not _langs: return

            _final_lang = 'en'


        if any(['srt' in list(el.values()) for el in _subts[_langs[_final_lang ]]]):
            _format = 'srt'
        elif any(['ttml' in list(el.values()) for el in _subts[_langs[_final_lang ]]]):
            _format = 'ttml'
        elif any(['vtt' in list(el.values()) for el in _subts[_langs[_final_lang ]]]):
            _format = 'vtt'
        else: return
        
        for el in _subts[_langs[_final_lang]]:
            if el['ext'] == _format:
                
                try:

                    _subts_file = Path(f"{self.info_dl['filename'].absolute().parent}/{self.info_dl['filename'].stem}.{_final_lang}.{_format}")

                    _content = _dl_subt(el['url'])

                    with open(_subts_file, "w") as f:
                        f.write(_content)

                    logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: subs file for [{_final_lang}] downloaded in {_format} format")

                    if _format == 'ttml':
                        _final_subts_file = Path(str(_subts_file).replace(f".{_format}", ".srt"))
                        cmd = f'tt convert -i "{_subts_file}" -o "{_final_subts_file}"'
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: convert subt - {cmd}")
                        res = subprocess.run(shlex.split(cmd), encoding='utf-8', capture_output=True)
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: subs file conversion result {res.returncode}")
                        if (res.returncode == 0) and _final_subts_file.exists():
                            _subts_file.unlink()
                            logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: subs file for [{_final_lang}] in {_format} format converted to srt format")
                            self.info_dl['downloaded_subtitles'].update({_final_lang: _final_subts_file})
                    elif _format == 'vtt':
                        _final_subts_file = Path(str(_subts_file).replace(f".{_format}", ".srt"))
                        cmd = f'ffmpeg -y -loglevel repeat+info -i file:\"{_subts_file}\" -f srt -movflags +faststart file:\"{_final_subts_file}\"'
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: convert subt - {cmd}")
                        res = self.syncpostffmpeg(cmd)
                        logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: subs file conversion result {res.returncode}")
                        if (res.returncode == 0) and _final_subts_file.exists():
                            _subts_file.unlink()
                            logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: subs file for [{_final_lang}] in {_format} format converted to srt format")
                            self.info_dl['downloaded_subtitles'].update({_final_lang: _final_subts_file})

                    else: 
                        self.info_dl['downloaded_subtitles'].update({_final_lang: _subts_file})

                    break

                except Exception as e:
                    logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: couldnt generate subtitle file: {repr(e)}")

    async def run_manip(self):


        aget_subts_files = sync_to_async(self._get_subts_files, self.ex_videodl)
        apostffmpeg = sync_to_async(self.syncpostffmpeg, self.ex_videodl)
        armtree = sync_to_async(partial(shutil.rmtree, ignore_errors=True), self.ex_videodl)
        amove = sync_to_async(shutil.move, self.ex_videodl)

        if not self.awrite_window:
            self.awrite_window = sync_to_async(self.write_window, self.ex_videodl)

        
        
        try:
            if not self.alock:
                self.alock = asyncio.Lock()
            
            self.info_dl['status'] = "manipulating"
            for dl in self.info_dl['downloaders']: 
                if dl.status == 'init_manipulating':
                    dl.status = 'manipulating'
                
            await self.awrite_window()


            blocking_tasks = [asyncio.create_task(async_ex_in_executor(self.ex_videodl, dl.ensamble_file)) 
                                for dl in self.info_dl['downloaders'] if (
                                    not any(_ in str(type(dl)).lower() for _ in ('aria2', 'ffmpeg')) and dl.status == 'manipulating')]
            
            if self.info_dict.get('subtitles') or self.info_dict.get('requested_subtitles'):
               blocking_tasks += [asyncio.create_task(aget_subts_files())]
            
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
                res = res and (_exists:= await os.path.exists(dl.filename)) and dl.status == "done"
                logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}] {dl.filename} exists: [{_exists}] status: [{dl.status}]")

            if res:    

                temp_filename = prepend_extension(str(self.info_dl['filename']), 'temp')

                if len(self.info_dl['downloaders']) == 1:
                    
                    rc = -1

                    if "ts" in self.info_dl['downloaders'][0].filename.suffix: #usamos ffmpeg para cambiar contenedor ts del DL de HLS de un sólo stream a mp4
                    
                        cmd = f"ffmpeg -y -probesize max -loglevel repeat+info -i file:\"{str(self.info_dl['downloaders'][0].filename)}\" -c copy -map 0 -dn -f mp4 -bsf:a aac_adtstoasc file:\"{temp_filename}\""

                        res = await apostffmpeg(cmd)
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {cmd}\n[rc] {res.returncode}\n[stdout]\n{res.stdout}\n[stderr]{res.stderr}")
                        rc = res.returncode
                        
                    else:                         

                        rc = -1
                        try:
                            
                            res = await amove(self.info_dl['downloaders'][0].filename, temp_filename)
                            if (res == temp_filename): rc = 0

                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())                
                            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when manipulating\n{'!!'.join(lines)}")
                            
                    if rc == 0 and (await os.path.exists(temp_filename)):
                                                                
                        self.info_dl['status'] = "done"
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: DL video file OK")

                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error move file: {rc}")
                        
                else:
                    
                    cmd = f"ffmpeg -y -loglevel repeat+info -i file:\"{str(self.info_dl['downloaders'][0].filename)}\" -i file:\"{str(self.info_dl['downloaders'][1].filename)}\" -c copy -map 0:v:0 -map 1:a:0 -bsf:a:0 aac_adtstoasc -movflags +faststart file:\"{temp_filename}\""
                    
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {cmd}")

                    rc = -1
                    
                    res = await apostffmpeg(cmd)

                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: ffmpeg rc[{res.returncode}]\n{res.stdout}")
                    rc = res.returncode
                    
                    if rc == 0 and (await os.path.exists(temp_filename)):
                                                
                        self.info_dl['status'] = "done"          
                        for dl in self.info_dl['downloaders']:
                            
                            await os.remove(dl.filename)
                            
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: Streams merged for: {self.info_dl['filename']}")
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: DL video file OK")
                    
                    else:
                        self.info_dl['status'] = "error"
                        raise Exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error merge, ffmpeg error: {rc}")

                if self.info_dl['status'] == "done":
                    if self.info_dl['downloaded_subtitles']:
                        if len(self.info_dl['downloaded_subtitles']) > 1:
                            lang = 'es'
                            subtfile = self.info_dl['downloaded_subtitles']['es']
                        else:
                            lang, subtfile = list(self.info_dl['downloaded_subtitles'].items())[0]

                        embed_filename = prepend_extension(self.info_dl['filename'], 'embed')

                        cmd = f"ffmpeg -y -loglevel repeat+info -i file:\"{temp_filename}\" -i file:\"{str(subtfile)}\" -map 0 -dn -ignore_unknown -c copy -c:s mov_text -map -0:s -map 1:0 -metadata:s:s:0 language={lang} -movflags +faststart file:\"{embed_filename}\""


                        res = await apostffmpeg(cmd)
                        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: {cmd}\n[rc] {res.returncode}\n[stdout]\n{res.stdout}\n[stderr]{res.stderr}")
                        if res.returncode == 0:
                            await os.replace(embed_filename, self.info_dl['filename'])
                            await os.remove(temp_filename)

                    else: await os.replace(temp_filename, self.info_dl['filename'])
                    await armtree(self.info_dl['download_path'])
                    
                    if (mtime:=self.info_dict.get("release_timestamp")):
                        await os.utime(self.info_dl['filename'], (int(datetime.now().timestamp()), mtime))

            else: self.info_dl['status'] = "error"
                               
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}]: error when manipulating\n{'!!'.join(lines)}")
            for t in blocking_tasks: t.cancel()
            await asyncio.wait(blocking_tasks)
            raise
        finally:
            await self.awrite_window()
            self.ex_videodl.shutdown(wait=False, cancel_futures=True)
            await asyncio.sleep(0) 

    
    def syncpostffmpeg(self, cmd):
        
        res = subprocess.run(shlex.split(cmd), encoding='utf-8', capture_output=True)
        return res

    def print_hookup(self):
        
        msg = ""
        for dl in self.info_dl['downloaders']:
            msg += f"  {dl.print_hookup()}"
        msg += "\n" 

        if self.info_dl['status'] == "downloading": _maxlen = 40
        else: _maxlen = 10

        _title = self.info_dict['title'] if ((_len:=len(self.info_dict['title'])) >= _len) else self.info_dict['title'] + ' '*(_maxlen - _len) 

        if self.info_dl['status'] == "done":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:_maxlen]}]: Completed [{naturalsize(self.info_dl['filename'].stat().st_size, format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "init":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:_maxlen]}]: Waiting to DL [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")  
        elif self.info_dl['status'] == "init_manipulating":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:_maxlen]}]: Waiting to create file [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")           
        elif self.info_dl['status'] == "error":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:_maxlen]}]: ERROR {naturalsize(self.info_dl['down_size'], format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "stop":
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:_maxlen]}]: STOPPED {naturalsize(self.info_dl['down_size'], format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "downloading":
            if self.pause_event and self.pause_event.is_set(): status = "PAUSED"
            else: status ="Downloading"            
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:_maxlen]}]: {status} [{naturalsize(self.info_dl['down_size'], format_='6.2f')}/{naturalsize(self.info_dl['filesize'], format_='6.2f')}]\n {msg}\n")
        elif self.info_dl['status'] == "manipulating": 
            if self.info_dl['filename'].exists(): _size = self.info_dl['filename'].stat().st_size
            else: _size = 0
            return (f"[{self.index+1}][{self.info_dict['id']}][{_title[:_maxlen]}]:  Ensambling/Merging {naturalsize(_size, format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n")
