#!/usr/bin/env python

from queue import Queue
import logging
import sys
import traceback
import json
import demjson
import tkinter as tk
import asyncio
import aiorun 
from pathlib import Path
import pandas as pd
from tabulate import tabulate
import re
import time
import shutil


from utils import ( 
    init_logging,
    init_ytdl,
    init_tk,
    init_argparser,
    patch_http_connection_pool,
    patch_https_connection_pool,
    naturalsize,
    is_playlist,
    get_extractor,
    wait_time,
    kill_processes,
    
)

from concurrent.futures import (
    ThreadPoolExecutor,
    wait as wait_for_futures
)

import subprocess
from yt_dlp.utils import sanitize_filename
from codetiming import Timer
from datetime import datetime
from operator import itemgetter
from videodownloader import VideoDownloader 


from threading import Lock

from yt_dlp.extractor.netdna import NetDNAIE
class AsyncDL():

    
    _INTERVAL_TK = 0.25
    
    def __init__(self, args):
    
        
        self.logger = logging.getLogger("asyncDL")
               
        #args
        self.args = args
        self.parts = self.args.p
        self.workers = self.args.w        
        self.init_nworkers = self.args.winit if self.args.winit > 0 else self.args.w
        
        #youtube_dl
        self.ytdl = init_ytdl(self.args)   
        #self.ytdl.params['dict_videos_to_dl'] = {}     
        
        
        #listas con videos y queues       
        self.list_videos = []
        self.list_initnok = []
        self.list_unsup_urls = []
        self.list_notvalid_urls = []
        self.list_urls_to_check = []
        self.list_initaldl = []
        self.list_dl = []
        self.files_cached = {}
        self.videos_to_dl = []
        
        
        self.queue_vid = Queue()        
        
        #tk control      
        self.stop_tk = False
        
        #contadores sobre número de workers init, workers run y workers manip
        self.count_init = 0
        self.count_run = 0        
        self.count_manip = 0 
        
        self.time_now = datetime.now()
        
        self.lock = Lock()
    
    
    async def run_tk(self, args_tk):
        '''
        Run a tkinter app in an asyncio event loop.
        '''
        
       
        root, text0, text1, text2 = args_tk
        count = 0
        while (not self.list_dl and not self.stop_tk):
            
            await wait_time(self._INTERVAL_TK)
            count += 1
            if count == 10:
                count = 0
                self.logger.debug("[RUN_TK] Waiting for dl")
        
        self.logger.debug(f"[RUN_TK] End waiting. Signal stop_tk[{self.stop_tk}]")
        
        try:  
            while not self.stop_tk:
                
                root.update()
                
                
                res = set([dl.info_dl['status'] for dl in self.list_dl])
                
                if res:
                      
                    _res = sorted(list(res))
                    if (_res == ["done", "error"] or _res == ["error"] or _res == ["done"]) and (self.count_init == self.init_nworkers):                        
                            break
                    else:
                      
                        text0.delete(1.0, tk.END)                        
                        text1.delete(1.0, tk.END)
                        text2.delete(1.0, tk.END)
                        list_downloading = []
                        list_manip = []    
                        for dl in self.list_dl:
                            mens = dl.print_hookup()
                            if dl.info_dl['status'] in ["init"]:
                                text0.insert(tk.END, mens)
                            if dl.info_dl['status'] in ["init_manipulating", "manipulating"]:
                                list_manip.append(mens) 
                            if dl.info_dl['status'] in ["downloading"]:
                                list_downloading.append(mens)  
                            if dl.info_dl['status'] in ["done", "error"]:
                                text2.insert(tk.END,mens)
                                         
                        if list_downloading:
                            text1.insert(tk.END, "\n\n-------DOWNLOADING VIDEO------------\n\n")
                            text1.insert(tk.END, ''.join(list_downloading))
                            
                        if list_manip:
                            text1.insert(tk.END, "\n\n-------CREATING FILE------------\n\n")
                            text1.insert(tk.END, ''.join(list_manip))
                                         
                        
                await wait_time(self._INTERVAL_TK)
       
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            self.logger.error(f"[run_tk]: error\n{'!!'.join(lines)}")
        
        self.logger.debug("[RUN_TK] BYE") 
    
    
    def get_videos_cached(self):        
        
        _repeated = []
        
        last_res = Path(Path.home(),"Projects/common/logs/files_cached.json")
        
        if self.args.nodlcaching and last_res.exists():
            
            with open(last_res,"r") as f:
                self.files_cached = json.load(f)
                
            self.logger.info(f"Total cached videos: [{len(self.files_cached)}]")
        
        else:  
        
            list_folders = [Path(Path.home(), "testing"), Path("/Volumes/Pandaext4/videos"), Path("/Volumes/T7/videos"), Path("/Volumes/Pandaext1/videos"), Path("/Volumes/WD/videos"), Path("/Volumes/WD5/videos")]
             
            for folder in list_folders:
                
                for file in folder.rglob('*'):
                    
                    if file.is_file() and not file.stem.startswith('.') and (file.suffix.lower() in ('.mp4', '.mkv', '.m3u8', '.zip')):

                        #_res = re.findall(r'^([^_]*)_(.*)', file.stem)
                        _res = file.stem.split('_', 1)
                        if len(_res) == 2:
                            _id, _title = _res[0], _res[1]
                            _title = sanitize_filename(_title, restricted=True)
                            _title = _title.upper()
                            _name = f"{_id}_{_title}"
                        else:
                            _name = sanitize_filename(file.stem, restricted=True).upper()

                        if not (_videopath:=self.files_cached.get(_name)): 
                            #if not file.is_symlink(): self.files_cached.update({_name: str(file)})
                            self.files_cached.update({_name: str(file)})
                            
                        else:
                            if _videopath != str(file):
                                #if not file.is_symlink(): _repeated.append({'title':_name, 'indict': _videopath, 'file': str(file)})
                                _repeated.append({'title':_name, 'indict': _videopath, 'file': str(file)})
                
        
            
            self.logger.info(f"Total cached videos: [{len(self.files_cached)}]")
            
            if _repeated:
                
                self.logger.warning("Please check videos repeated in logs")
                self.logger.debug(f"videos repeated: \n {_repeated}")
            
                        
            prev_res = Path(Path.home(),"Projects/common/logs/prev_files_cached.json")
            
                
            if last_res.exists():
                if prev_res.exists(): prev_res.unlink()
                last_res.rename(Path(last_res.parent,f"prev_files_cached.json"))
            
            with open(last_res,"w") as f:
                json.dump(self.files_cached,f)     
            
        return self.files_cached
               

    def get_list_videos(self):
        
        
        fileres = Path(Path.home(), "Projects/common/logs/list_videos.json")
        filecaplinks = Path(Path.home(), "Projects/common/logs/captured_links.txt")
        
        if self.args.lastres:                
                
            if fileres.exists():
                try:
                    with open(fileres, "r") as file:
                        self.list_videos += (json.load(file)).get('entries')
                except Exception as e:
                    self.logger.error("Couldnt get info form last result")
                    
            else:
                self.logger.error("Couldnt get info form last result")
        
        url_list = []
        
        if self.args.caplinks:
            
            with open(filecaplinks, "r") as file:
                _content = file.read()            
                
            url_list += list(set([_line for _line in _content.splitlines() if _line]))
            
        if self.args.collection:
            
            url_list += list(set(self.args.collection))    
       
        
        url_pl_list = []
        for _url in url_list:
            if (_info:=is_playlist(_url, self.ytdl))[0]:
                url_pl_list.append(_url)
                # if (ie_key:=_info[1]):
                #     if not self.ytdl.params['dict_videos_to_dl'].get(ie_key):
                #         self.ytdl.params['dict_videos_to_dl'][ie_key] = []
                #     self.ytdl.params['dict_videos_to_dl'][ie_key].append(_url)
                
                
            else:
                
                _iekey = get_extractor(_url, self.ytdl)
                _info_video = {}
                if 'NetDNA' in _iekey:
                    _info_video = NetDNAIE.get_video_info(_url)
                
                                        
                self.list_videos.append({'_type': 'url', 'url': _url, 'ie_key': _iekey, 'id': _info_video.get('id', '').replace('_', ''), 'title': _info_video.get('title', ''), 'filesize': _info_video.get('filesize', '')})
                


        #self.logger.info(f"DICT EXTRACTORS TO USE SO FAR: {self.ytdl.params['dict_videos_to_dl']}")
        
        if url_pl_list:
            
            self.logger.info(f"[url_playlist_list] {url_pl_list}")
        
            
            
            with ThreadPoolExecutor(max_workers=min(self.init_nworkers, len(url_pl_list))) as ex:
                    
                fut = [ex.submit(self.ytdl.extract_info, url_pl, download=False) for url_pl in url_pl_list]
                done, _ = wait_for_futures(fut)
                            
            _url_pl = []
            for d in done:
                _url_pl += (d.result()).get('entries')
      
            self.logger.debug(f"[url_playlist_lists] entries \n{_url_pl}")
            _items = {}
            for entry in _url_pl:
                _items[entry['url']] = entry
            self.logger.debug(f"[url_playlist_lists] entries dict \n{_items}")            
            self.list_videos += list(_items.values())
            self.logger.debug(f"[url_playlist_lists] list videos \n{self.list_videos}") 
           
            
        if self.args.collection_files:
            
            def get_info_json(file):
                try:
                    with open(file, "r") as f:
                        return json.loads(f.read())
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())
                    self.logger.error(f"[get_list_videos] Error:{repr(e)} \n{'!!'.join(lines)}")
                    return {}
                    
            
            for file in self.args.collection_files:
                self.list_videos += get_info_json(file).get('entries')
        
                
        with open(fileres,"w") as f:
            json.dump({'entries': self.list_videos},f)
        
                
        self.logger.debug(f"[get_list_videos] list videos: \n{self.list_videos}")
        
        return self.list_videos
        
        
    def _check_to_dl(self, info_dict):  
                    
               
        if (info_dict.get('_type') == "url_transparent"): 
            info_dict['aldl_checked'] = False
            return True
        
        if not (_id := info_dict.get('id') ) or not ( _title := info_dict.get('title')):
            info_dict['aldl_checked'] = False
            return True
        
        _title = sanitize_filename(_title, restricted=True).upper()
        #_id = _id[:10] if len(_id) > 10 else _id
        vid_name = f"{_id}_{_title}"                    

        if not (vid_path:=self.files_cached.get(vid_name)):
            
            info_dict['aldl_checked'] = True
            return True
        
        
        else: #video en local
            
            info_dict['aldl_checked'] = True
            
            self.logger.debug(f"[{vid_name}]: already DL: {vid_path}")
                
            self.list_initaldl.append({'title': vid_name, 'path': vid_path})                        
            if self.args.path:
                _filepath = Path(self.args.path)
            else:
                _filepath = Path(Path.home(),"testing",self.time_now.strftime('%Y%m%d'))
            _filepath.mkdir(parents=True, exist_ok=True)
            file_aldl = Path(_filepath, Path(vid_path).name)
            if file_aldl not in _filepath.iterdir():
                file_aldl.symlink_to(Path(vid_path))
                
            return False
        
 
    def get_videos_to_dl(self): 
        
        if self.args.index:
            if self.args.index < len(self.list_videos):
                self.list_videos = [self.list_videos[self.args.index]]
            else: raise IndexError(f"index video {self.args.index} out of range [0..{len(self.videos_to_dl)-1}]")
                
        # elif self.args.first and self.args.last:
        #     if self.args.first <= self.args.last < len(self.list_videos):
        #         self.list_videos = self.list_videos[self.args.first-1:self.args.last] 
        #     else: raise IndexError(f"index issue with '--first {self.args.first}' and '--last {self.args.last}' options and index video range [0..{len(self.videos_to_dl)-1}]")
            
        elif self.args.first:
            if self.args.first <= len(self.list_videos):
                if self.args.last:
                    if self.args.last >= self.args.first:
                        _last = self.args.last - 1
                    else: raise IndexError(f"index issue with '--first {self.args.first}' and '--last {self.args.last}' options and index video range [0..{len(self.videos_to_dl)-1}]")
                else: _last = len(self.list_videos)
                self.list_videos = self.list_videos[self.args.first-1:_last]
            else: raise IndexError(f"index issue with '--first {self.args.first}' and '--last {self.args.last}' options and index video range [0..{len(self.videos_to_dl)-1}]")
                 
            
        for video in self.list_videos:
            if (_id:=video.get('id')):
                video['id'] = _id.replace('_', '')                
            if self._check_to_dl(video): self.videos_to_dl.append(video)  
            
            
        for vid in self.videos_to_dl:
            if not vid.get('filesize'): vid.update({'filesize' : 0})
        #     if (ie_key:=(vid.get('ie_key'))):
        #         if not self.ytdl.params['dict_videos_to_dl'].get(ie_key):
        #             self.ytdl.params['dict_videos_to_dl'][ie_key] = []
        #         self.ytdl.params['dict_videos_to_dl'][ie_key].append(vid['url'])
        
        #self.logger.info(f"DICT EXTRACTORS TO USE SO FAR AFTER CHECKING ALREADY DL BEFORE WORKER INITS: \n {self.ytdl.params['dict_videos_to_dl']}")
        
            
        if self.args.byfilesize:
            self.videos_to_dl = sorted(self.videos_to_dl, key=itemgetter('filesize'), reverse=True)
            
        self.logger.debug(f"[get_videos_to_dl] videos to dl: \n{self.videos_to_dl}")
        
                
        #preparo queue de videos para workers init
        for i, video in enumerate(self.videos_to_dl):
            self.queue_vid.put((i, video))             
        for _ in range(self.init_nworkers-1):
            self.queue_vid.put((-1, "KILL"))        
        self.queue_vid.put((-1, "KILLANDCLEAN"))
        
        
        self.totalbytes2dl = sum([vid.get('filesize') for vid in self.videos_to_dl])
        self.logger.info(f"Videos to DL not in local storage: [{len(self.videos_to_dl)}] Total size: [{naturalsize(self.totalbytes2dl)}]")       
        
        
        self.logger.debug(f"Queue content for workers inits: \n {list(self.queue_vid.queue)}")
        
        return self.videos_to_dl
    

    def worker_init(self, i):
        #worker que lanza la creación de los objetos VideoDownloaders, uno por video
        
        self.logger.debug(f"worker_init[{i}]: launched")

        try:
        
            while True:
                
                num, vid = self.queue_vid.get(block=True)    
                
                if vid == "KILL":
                    self.logger.debug(f"worker_init[{i}]: finds KILL")
                    break
                elif vid == "KILLANDCLEAN":
                    self.logger.debug(f"worker_init[{i}]: finds KILLANDCLEAN")
                    
                    #wait for the others workers_init to finish
                    while (self.count_init < (self.init_nworkers - 1)):
                        time.sleep(1)
                    
                    for _ in range(self.workers - 1): self.queue_run.put_nowait("KILL")
                    
                    self.queue_run.put_nowait("KILLANDCLEAN")
                    
                    if self.list_dl:
                        info_dl = {"entries": [dl.info_dict for dl in self.list_dl]}
                        
                        if info_dl:
                            #self.logger.info(info_dl)                   
                            _info_dl_str = demjson.encode(info_dl)
                            with open(Path(Path.home(), f"Projects/common/logs/lastsession.json"), "w") as f:                        
                                f.write(_info_dl_str)
                    else:
                        self.stop_tk = True
                        
                    #kill any zombie process from extractors - firefox, geckodriver, etc
                    kill_processes(self.logger)   
                    
                    break
                
                else: 
                    
                    _strvid = f"[{vid['id']}][{vid['title']}]" if not vid.get('_type') else f"[{vid['url']}]"   
                
                    self.logger.info(f"worker_init[{i}]: [{num}] {_strvid}")       
                    
                    try: 
                        
                        
                        if "url" in vid.get('_type', ''):
                            
                            try:                                    
                                info = None
                                info = self.ytdl.extract_info(vid['url'], download=False) 
                            
                            except Exception as e:
                                lines = traceback.format_exception(*sys.exc_info())
                                self.list_initnok.append((vid, f"{str(e)}"))
                                self.logger.error(f"worker_init[{i}]: DL constructor failed for {vid} - {str(e)}\n{'!!'.join(lines)}")
                                if 'unsupported url' in str(e).lower():
                                    
                                    self.list_unsup_urls.append(vid)
                                    
                                elif any(_ in str(e).lower() for _ in ['not found', '404', 'flagged', '403', 'suspended', 'unavailable']): 
                                    
                                    self.list_notvalid_urls.append(vid)
                                    
                                else: self.list_urls_to_check.append((vid, str(e)))
                                    
                                continue
                            
                                
                            if info:
                                
                                
                                
                                if info.get('_type') == 'playlist':
                                    info = info['entries'][0]
                                    info['aldl_checked'] = False
                                
                                else:
                            
                                    info['aldl_checked'] = vid.get('aldl_checked')
                            
                                
                                if (_id:=info.get('id')):
                                    
                                    info['id'] = _id.replace('_', '')
                                    
                                self.logger.debug(f"worker_init[{i}] {vid} \n{info}")
                                
                                if not info['aldl_checked']: 
                                
                                    if not self._check_to_dl(info):
                                        
                                        self.logger.info(f"worker_init[{i}]: [{info.get('id','')}][{info.get('title','')}] already DL")
                                        self.videos_to_dl.remove(vid)
                                        #as we have already used extractor, the video would have been removed from self.ytdl.params['dict_videos_to_dl'
                                        if (_filesize:=vid.get('filesize',0)):
                                            self.totalbytes2dl -= _filesize
                                        
                                        continue
                            else:                                        
                                raise Exception("no info dict")
                        
                        else: info = vid
                                
                        dl = VideoDownloader(info, self.ytdl, self.parts, self.args.path, self.args.aria2c)
                                
                        if dl and not dl.info_dl.get('status') == "error":
                            
                            _url = vid['url']
                            _filesize = vid.get('filesize', 0)
                            
                            for video in self.videos_to_dl:
                                if video['url'] == _url:                                    
                                    video.update({'filesize': dl.info_dl.get('filesize',0), 'id': dl.info_dl['id'], 'title': dl.info_dl['title'], 'filename': dl.info_dl['filename'], 'status': dl.info_dl['status']})
                                    if video['filesize']: self.totalbytes2dl = self.totalbytes2dl - _filesize + video['filesize']
                                    break
                            
                            for video in self.list_videos:
                                if video['url'] == _url:
                                    video.update({'filesize': dl.info_dl['filesize'], 'id': dl.info_dl['id'], 'title': dl.info_dl['title']})
                                    break

                        
                            self.list_dl.append(dl)
                            
                            if dl.info_dl['status'] in ("init_manipulating", "done"):
                                self.queue_manip.put_nowait(dl)
                                self.logger.info(f"worker_init[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : video parts DL, lets create it [{num} out of {len(self.videos_to_dl)}] : progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                            else:
                                self.queue_run.put_nowait(dl)
                                self.logger.info(f"worker_init[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : [{num} out of {len(self.videos_to_dl)}] : progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                                        
                                        
                        else:                                         
                            raise Exception("no DL init")
                    
                        
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        self.list_initnok.append((vid, f"Error:{repr(e)}"))
                        self.logger.error(f"worker_init[{i}]: DL constructor failed for {vid} - Error:{repr(e)} \n{'!!'.join(lines)}")
                        
                        if info: self.list_urls_to_check.append((info,str(e)))
                        else: self.list_urls_to_check.append((vid,str(e)))
                        
                        continue       
        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.error(f"worker_init[{i}]: Error:{repr(e)} \n{'!!'.join(lines)}")
                    
        finally:
            with self.lock:
                self.count_init += 1                
            self.logger.debug(f"worker_init[{i}]: BYE")
    
    
    async def worker_run(self, i):
        
        self.logger.debug(f"worker_run[{i}]: launched")       
        await asyncio.sleep(0)
        
        try:
            
            while True:
            
                video_dl = await self.queue_run.get()
                self.logger.debug(f"worker_run[{i}]: get for a video_DL")
                await asyncio.sleep(0)
                
                if video_dl == "KILL":
                    self.logger.debug(f"worker_run[{i}]: get KILL, bye")                    
                    await asyncio.sleep(0)
                    break
                
                elif video_dl == "KILLANDCLEAN":
                    self.logger.debug(f"worker_run[{i}]: get KILLANDCLEAN, bye")  
                    #nworkers = min(self.workers,len(self.videos_to_dl))
                    nworkers = self.workers
                    while (self.count_run < (nworkers - 1)):
                        await asyncio.sleep(1)
                    
                    for _ in range(nworkers):
                        self.queue_manip.put_nowait("KILL") 
                    await asyncio.sleep(0)
                    
                    break
                
                else:
                    self.logger.debug(f"worker_run[{i}]: get dl: {type(video_dl)}")
                    self.logger.debug(f"worker_run[{i}]: start to dl {video_dl.info_dl['title']}")
                    
                    task_run = asyncio.create_task(video_dl.run_dl())
                    #task_run.set_name(f"worker_run[{i}][{video_dl.info_dict['title']}]")
                    await asyncio.sleep(0)
                    done, pending = await asyncio.wait([task_run])
                    
                    for d in done:
                        try:
                            d.result()
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            self.logger.error(f"worker_run[{i}][{video_dl.info_dict['title']}]: Error with video DL:\n{'!!'.join(lines)}")
                    
                    if video_dl.info_dl['status'] == "init_manipulating": self.queue_manip.put_nowait(video_dl)
                    else: 
                        self.logger.error(f"worker_run[{i}][{video_dl.info_dict['title']}]: error when dl video, can't go por manipulation")
                        
                    await asyncio.sleep(0)
                                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.error(f"worker_run[{i}]: Error: {repr(e)}\n{'!!'.join(lines)}")
        
        finally:
            self.count_run += 1 
            self.logger.debug(f"worker_run[{i}]: BYE")
        
    
    async def worker_manip(self, i):
       
        self.logger.debug(f"worker_manip[{i}]: launched")       
        await asyncio.sleep(0)

        try:
            
            while True:
            
                
                video_dl = await self.queue_manip.get()                              
                self.logger.debug(f"worker_manip[{i}]: get for a video_DL")
                await asyncio.sleep(0)
                
                if video_dl == "KILL":
                    self.logger.debug(f"worker_manip[{i}]: get KILL, bye")                    
                    #await asyncio.sleep(0)
                    break                

                else:
                    self.logger.debug(f"worker_manip[{i}]: start to manip {video_dl.info_dl['title']}")
                    task_run_manip = asyncio.create_task(video_dl.run_manip(), name=f"worker_manip[{i}][{video_dl.info_dict['title']}]")      
                    done, pending = await asyncio.wait([task_run_manip])
                    
                    for d in done:
                        try:
                            d.result()
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            self.logger.error(f"worker_manip[{i}][{video_dl.info_dict['title']}]: Error with video DL:\n{'!!'.join(lines)}")
         
                        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            self.logger.error(f"worker_manip[{i}]: Error: {repr(e)}\n{'!!'.join(lines)}")
        finally:
            self.count_manip += 1 
            self.logger.debug(f"worker_manip[{i}]: BYE")       

 
    async def async_ex(self, args_tk):
    
        self.queue_run = asyncio.Queue()
        self.queue_manip = asyncio.Queue()

        
        self.logger.info(f"MAX WORKERS [{self.workers}]")
        
        try:        
          
 
            tasks_run = []
            task_tk = []
            tasks_manip = []

            tasks_init = [asyncio.create_task(asyncio.to_thread(self.worker_init, i)) for i in range(self.init_nworkers)]
                            
            if not self.args.nodl:
            
                task_tk = [asyncio.create_task(self.run_tk(args_tk))] 
                tasks_run = [asyncio.create_task(self.worker_run(i)) for i in range(self.workers)]                  
                tasks_manip = [asyncio.create_task(self.worker_manip(i)) for i in range(self.workers)]
                
            done, _ = await asyncio.wait(tasks_init + task_tk + tasks_run + tasks_manip)
            
            for d in done:
                try:
                    d.result()
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())                
                    self.logger.error(f"[async_ex] {repr(e)}\n{'!!'.join(lines)}")

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            self.logger.error(f"[async_ex] {repr(e)}\n{'!!'.join(lines)}")
    
        asyncio.get_running_loop().stop()      
        

    def get_results_info(self):
        
                
        if self.args.byfilesize:
            self.videos_to_dl = sorted(self.videos_to_dl, key=itemgetter('filesize'), reverse=True)
        
        #requests inicial de videos
        _videos_dl = self.list_dl
        
        #tras descartar los que ya están en local
        _videos_2dl = self.videos_to_dl
        
        #will store videos already DL        
        _videos_aldl = self.list_initaldl   
        
        #will store videos nok during init     
        _videos_initnok = self.list_initnok
        
        _videos_url_notsupported = self.list_unsup_urls
        
        _videos_url_notvalid = self.list_notvalid_urls
        
        _videos_url_tocheck = self.list_urls_to_check
          
            
        videos_okdl = []
        videos_kodl = []
        videos_kodl_str = []
        if _videos_2dl and not self.args.nodl:

            for _video in _videos_2dl:
                _vid = _video.get('filename')
                if not _vid or not _vid.exists():
                    videos_kodl.append(f"[{_video.get('id')}][{_video.get('title')}][{_video.get('url')}]") 
                    videos_kodl_str.append(f"{_video['url']}")
                else: videos_okdl.append(f"[{_video['id']}][{_video['title']}]")
        
        videos_initnok = []
        videos_initnok_str = []        
        
        if _videos_initnok:
            
            for vid in _videos_initnok:
                _id = vid[0].get('id')
                _title = vid[0].get('title')
                if _id and _title:
                    item = f"[{_id}][{_title}]"
                else: item = vid[0].get('url')
                videos_initnok.append(item) 
                videos_initnok_str.append(f"{vid[0].get('url')}")        
        
        videos_initok = []
        videos_initok_str = []
        
        for vid in _videos_dl:
            _id = vid.info_dict.get('id') 
            _title = vid.info_dict.get('title') 
            if _id and _title:
                item = f"[{_id}][{_title}]"
            else: item = vid.info_dict.get('url')
            videos_initok.append(item) 
            videos_initok_str.append(f"{vid.info_dict.get('url')}") 
            
            
        self.print_list_videos()
        
        
        self.logger.info(f"******************************************************")
        self.logger.info(f"******************************************************")
        self.logger.info(f"*********** FINAL SUMMARY ****************************")
        self.logger.info(f"******************************************************")
        self.logger.info(f"******************************************************")
        self.logger.info(f"")
        self.logger.info(f"Request to DL: [{len(self.list_videos)}]")
        self.logger.info(f"") 
        self.logger.info(f"         Already DL: [{len(_videos_aldl)}]")
        self.logger.info(f"         Videos to DL: [{len(_videos_2dl)}]")
        self.logger.info(f"")                
        self.logger.info(f"                 OK init DL: [{len(videos_initok)}]")
        self.logger.info(f"                 OK DL: [{len(videos_okdl)}]")
        self.logger.info(f"                 ERROR DL: [{len(videos_kodl)}]")
        self.logger.info(f"                     ERROR init DL: [{len(videos_initnok)}]")
        self.logger.info(f"                         UNSUP URLS: [{len(_videos_url_notsupported)}]")
        self.logger.info(f"                         NOTVALID URLS: [{len(_videos_url_notvalid)}]")
        self.logger.info(f"                         TO CHECK URLS: [{len(_videos_url_tocheck)}]")
        self.logger.info(f"")
        self.logger.info(f"*****************************************************")
        self.logger.info(f"*****************************************************")
        self.logger.info(f"*****************************************************")
        self.logger.info(f"*****************************************************")
        
                
        
        self.logger.info(f"******* VIDEO RESULT LISTS **************************")        
        self.logger.info(f"Videos ERROR INIT DL: {videos_initnok} \n [-u {' -u '.join(videos_initnok_str)}]")
        self.logger.info(f"Videos ERROR DL: {videos_kodl} \n[-u {' -u '.join(videos_kodl_str)}]")
        self.logger.info(f"Videos ALREADY DL: {_videos_aldl}") 
        self.logger.info(f"Videos OK INIT DL: {videos_initok}")
        self.logger.info(f"Videos DL: {videos_okdl}")
        
        self.logger.info(f"Unsupported URLS: \n{_videos_url_notsupported}")
        self.logger.info(f"Not Valid URLS: \n{_videos_url_notvalid}")
        self.logger.info(f"To check URLS: \n{_videos_url_tocheck}")
        
        #self.logger.info(f"DICT EXTRACTORS DL: \n{self.ytdl.params['dict_videos_to_dl']}")
        
        return ({'videos_req': self.list_videos, 'videos_2_dl': _videos_2dl, 'videos_al_dl': _videos_aldl, 'videos_ok_dl': videos_okdl, 'videos_error_init': videos_initnok_str, 'videos_error_dl': videos_kodl_str})

    def print_list_videos(self):
    
        list_videos = self.list_videos
        
        list_videos2dl = self.videos_to_dl    
        
            
        list_videos_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0)), 'url': (vid.get('url','')[:100])} for vid in list_videos]
        list_videos2dl_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0)), 'url': (vid.get('url','')[:100])} for vid in list_videos2dl]
        
      
                
        self.logger.info(f"RESULT: Total videos [{(_tv:=len(list_videos))}] To DL [{(_tv2dl:=len(list_videos2dl))}] Already DL [{(_tval:=len(self.list_initaldl))}]")
        
        #pd.set_option("max_rows", _tv) 
        
        df_tv = pd.DataFrame(list_videos_str)
        tab_tv = tabulate(df_tv, showindex=True, headers=df_tv.columns)      
        #df_tv = pd.DataFrame(list_videos_str)
        
        
        #self.logger.info(f"Total videos: [{_tv}]\n\n\n{tab_tv}\n\n\n")
        
            
        df_v2dl = pd.DataFrame(list_videos2dl_str)
        tab_v2dl = tabulate(df_v2dl, showindex=True, headers=df_v2dl.columns)
                
        self.logger.info(f"Videos to DL: [{_tv2dl}]\n\n\n{tab_v2dl}\n\n\n")
                
        self.logger.info(f"Total bytes to DL: [{naturalsize(self.totalbytes2dl)}]")
        


@Timer(name="decorator")
def main():
    
    init_logging()
    logger = logging.getLogger("async_all")
    patch_http_connection_pool(maxsize=1000)
    patch_https_connection_pool(maxsize=1000)
    
    args = init_argparser()
    
    logger.info(f"Hi, lets dl!\n{args}")
    
    asyncDL = AsyncDL(args)    
    
    
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut = [ex.submit(asyncDL.get_videos_cached), ex.submit(asyncDL.get_list_videos)]
        wait_for_futures(fut)

    asyncDL.get_videos_to_dl()    
    
    asyncDL.print_list_videos()      
       
    if asyncDL.videos_to_dl:    
            
        try:
            if args.aria2c:
                subprocess.run(["aria2c","--enable-rpc","--daemon"])
            args_tk = init_tk()        
            aiorun.run(asyncDL.async_ex(args_tk), use_uvloop=True) 
                
        except Exception as e:
            logger.error(str(e), exc_info=True)
            
        finally:
            kill_processes(logger)            


    
    res = asyncDL.get_results_info()     
 
    if args.caplinks:
        
        shutil.copy("/Users/antoniotorres/Projects/common/logs/captured_links.txt", "/Users/antoniotorres/Projects/common/logs/prev_captured_links.txt")
        with open("/Users/antoniotorres/Projects/common/logs/captured_links.txt", "w") as file:
        
            session_errors = res['videos_error_dl'] + res['videos_error_init']
            for _video in set(session_errors):
                line = _video + "\n"
                file.write(line)  

if __name__ == "__main__":
    
    main()

    
