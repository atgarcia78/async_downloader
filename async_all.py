#!/usr/bin/env python


from queue import Queue
import logging
import sys
import traceback
import json
import tkinter as tk
import asyncio
import aiorun 
from pathlib import Path
import pandas as pd
from tabulate import tabulate
import humanfriendly
import re
import time


from common_utils import ( 
    init_logging,
    init_ytdl,
    init_tk,
    init_argparser,
    patch_http_connection_pool,
    patch_https_connection_pool,
    naturalsize,
    shorter_str
)

from concurrent.futures import (
    ThreadPoolExecutor,
    wait
)

from youtube_dl.utils import sanitize_filename

from codetiming import Timer

from datetime import datetime

from operator import itemgetter

from videodownloader import VideoDownloader    

from collections import defaultdict 

import httpx         

class AsyncDL():

    def __init__(self, args):
    
        
        self.logger = logging.getLogger("asyncDL")
               
        self.queue_vid = Queue()
        
        self.list_initnok = []
        self.list_initaldl = []
        self.list_dl = []
        self.files_cached = dict()
        self.videos_to_dl = []        
        
        self.args = args
        self.parts = self.args.p
        self.workers = self.args.w    

        self.ytdl = init_ytdl(self.args)
       
        self.stop_tk = False
        self.count_init = 0
        self.count_run = 0        
        self.count_manip = 0 
        
        self.time_now = datetime.now()
    
    async def wait_time(self, n):
        _timer = httpx._utils.Timer()
        await _timer.async_start()
        while True:
            _t = await _timer.async_elapsed()
            if _t > n: break
            else: await asyncio.sleep(0)
    
    def get_videos_cached(self):        
        
        _repeated = []
        
        last_res = Path(Path.home(),"Projects/common/logs/files_cached.json")
        
        if self.args.nodlcaching and last_res.exists():
            
            with open(last_res,"r") as f:
                self.files_cached = json.load(f)
                
            self.logger.info(f"Total cached videos: [{len(self.files_cached)}]")
        
        else:  
        
            list_folders = [Path(Path.home(), "testing"), Path("/Volumes/Pandaext4/videos"), Path("/Volumes/T7/videos"), Path("/Volumes/Pandaext1/videos"), Path("/Volumes/DatosToni/videos"), Path("/Volumes/WD/videos")]
             
            for folder in list_folders:
                #self.logger.debug(f"[CACHED] Folder: {folder.name}")
                for file in folder.rglob('*'):
                    #self.logger.debug(f"[CACHED] \tFile: {file.name}")
                    if file.is_file() and not file.stem.startswith('.') and (file.suffix.lower() in ('.mp4', '.mkv', '.m3u8', '.zip')):

                        _res = re.findall(r'^([^_]*)_(.*)', file.stem)
                        if _res:
                            (_id, _title) = _res[0]
                            _title = sanitize_filename(_title, restricted=True)
                            _title = _title.upper()
                            _name = f"{_id}_{_title}"
                        else:
                            _name = sanitize_filename(file.stem, restricted=True).upper()

                        if not (_videopath:=self.files_cached.get(_name)): 
                            if not file.is_symlink(): self.files_cached.update({_name: str(file)})
                        else:
                            if _videopath != str(file):
                                if not file.is_symlink(): _repeated.append({'title':_name, 'indict': _videopath, 'file': str(file)})
                            
                
        
            
            self.logger.info(f"Total cached videos: [{len(self.files_cached)}]")
            
            if _repeated:
                self.logger.warning(f"Please check videos repeated: \n {_repeated}")
            
                        
            prev_res = Path(Path.home(),"Projects/common/logs/prev_files_cached.json")
            
                
            if last_res.exists():
                if prev_res.exists(): prev_res.unlink()
                last_res.rename(Path(last_res.parent,f"prev_files_cached.json"))
            
            with open(last_res,"w") as f:
                json.dump(self.files_cached,f)     
            
        return self.files_cached
               
        
    def get_list_videos(self):
        
        
        self.list_videos = []
        fileres = Path(Path.home(), f"Projects/common/logs/list_videos.json")
        filecaplinks = Path(Path.home(), f"Projects/common/logs/captured_links.txt")
        
        
        if self.args.caplinks:
            with open(filecaplinks, "r") as file:
                _content = file.read()            
                
            url_list = list(set(_content.splitlines()))
            
            self.list_videos += [{'_type': 'url', 'url': _url} for _url in url_list]
            
            with open(filecaplinks, "w") as file: 
                file.truncate()
                
                
            with open(Path(Path.home(), f"Projects/common/logs/list_videos.json"),"w") as f:
                json.dump({'entries': self.list_videos},f)
        
        
            self.logger.debug(f"[get_list_videos] list videos: \n{self.list_videos}")
        
            return self.list_videos
            
            
        
        if self.args.playlist:
 
            if self.args.lastres:                
                
                if fileres.exists():
                    try:
                        with open(Path(Path.home(), f"Projects/common/logs/list_videos.json"),"r") as f:
                            self.list_videos = (json.load(f)).get('entries')
                    except Exception as e:
                        self.logger.error("Couldnt get info form last result")
                        
                else:
                    self.logger.error("Couldnt get info form last result")
                    
            else:    
            
                if self.args.collection:

                    
                    url_pl_list = list(set(self.args.collection))
                    if len(url_pl_list) == 1: 
                        info_dict = self.ytdl.extract_info(url_pl_list[0], download=False)
                        self.logger.debug(f"[get_list_videos]: \n{info_dict.get('entries')}")
                        _url_pl = info_dict.get('entries')
                    else:
                        with ThreadPoolExecutor(max_workers=self.workers) as ex:
                            fut = [ex.submit(self.ytdl.extract_info, url_pl, download=False) for url_pl in url_pl_list]
                            done, _ = wait(fut)
                            
                        _url_pl = []
                        for d in done:
                            _url_pl += (d.result()).get('entries')
                        
                        self.logger.info(_url_pl) 
                        # for url_pl, fut in zip(url_pl_list, futures):
                        #     done, _ = wait([fut])
                        #     res = [d.result() for d in done]
                        #     _url_pl = res[0].get('entries')
                    
                            
                    #por si hay repetidos
                    items = defaultdict(list)
                    for entry in _url_pl:
                        items[entry['url']].append(entry)
                            
                    self.list_videos += [_value[0] for _value in items.values()]
                              
                                
                  

                if self.args.collection_files:
                    
                    file_list = list(set(self.args.collection_files))
                    for file in file_list:
                        with open(file, "r") as file_json:
                            info_json = json.loads(file_json.read())
                    
                        self.list_videos += list(info_json)

        else: #url no son playlist

            if self.args.lastres:
                
                if fileres.exists():
                    try:
                        with open(Path(Path.home(), f"Projects/common/logs/list_videos.json"),"r") as f:
                            self.list_videos = (json.load(f)).get('entries')
                    except Exception as e:
                        self.logger.error("Couldnt get info form last result")
                        
                else:
                    self.logger.error("Couldnt get info form last result")
            else:
            
                if self.args.collection:
                    url_list = list(set(self.args.collection))  
                    self.list_videos += [{'_type': 'url', 'url': _url} for _url in url_list]

                if self.args.collection_files:
                    def get_info_json(file):
                        with open(file, "r") as f:
                            return json.loads(f.read())
                    
                    self.list_videos += [get_info_json(file) for file in self.args.collection_files]
                    
        
        
        with open(Path(Path.home(), f"Projects/common/logs/list_videos.json"),"w") as f:
            json.dump({'entries': self.list_videos},f)
        
        
        self.logger.debug(f"[get_list_videos] list videos: \n{self.list_videos}")
        
        return self.list_videos
        

        
    def get_sublist_videos(self):
        
        if self.args.index:
            if self.args.index in range(1,len(self.videos_to_dl)):
                self.videos_to_dl = [self.videos_to_dl[self.args.index-1]]
            else:
                self.logger.error(f"index video {self.args.index} out of range [1..{len(self.videos_to_dl)}]")
                sys.exit(127)
                
        if self.args.first and self.args.last:
            if (self.args.first in range(1,len(self.videos_to_dl))) and (self.args.last in range(1,len(self.videos_to_dl))) and (self.args.first <= self.args.last):
                self.videos_to_dl = self.videos_to_dl[self.args.first-1:self.args.last]       

        
        if (self.args.maxsize or self.args.minsize):
            
            if self.args.maxsize: maxbytes = humanfriendly.parse_size(self.args.maxsize)            
            else: maxbytes = 10**12
            if self.args.minsize: minbytes = humanfriendly.parse_size(self.args.minsize)
            else: minbytes = -1
            
            new_list = []
            for vid in self.videos_to_dl:
                if (_size:=vid.get('filesize')) > 0:
                    if (minbytes < (_size) < maxbytes):
                        new_list.append(vid)
                        self.logger.debug(f"Video with right size: minsize[{minbytes}] size video [{_size}] maxsize[{maxbytes}]: {vid}")
                    
                    else:
                        self.logger.debug(f"Video removed due to size: minsize[{minbytes}] size video [{_size}] maxsize[{maxbytes}]: {vid}")   
                else:
                    new_list.append(vid)
                    self.logger.debug(f"Video with unkmown size[0]]: {vid}")              
                
            self.videos_to_dl = new_list
            
        if self.args.col_names:
            
                        
            new_list = []
            for vid in self.videos_to_dl:
                if any(name in vid['title'] for name in self.args.col_names):                
                    new_list.append(vid)
                    self.logger.debug(f"Video included with title[{vid['title']}] including name[{self.args.col_names}]")
                else:
                    self.logger.debug(f"Video removed with title[{vid['title']}] not including name[{self.args.col_names}]")
                    
            self.videos_to_dl = new_list    
            
        self.logger.debug(f"[get_sub_list_videos] sub list videos to dl: \n{self.videos_to_dl}")
        return(self.videos_to_dl)
        
        
    def get_videos_to_dl(self):        
        
        
        for video in self.list_videos:
            if (_id := video.get('id') ) and (_title := video.get('title')):               
            #if (_title := video.get('title') or video.get('video_title')):
                _title = sanitize_filename(_title, restricted=True).upper()
                _id = _id[:8] if len(_id) > 8 else _id
                vid_name = f"{_id}_{_title}"
                

                if (vid_path:=self.files_cached.get(vid_name)):
                    #self.list_initaldl.append({'title': vid_name, 'path': vid_path})
                    self.list_initaldl.append({'title': vid_name, 'path': vid_path})
                    self.logger.debug(f"[{vid_name}]: already DL")
                    if self.args.path:
                        _filepath = Path(self.args.path)
                    else:
                        _filepath = Path(Path.home(),"testing",self.time_now.strftime('%Y%m%d'))
                    _filepath.mkdir(parents=True, exist_ok=True)
                    file_aldl = Path(_filepath, Path(vid_path).name)
                    if file_aldl not in _filepath.iterdir():
                        file_aldl.symlink_to(Path(vid_path))
                    
                else: 
                    self.videos_to_dl.append(video)
            else: self.videos_to_dl.append(video)
            
        for vid in self.videos_to_dl:
            if not vid.get('filesize'): vid.update({'filesize' : 0})
            
        if self.args.byfilesize:
            self.videos_to_dl = sorted(self.videos_to_dl, key=itemgetter('filesize'), reverse=True)
            
        self.logger.debug(f"[get_videos_to_dl] videos to dl: \n{self.videos_to_dl}")
        
        self.get_sublist_videos()
        
       
        for i,video in enumerate(self.videos_to_dl):
            self.queue_vid.put((i, video))
            
        self.totalbytes2dl = sum([vid.get('filesize') for vid in self.videos_to_dl])
        self.logger.info(f"Videos to DL not in local storage: [{len(self.videos_to_dl)}] Total size: [{naturalsize(self.totalbytes2dl)}")       
        
        
        #nworkers = min(self.workers, len(self.videos_to_dl))
        nworkers = self.workers
        for _ in range(nworkers-1):
            self.queue_vid.put((-1, "KILL"))
        self.queue_vid.put((-1, "KILLANDCLEAN"))
        
        self.logger.debug(f"Queue content for running: \n {list(self.queue_vid.queue)}")
        
        return self.videos_to_dl
    
    async def run_tk(self, args_tk, interval):
        '''
        Run a tkinter app in an asyncio event loop.
        '''
        
        root, text0, text4, text1, text2, root2, text3 = args_tk
        count = 0
        while (not self.list_dl and not self.stop_tk):
            
            await self.wait_time(interval)
            count += 1
            if count == 10:
                count = 0
                self.logger.debug("[RUN_TK] Waiting for dl")
        
        self.logger.debug(f"[RUN_TK] End waiting. Signal stop_tk[{self.stop_tk}]")
        
        try:

                 
            while not self.stop_tk:
                
                root.update()
                root2.update()
                
                res = set([dl.info_dl['status'] for dl in self.list_dl])
                
                if not res:
                    pass
                
                else:     
                    _res = sorted(list(res))
                    if (_res == ["done", "error"] or _res == ["error"] or _res == ["done"]) and (self.count_init == self.workers):                        
                            break
                    else:
                        text3.delete(1.0,tk.END)
                        text3.insert(tk.END, naturalsize(self.totalbytes2dl))
                        text0.delete(1.0, tk.END)
                        text4.delete(1.0, tk.END)
                        text1.delete(1.0, tk.END)
                        text2.delete(1.0, tk.END)
                        list_downloading = []
                        list_manip = []    
                        for dl in self.list_dl:
                            mens = dl.print_hookup()
                            if dl.info_dl['status'] in ["init"]:
                                text0.insert(tk.END, mens)
                            if dl.info_dl['status'] in ["init_manipulating"]:
                                text4.insert(tk.END, mens)
                            if dl.info_dl['status'] in ["downloading"]:
                                list_downloading.append(mens)
                            if dl.info_dl['status'] in ["creating"]: 
                                list_manip.append(mens)                       
                                
                            if dl.info_dl['status'] in ["done", "error"]:
                                text2.insert(tk.END,mens)
                                         
                        if list_downloading:
                            text1.insert(tk.END, "\n\n-------DOWNLOADING VIDEO------------\n\n")
                            text1.insert(tk.END, ''.join(list_downloading))
                            
                        if list_manip:
                            text1.insert(tk.END, "\n\n-------CREATING VIDEO------------\n\n")
                            text1.insert(tk.END, ''.join(list_manip))
                                         
                        
                await self.wait_time(interval)
       
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            self.logger.error(f"[run_tk]: error\n{'!!'.join(lines)}")
        
        self.logger.debug("[RUN_TK] BYE") 


    def worker_init_dl(self, i):
        #worker que lanza los Downloaders, uno por video
        
        self.logger.debug(f"worker_init_dl[{i}]: launched")

        try:
        
            while True:
                
                num, vid = self.queue_vid.get()
                                
                self.logger.debug(f"worker_init_dl[{i}]: [{num}]{vid}")
                if vid == "KILL":
                    self.logger.debug(f"worker_init_dl[{i}]: finds KILL")
                    break
                elif vid == "KILLANDCLEAN":
                    self.logger.debug(f"worker_init_dl[{i}]: finds KILLANDCLEAN")
                    nworkers = self.workers
                    #wait for the others workers_init to finish
                    while (self.count_init < (nworkers - 1)):
                        time.sleep(1)
                    
                    for _ in range(nworkers - 1): self.queue_run.put_nowait("KILL")
                    
                    self.queue_run.put_nowait("KILLANDCLEAN")
                    
                    if self.list_dl:
                        info_dl = [dl.info_dict for dl in self.list_dl]
                        if info_dl:                   
                            with open(Path(Path.home(), f"Projects/common/logs/lastsession.json"), "w") as f:                        
                                json.dump(info_dl, f)
                    else:
                        self.stop_tk = True                
                    
                    break
                
                else:        
                    
                    if self.args.nodl and vid.get('filesize'): continue
                    
                    try: 
                        _filesize = vid.get('filesize', 0)
                        if not vid.get('format_id') and not vid.get('requested_formats'):
                        
                            info_dict = None    
                            
                            count = 0
                            while(count < 3):
                                try:
                                    info = None
                                    info = self.ytdl.extract_info(vid['url'], download=False,process=False)   
                                except Exception as e:
                                    pass
                                if info: break
                                else:
                                    time.sleep(2)
                                    count += 1
                                    
                            if info:                        
                                self.logger.debug(f"worker_init_dl[{i}] {info}")
                                info_dict = info
                                
                                if (_id := info.get('id') ) and (_title := info.get('title') ):               
                
                                    _title = sanitize_filename(_title, restricted=True).upper()
                                    _id = _id[:8] if len(_id) > 8 else _id
                                    _name = f"{_id}_{_title}"
                                    self.logger.debug(f"Look in dict: {_name}")
                                    if (vid_path:=self.files_cached.get(_name)):
                                        self.list_initaldl.append({'title': _name, 'path': vid_path})
                                        self.logger.debug(f"[{_name}] : already DL")                                   
                                        if self.args.path:
                                            _filepath = Path(self.args.path)
                                        else:
                                            _filepath = Path(Path.home(),"testing",self.time_now.strftime('%Y%m%d'))
                                        _filepath.mkdir(parents=True, exist_ok=True)
                                        file_aldl = Path(_filepath, Path(vid_path).name)
                                        if file_aldl not in _filepath.iterdir():
                                            file_aldl.symlink_to(Path(vid_path))
                                        self.logger.debug(f"worker_init_dl[{i}] {_name} already DL")
                                        self.videos_to_dl.remove(vid)                            
                                        continue
    
                                if not info.get('format_id') and not info.get('requested_formats'):
                                    
                                    info_dict = self.ytdl.process_ie_result(info,download=False)
                        else:
                            info_dict = vid
                        
                        if info_dict:
                                
                            self.logger.debug(f"worker_init_dl[{i}] {info_dict}")
                            dl = VideoDownloader(info_dict, self.ytdl, self.parts, self.args.path)
                                    
                            if dl:
                                
                                _url = vid['url']
                                for video in self.videos_to_dl:
                                    if video['url'] == _url:                                    
                                        video.update({'filesize': dl.info_dl['filesize'], 'id': dl.info_dl['id'], 'title': dl.info_dl['title'], 'filename': dl.info_dl['filename'], 'status': dl.info_dl['status']})
                                        if _filesize == 0: self.totalbytes2dl += dl.info_dl['filesize']
                                        break
                                for video in self.list_videos:
                                    if video['url'] == _url:
                                        video.update({'filesize': dl.info_dl['filesize'], 'id': dl.info_dl['id'], 'title': dl.info_dl['title']})
                                        break
                                    
                                if self.args.maxsize or self.args.minsize:
                
                                    if self.args.maxsize: maxbytes = humanfriendly.parse_size(self.args.maxsize)            
                                    else: maxbytes = 10**12
                                    if self.args.minsize: minbytes = humanfriendly.parse_size(self.args.minsize)
                                    else: minbytes = -1 
                                    if (minbytes < vid['filesize'] < maxbytes):                            
                                
                                        self.list_dl.append(dl)
                                        
                                        if dl.info_dl['status'] == "init_manipulating":
                                            self.queue_manip.put_nowait(dl)
                                            self.logger.info(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : video parts DL, lets create it [{num} out of {len(self.videos_to_dl)}] : {minbytes} < size[{dl.info_dl['filesize']}] < {maxbytes} progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                                        else:
                                            self.queue_run.put_nowait(dl)
                                            self.logger.debug(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : [{num} out of {len(self.videos_to_dl)}] : {minbytes} < size[{dl.info_dl['filesize']}] < {maxbytes} progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                                        
                                        
                                    else:
                                        self.logger.debug(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : [{num} out of {len(self.videos_to_dl)}] : but not with requested size: NOT {minbytes} < size[{dl.info_dl['filesize']}] < {maxbytes}")
                                        self.videos_to_dl.remove(vid)
                                else:
                                    self.list_dl.append(dl)
                                    
                                    if dl.info_dl['status'] == "init_manipulating":
                                        self.queue_manip.put_nowait(dl)
                                        self.logger.info(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : video parts DL, lets create it [{num} out of {len(self.videos_to_dl)}] : progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                                    else:
                                        self.queue_run.put_nowait(dl)
                                        self.logger.info(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : [{num} out of {len(self.videos_to_dl)}] : progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                                    
                                        
                            else:                                         
                                raise Exception("no DL init")
                        else:                                        
                            raise Exception("no info dict")
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        self.list_initnok.append((vid, f"Error:{repr(e)}"))
                        self.logger.error(f"worker_init_dl[{i}]: DL constructor failed for {vid} - Error:{repr(e)} \n{'!!'.join(lines)}")       
                
        finally:
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
                    task_run.set_name(f"worker_run[{i}][{video_dl.info_dict['title']}]")
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

            
    

    
    async def async_ex(self, args_tk, interval):
    
        self.queue_run = asyncio.Queue()
        self.queue_manip = asyncio.Queue()

        #nworkers = min(self.workers,len(self.videos_to_dl))
        nworkers = self.workers
        self.logger.info(f"MAX WORKERS [{nworkers}]")
        
        try:        
            #loop = asyncio.get_event_loop()
                   
            tasks_run = []
            task_tk = []
            tasks_manip = []

            
            tasks_init = [asyncio.create_task(asyncio.to_thread(self.worker_init_dl, i)) for i in range(nworkers)]
                            
            if not self.args.nodl:
            
                task_tk = asyncio.create_task(self.run_tk(args_tk, interval)) 
                tasks_run = [asyncio.create_task(self.worker_run(i)) for i in range(nworkers)]                  
                tasks_manip = [asyncio.create_task(self.worker_manip(i)) for i in range(nworkers)]
                
            done, _ = await asyncio.wait(tasks_init + [task_tk] + tasks_run + tasks_manip)
            
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
        
        _videos_dl = self.list_dl
        _videos_2dl = self.videos_to_dl
        #will store videos nok during init     
        _videos_initnok = self.list_initnok
        #will store videos already DL        
        _videos_aldl = self.list_initaldl     
            
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
        
        
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        self.logger.info(f"*********** FINAL SUMMARY ***********")
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        self.logger.info(f"")
        self.logger.info(f"Init request: Request to DL: [{len(self.list_videos)}]")
        self.logger.info(f"              Already DL: [{len(_videos_aldl)}]")
        self.logger.info(f"")
        self.logger.info(f"Videos to DL: [{len(_videos_2dl)}]")
        self.logger.info(f"              ERROR init DL: [{len(videos_initnok)}]")
        self.logger.info(f"              OK init DL: [{len(videos_initok)}]")
        self.logger.info(f"              OK DL: [{len(videos_okdl)}]")
        self.logger.info(f"              ERROR DL: [{len(videos_kodl)}]")
        self.logger.info(f"")
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        
                
        
        self.logger.info(f"******* VIDEO RESULT LISTS *******")        
        self.logger.info(f"Videos ERROR INIT DL: {videos_initnok} \n [-u {' -u '.join(videos_initnok_str)}]")
        self.logger.info(f"Videos ERROR DL: {videos_kodl} \n[-u {' -u '.join(videos_kodl_str)}]")
        self.logger.info(f"Videos ALREADY DL: {_videos_aldl}") 
        self.logger.info(f"Videos OK INIT DL: {videos_initok}")
        self.logger.info(f"Videos DL: {videos_okdl}")
        
        return ({'videos_req': self.list_videos, 'videos_2_dl': _videos_2dl, 'videos_al_dl': _videos_aldl, 'videos_ok_dl': videos_okdl, 'videos_error_init': videos_initnok, 'videos_error_dl': videos_kodl})

    def print_list_videos(self):
    
        list_videos = self.list_videos
        
        list_videos2dl = self.videos_to_dl    
        
            
        list_videos_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0)), 'url': shorter_str(vid.get('url'), 50)} for vid in list_videos]
        list_videos2dl_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0)), 'url': shorter_str(vid.get('url'), 50)} for vid in list_videos2dl]
        
        # list_videos_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0))} for vid in list_videos]
        # list_videos2dl_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0))} for vid in list_videos2dl]
        
                
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
        
        #self.logger.info(f"Already DL: [{_tval}]\n{self.list_initaldl}")
        
        if self.args.listvideos:
            
            self.logger.info(f"Total videos with CL format: \n{' -u '.join([vid['url'] for vid in list_videos_str])}")
            self.logger.info(f"Videos to DL with CL format: \n{' -u '.join([vid['url'] for vid in list_videos2dl_str])}")
            sys.exit()


@Timer(name="decorator")
def main():
    
    init_logging()
    logger = logging.getLogger("async_all")
    patch_http_connection_pool(maxsize=100)
    patch_https_connection_pool(maxsize=100)
    
    args = init_argparser()
    
    logger.info(f"Hi, lets dl!\n{args}")
    
    
    asyncDL = AsyncDL(args)    
    
    
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut = [ex.submit(asyncDL.get_videos_cached), ex.submit(asyncDL.get_list_videos)]
        

    asyncDL.get_videos_to_dl()
    
    
    asyncDL.print_list_videos()    
     
       
    if len(asyncDL.videos_to_dl) > 0:    
            
        try:

            args_tk = init_tk(len(asyncDL.videos_to_dl))        
            res = aiorun.run(asyncDL.async_ex(args_tk, 0.25), use_uvloop=True) 
                
        except Exception as e:
            logger.error(str(e), exc_info=True)
            res = 1

    
    
    res = asyncDL.get_results_info()   
  


if __name__ == "__main__":
    
    main()

    
