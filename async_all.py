#!/usr/bin/env python


from queue import Queue
import logging
import sys
import traceback
import json
import ast
import tkinter as tk
import asyncio
import aiorun 
from pathlib import Path
import pandas as pd
from tabulate import tabulate
import humanfriendly
import re
from aiotools import TaskGroup


from common_utils import ( 
    init_logging,
    init_ytdl,
    init_tk,
    get_info_dl,
    init_argparser,
    patch_http_connection_pool,
    patch_https_connection_pool,
    naturalsize,
)

from concurrent.futures import (
    ThreadPoolExecutor,
    wait
)

from youtube_dl.utils import sanitize_filename

from codetiming import Timer

from datetime import datetime

from asynclogger import AsyncLogger

from operator import itemgetter

from videodownloader import VideoDownloader              

class AsyncDL():

    def __init__(self, args):
    
        
        self.logger = logging.getLogger("asyncDL")
        self.alogger = AsyncLogger(self.logger)
        self.queue_vid = Queue()
        
        self.list_initnok = []
        self.list_initaldl = []
        self.list_dl = []
        self.files_cached = dict()
        self.videos_to_dl = []
        
        
        self.args = args
        self.parts = self.args.p
        self.workers = self.args.w
    
        dict_opts = {'format': self.args.format, 'proxy': self.args.proxy}
        if self.args.nocheckcert:
            dict_opts.update({"nocheckcertificate" : True})
        if self.args.ytdlopts:
            dict_opts.update(ast.literal_eval(self.args.ytdlopts))        
        self.ytdl = init_ytdl(dict_opts,self.args.useragent, self.args.referer)
        
        self.time_now = datetime.now()
 
        
        
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
            
            #date_file = f"{self.time_now.strftime('%Y%m%d')}_{self.time_now.strftime('%H%M%S')}"
            
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
                        self.list_videos = info_dict.get('entries')
                    else:
                        with ThreadPoolExecutor(max_workers=self.workers) as ex:
                            futures = [ex.submit(self.ytdl.extract_info, url_pl, download=False) for url_pl in url_pl_list]
                            
                        
                        for url_pl, fut in zip(url_pl_list, futures):
                            done, _ = wait([fut])
                            res = [d.result() for d in done]
                            list_url_pl = res[0].get('entries')
                            self.logger.debug(f"[get_list_videos] url_pl {url_pl} \n {list_url_pl}")
                            self.list_videos += list_url_pl


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
                    
        
       # date_file = f"{self.time_now.strftime('%Y%m%d')}_{self.time_now.strftime('%H%M%S')}"
        
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
        

        #self.logger.debug(f"Total requested videos: {len(self.videos_to_dl)}")
        #self.logger.debug(self.videos_to_dl)
        
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
                
                #vid_name = _title.upper()
                #if (vid_path:=self.files_cached.get(vid_name)):
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
        #self.logger.info(f"Videos already DL in local storage: [{len(self.list_initaldl)}] \n {self.list_initaldl}")
        self.logger.info(f"Videos to DL not in local storage: [{len(self.videos_to_dl)}] Total size: [{naturalsize(self.totalbytes2dl)}")
        
        
        
        nworkers = min(self.workers, len(self.videos_to_dl))
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
        
        try:

                 
            while True:
                
                root.update()
                root2.update()
 
                
                # video_queue = []
                # for dl in (_dl_queue:=list(self.queue_manip._queue)):
                #     if dl in ("KILL", "KILLTK", "KILLANDCLEAN"): video_queue.append(dl)
                #     else: video_queue.append(dl.info_dict['title'])
                    
                

                #if (len(video_queue) == 1) and (video_queue[0] == "KILLTK"):
                #    break
                
                
                res = set([dl.info_dl['status'] for dl in self.list_dl])
                if not res:
                    pass
                else:
                    #if ("init" in res and not "downloading" in res): 
                    #    pass
                    #if (not "init" in res and not "downloading" in res and not "creating" in res and not "init_manipulating"):                
                    _res = sorted(list(res))
                    if _res == ["done", "error"] or _res == ["error"] or _res == ["done"]:
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
                            #logger.debug(mens)
                       
                        if list_downloading:
                            text1.insert(tk.END, "\n\n-------DOWNLOADING VIDEO------------\n\n")
                            text1.insert(tk.END, ''.join(list_downloading))
                            
                        if list_manip:
                            text1.insert(tk.END, "\n\n-------CREATING VIDEO------------\n\n")
                            text1.insert(tk.END, ''.join(list_manip))
                                         
                        
                await asyncio.sleep(interval)
                #time.sleep(interval)    
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            await self.alogger.error(f"[run_tk]: error\n{'!!'.join(lines)}")
        
        self.logger.debug("RUN TK BYE")     




    def worker_init_dl(self, i):
        #worker que lanza los Downloaders, uno por video
        
        self.logger.debug(f"worker_init_dl[{i}]: launched")

        while True:
            
            num, vid = self.queue_vid.get()
            
            self.logger.debug(f"worker_init_dl[{i}]: [{num}]:{vid}")
            if vid == "KILL":
                self.logger.debug(f"worker_init_dl[{i}]: finds KILL, says bye")
                break
            elif vid == "KILLANDCLEAN":
                self.logger.debug(f"worker_init_dl[{i}]: finds KILLANDCLEAN, says bye")
                for _ in range(min(self.workers,len(self.videos_to_dl))-1):
                        self.queue_dl.put_nowait("KILL")
                self.queue_dl.put_nowait("KILLANDCLEAN")
                info_dl = [dl.info_dict for dl in self.list_dl]
                if info_dl:                   
                    #date_file = f"{self.time_now.strftime('%Y%m%d')}_{self.time_now.strftime('%H%M%S')}"
                    with open(Path(Path.home(), f"Projects/common/logs/lastsession.json"), "w") as f:                        
                        json.dump(info_dl, f)                
                break
            
            else:        
                
                if self.args.nodl and vid.get('filesize'): continue
                
                try: 
                    _filesize = vid.get('filesize', 0)
                    if not vid.get('format_id') and not vid.get('requested_formats'):
                    
                        info_dict = None    

                        
                        info = self.ytdl.extract_info(vid['url'], download=False,process=False)   
                        if info:                        
                            self.logger.debug(f"worker_init_dl[{i}] {info}")
                            #if info.get('_type') == 'url_transparent':
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
                                                        
                                #self.logger.debug(f"protocol: {protocol}")
                                #self.logger.debug(final_dict)
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
                                    video.update({'filesize': dl.info_dl['filesize'], 'id': dl.info_dl['videoid'], 'title': dl.info_dl['title'], 'filename': dl.info_dl['filename'], 'status': dl.info_dl['status']})
                                    if _filesize == 0: self.totalbytes2dl += dl.info_dl['filesize']
                                    break
                            for video in self.list_videos:
                                if video['url'] == _url:
                                    video.update({'filesize': dl.info_dl['filesize'], 'id': dl.info_dl['videoid'], 'title': dl.info_dl['title']})
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
                                        self.queue_dl.put_nowait(dl)
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
                                    self.queue_dl.put_nowait(dl)
                                    self.logger.info(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : [{num} out of {len(self.videos_to_dl)}] : progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                                
                                    
                        else: 
                                    
                            raise Exception("no DL init")
                    else:
                                      
                        raise Exception("no info dict")
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())
                    self.list_initnok.append((vid, f"Error:{type(e)}"))
                    self.logger.error(f"worker_init_dl[{i}]: DL constructor failed for {vid} - Error:{type(e)} \n{'!!'.join(lines)}")       
            
    async def worker_manip(self, i):
       
        await self.alogger.debug(f"worker_manip[{i}]: launched")       
        await asyncio.sleep(0)
        
        while True:
            
            try:
            
                
                video_dl = await self.queue_manip.get()
                _video_dl_str = video_dl if video_dl in ["KILL", "KILLANDCLEAN"] else video_dl.info_dl['title']                
                await self.alogger.debug(f"worker_manip[{i}]: get for a video_DL {_video_dl_str}")
                await asyncio.sleep(0)
                
                if video_dl == "KILL":
                    await self.alogger.debug(f"worker_manip[{i}]: get KILL, bye")                    
                    #await asyncio.sleep(0)
                    break
                
                elif video_dl == "KILLANDCLEAN":                     
                    await self.alogger.debug(f"worker_manip[{i}]: get KILLANDCLEAN, bye")
                    #await asyncio.sleep(0) 
                    break
                else:
                    #await self.alogger.debug(f"worker_manip[{i}]: get dl to manip: {type(video_dl)}")
                    await self.alogger.debug(f"worker_manip[{i}]: start to manip {video_dl.info_dl['title']}")
                    task_run_manip = asyncio.create_task(video_dl.run_manip(), name=f"worker_manip[{i}][{video_dl.info_dict['title']}]")
                    #task_run_manip.set_name(f"worker_manip[{i}][{video_dl.info_dict['title']}]")
                    #await asyncio.sleep(0)
                    done, pending = await asyncio.wait([task_run_manip])
                    try:
                        for d in done:
                            d.result()
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        await self.alogger.error(f"worker_manip[{i}][{video_dl.info_dict['title']}]: Error with video DL:\n{'!!'.join(lines)}")
         
                        
            except Exception as e:
                lines = traceback.format_exception(*sys.exc_info())
                await self.alogger.error(f"worker_manip[{i}]: Error:\n{'!!'.join(lines)}")
        
        
            
    
    async def worker_run(self, i):
        
        await self.alogger.debug(f"worker_run[{i}]: launched")       
        await asyncio.sleep(0)
        
        while True:
            
            try:
            
                
                video_dl = await self.queue_dl.get()
                await self.alogger.debug(f"worker_run[{i}]: get for a video_DL")
                await asyncio.sleep(0)
                
                if video_dl == "KILL":
                    await self.alogger.debug(f"worker_run[{i}]: get KILL, bye")                    
                    await asyncio.sleep(0)
                    break
                
                elif video_dl == "KILLANDCLEAN":
                    await self.alogger.debug(f"worker_run[{i}]: get KILLANDCLEAN, bye")  
                    for _ in range(min(self.workers,len(self.videos_to_dl))-1):
                        self.queue_manip.put_nowait("KILL")                    
                    self.queue_manip.put_nowait("KILLANDCLEAN")
                    await asyncio.sleep(0)
                    
                    break
                else:
                    await self.alogger.debug(f"worker_run[{i}]: get dl: {type(video_dl)}")
                    await self.alogger.debug(f"worker_run[{i}]: start to dl {video_dl.info_dl['title']}")
                    
                    task_run = asyncio.create_task(video_dl.run_dl())
                    task_run.set_name(f"worker_run[{i}][{video_dl.info_dict['title']}]")
                    await asyncio.sleep(0)
                    done, pending = await asyncio.wait([task_run])
                    try:
                        for d in done:
                            d.result()
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        await self.alogger.error(f"worker_run[{i}][{video_dl.info_dict['title']}]: Error with video DL:\n{'!!'.join(lines)}")
                    
                    if video_dl.info_dl['status'] == "init_manipulating": self.queue_manip.put_nowait(video_dl)
                    await asyncio.sleep(0)

                        
                        
            except Exception as e:
                lines = traceback.format_exception(*sys.exc_info())
                await self.alogger.error(f"worker_run[{i}]: Error:\n{'!!'.join(lines)}")
                
    
    async def async_ex(self, args_tk, interval):
    
        self.queue_dl = asyncio.Queue()
        self.queue_manip = asyncio.Queue()

        nworkers = min(self.workers,len(self.videos_to_dl))
        await self.alogger.info(f"MAX WORKERS [{nworkers}]")
        
        try:        
            loop = asyncio.get_event_loop()
            #with ThreadPoolExecutor(max_workers=nworkers) as executor:
            
            tasks_run = []
            t1 = []
            tasks_manip = []
            executor = ThreadPoolExecutor(max_workers=nworkers)
            tasks_init = [loop.run_in_executor(executor, self.worker_init_dl, i) for i in range(nworkers)]
            
            if not self.args.nodl:
                t1 = [asyncio.create_task(self.run_tk(args_tk, interval))]
                t1[0].set_name("tk")
                tasks_run = [asyncio.create_task(self.worker_run(i)) for i in range(nworkers)]
                for i,t in enumerate(tasks_run):
                    t.set_name(f"worker_run[{i}]") 
                tasks_manip = [asyncio.create_task(self.worker_manip(i)) for i in range(nworkers)]
                for i,t in enumerate(tasks_manip):
                    t.set_name(f"worker_manip[{i}]")             
            
                                
            done, _ = await asyncio.wait(t1 + tasks_init + tasks_run + tasks_manip)
            
            if done:
                for d in done:
                    try:                        
                        d.result()  
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        await self.alogger.error(f"[async_ex] {lines}")
 
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
                
            await self.alogger.error(f"[async_ex] {lines}")
    
        asyncio.get_running_loop().stop()
        
        


    def get_results_info(self, log=None):
        
        if log: logger = log
        else: logger = self.logger
    
        
        
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
                    videos_kodl.append(f"[{_video.get('id')}][{_video.get('title')}][[{_video.get('url')}]") 
                    videos_kodl_str.append(f"{_video['url']}")
                else: videos_okdl.append(f"[{_video['id']}][{_video['title']}")
        
        videos_initnok = []
        videos_initnok_str = []        
        
        if _videos_initnok:

            
            for vid in _videos_initnok:
                #logger.debug(vid)
                _id = vid[0].get('id') or vid[0].get('videoid')
                _title = vid[0].get('title') or vid[0].get('video_title')
                if _id and _title:
                    item = f"[{_id}][{_title}]"
                else: item = vid[0].get('url')
                videos_initnok.append(item) 
                videos_initnok_str.append(f"{vid[0].get('url')}")        
            
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        self.logger.info(f"*********** FINAL SUMMARY ***********")
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        self.logger.info(f"")
        self.logger.info(f"Init request: Request to DL: [{len(self.list_videos)}]")
        self.logger.info(f"              Already DL: [{len(_videos_aldl)}]")
        self.logger.info(f"              ERROR init DL: [{len(videos_initnok)}]")
        self.logger.info(f"")
        self.logger.info(f"Videos to DL: [{len(_videos_2dl)}]")
        self.logger.info(f"              OK DL: [{len(videos_okdl)}]")
        self.logger.info(f"              ERROR DL: [{len(videos_kodl)}]")
        self.logger.info(f"")
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        self.logger.info(f"*************************************")
        
        self.print_list_videos()
        
        
        self.logger.info(f"******* VIDEO RESULT LISTS *******")        
        self.logger.info(f"Videos ERROR INIT DL: {videos_initnok} \n [-u {' -u '.join(videos_initnok_str)}]")
        self.logger.info(f"Videos ERROR DL: {videos_kodl} \n[-u {' -u '.join(videos_kodl_str)}]")
        self.logger.info(f"Videos ALREADY DL: {_videos_aldl}") 
        self.logger.info(f"Videos DL: {videos_okdl}")
        
        return ({'videos_req': self.list_videos, 'videos_2_dl': _videos_2dl, 'videos_al_dl': _videos_aldl, 'videos_ok_dl': videos_okdl, 'videos_error_init': videos_initnok, 'videos_error_dl': videos_kodl})

    def print_list_videos(self):
    
        list_videos = self.list_videos
        
        list_videos2dl = self.videos_to_dl    
        
            
        # list_videos_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0)), 'url': vid.get('url')} for vid in list_videos]
        # list_videos2dl_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0)), 'url': vid.get('url')} for vid in list_videos2dl]
        
        list_videos_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0))} for vid in list_videos]
        list_videos2dl_str = [{'id' : vid.get('id'), 'title': vid.get('title'), 'filesize' : naturalsize(vid.get('filesize',0))} for vid in list_videos2dl]
        
                
        self.logger.info(f"RESULT: Total videos [{(_tv:=len(list_videos))}] To DL [{(_tv2dl:=len(list_videos2dl))}] Already DL [{(_tval:=len(self.list_initaldl))}]")
        
        #pd.set_option("max_rows", _tv) 
        
        df_tv = pd.DataFrame(list_videos_str)
        tab_tv = tabulate(df_tv, showindex=True, headers=df_tv.columns)      
        #df_tv = pd.DataFrame(list_videos_str)
        
        
        self.logger.info(f"Total videos: [{_tv}]\n\n\n{tab_tv}\n\n\n")
        
            
        df_v2dl = pd.DataFrame(list_videos2dl_str)
        tab_v2dl = tabulate(df_v2dl, showindex=True, headers=df_v2dl.columns)
                
        self.logger.info(f"Videos to DL: [{_tv2dl}]\n\n\n{tab_v2dl}\n\n\n")
        
        
        
        self.logger.info(f"Total bytes to DL: [{naturalsize(self.totalbytes2dl)}]")
        
        self.logger.info(f"Already DL: [{_tval}]\n{self.list_initaldl}")
        
        if self.args.listvideos:
            
            self.logger.info(f"Total videos with CL format: \n{' -u '.join([vid['url'] for vid in list_videos_str])}")
            self.logger.info(f"Videos to DL with CL format: \n{' -u '.join([vid['url'] for vid in list_videos2dl_str])}")
            sys.exit()


@Timer(name="decorator")
def main_program(logger):

    
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

    init_logging()
    logger = logging.getLogger("async_all")
    patch_http_connection_pool(maxsize=100)
    patch_https_connection_pool(maxsize=100)

    return_value = main_program(logger)

    logger.debug(f"async_all return code: {return_value}")

    if return_value != 0:
        sys.exit(return_value)
