#!/usr/bin/env python


from queue import Queue
import logging
import sys
import json
import ast
import tkinter as tk
import asyncio
import aiorun 
from asyncio_pool import AioPool
from pathlib import Path
from tqdm import tqdm


from asynchttpdownloader import (
    AsyncHTTPDownloader
)

from asynchlsdownloader import (
    AsyncHLSDownloader
)

from common_utils import ( 
    init_logging,
    init_ytdl,
    init_tk,
    get_info_dl,
    init_argparser,
    patch_http_connection_pool,
    patch_https_connection_pool
)

from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    ALL_COMPLETED,
    wait
)

from youtube_dl.utils import sanitize_filename

from codetiming import Timer

import time

from datetime import datetime


class AsyncDL():

    def __init__(self, args):
    
        
        self.logger = logging.getLogger("asyncDL")
        self.queue_vid = Queue()
        
        self.list_initnok = []
        self.list_initaldl = []
        self.list_dl = []
        
        
        self.args = args
        self.parts = self.args.p
        self.workers = self.args.w
    
        dict_opts = {'format': self.args.format, 'proxy': self.args.proxy}
        if self.args.nocheckcert:
            dict_opts.update({"nocheckcertificate" : True})
        if self.args.ytdlopts:
            dict_opts.update(ast.literal_eval(self.args.ytdlopts))        
        self.ytdl = init_ytdl(dict_opts,self.args.useragent, self.args.referer)
    

        self.get_videos_cached()
        self.get_list_videos()
        
        
        
    def get_videos_cached(self):
        
        self.files_cached = dict()
        if self.args.cache:
        
            folder_extra_list = [Path(folder) for folder in self.args.cache.split(',')]
            for folder in folder_extra_list:
                for file in folder.rglob('*'):
                    if (file.is_file() or file.is_symlink()) and not file.stem.startswith('.') and file.suffix.lower() in ('.mp4', '.mkv', '.m3u8', '.zip'):
                        _filestr = file.stem.upper()
                        _index = _filestr.find("_")
                        _filestr = sanitize_filename(_filestr[_index+1:], restricted=True)
                        self.files_cached.update({f"{_filestr}": str(file)})
          
        
        list_folders = [Path(Path.home(), "testing"), Path("/Volumes/Pandaext4/videos"), Path("/Volumes/T7/videos")]
        
        for folder in list_folders:
                for file in folder.rglob('*'):
                    if (file.is_file() or file.is_symlink()) and not file.stem.startswith('.') and file.suffix.lower() in ('.mp4', '.mkv', '.m3u8', '.zip'):
                        _filestr = file.stem.upper()
                        _index = _filestr.find("_")
                        _filestr = sanitize_filename(_filestr[_index+1:], restricted=True)
                        self.files_cached.update({f"{_filestr}": str(file)})
        
        
       
           
        self.logger.info(f"Total cached videos: [{len(self.files_cached)}]")
        time_now = datetime.now()
        date_file = f"{time_now.strftime('%Y%m%d')}_{time_now.strftime('%H%M%S')}"
        
        with open(Path(Path.home(), f"Projects/common/logs/{date_file}files_cached.json"),"w") as f:
            json.dump(self.files_cached,f)
               
        
    def get_list_videos(self):
        
        
        self.list_videos = []
        
        if self.args.playlist:
 
            if not self.args.collection_files:

                
                url_pl_list = self.args.collection
                if len(url_pl_list) == 1: 
                    info_dict = self.ytdl.extract_info(url_pl_list[0], download=False)
                    self.logger.debug(info_dict)
                    self.list_videos = info_dict.get('entries')
                else:
                    with ThreadPoolExecutor(max_workers=self.workers) as ex:
                        futures = [ex.submit(self.ytdl.extract_info, url_pl, download=False) for url_pl in url_pl_list]
                    
                    for fut in futures:
                        self.logger.debug(fut)
                        self.list_videos += fut.get('entries')                        


            else:
                
                file_list = self.args.collection_files
                for file in file_list:
                    with open(file, "r") as file_json:
                        info_json = json.loads(file_json.read())
                
                    self.list_videos += list(info_json)

        else: #url no son playlist

            if not self.args.collection_files:
                
                self.list_videos = [{'_type': 'url', 'url': _url} for _url in self.args.collection]

            else:
                def get_info_json(file):
                    with open(file, "r") as f:
                        return json.loads(f.read())
                
                self.list_videos = [get_info_json(file) for file in self.args.collection_files]
        
        time_now = datetime.now()
        date_file = f"{time_now.strftime('%Y%m%d')}_{time_now.strftime('%H%M%S')}"
        with open(Path(Path.home(), f"Projects/common/logs/{date_file}_videolist.json"), "w") as f:
            json.dump(self.list_videos,f)

        if self.args.index:
            if self.args.index in range(1,len(self.list_videos)):
                self.list_videos = [self.list_videos[self.args.index-1]]
            else:
                self.logger.error(f"index video {self.args.index} out of range [1..{len(self.list_videos)}]")
                sys.exit(127)
                
        if self.args.first and self.args.last:
            if (self.args.first in range(1,len(self.list_videos))) and (self.args.last in range(1,len(self.list_videos))) and (self.args.first <= self.args.last):
                self.list_videos = self.list_videos[self.args.first-1:self.args.last]
        

        self.logger.debug(f"Total requested videos: {len(self.list_videos)}")
        self.logger.debug(self.list_videos)
        
        self.videos_to_dl = []
        for video in self.list_videos:
            #if (_id := video.get('id') or video.get('video_id')) and (_title := video.get('title') or video.get('video_title')):               
            if (_title := video.get('title') or video.get('video_title')):
                #vid_name = f"{_id}_{_title}".upper()
                
                vid_name = _title.upper()
                #if (vid_path:=self.files_cached.get(vid_name)):
                if (vid_path:=self.files_cached.get(vid_name)):
                    self.list_initaldl.append({'title': vid_name})
                    self.logger.info(f"[{vid_name}]: already DL")
                    daypath = Path(Path.home(),"testing",time_now.strftime('%Y%m%d'))
                    daypath.mkdir(parents=True, exist_ok=True)
                    file_aldl = Path(daypath, Path(vid_path).name)
                    if file_aldl not in daypath.iterdir():
                        file_aldl.symlink_to(Path(vid_path))
                    
                else: 
                    self.videos_to_dl.append(video)
            else: self.videos_to_dl.append(video)
            
        
        self.nvideos = len(self.videos_to_dl)
        for i,video in enumerate(self.videos_to_dl):
            self.queue_vid.put((i, video))
        self.logger.debug(f"Videos to DL not in local storage: {self.nvideos}")
        self.logger.debug(self.videos_to_dl)
        
        nworkers = min(self.workers, self.nvideos)
        for _ in range(nworkers-1):
            self.queue_vid.put((-1, "KILL"))
        self.queue_vid.put((-1, "KILLANDCLEAN"))
        
        self.logger.debug(f"Queue content for running: \n {list(self.queue_vid.queue)}")
    
    async def run_tk(self, args_tk, interval):
        '''
        Run a tkinter app in an asyncio event loop.
        '''
        
        root, text0, text1, text2, root2, text3 = args_tk
        
        try:

                 
            while True:
                
                root.update()
                root2.update()
                
                #self.logger.debug(f"[tk] {all_tasks}")                
                
                # for dl in list(self.queue_dl._queue):
                #     if dl == "KILL" or dl == "KILLANDCLEAN":
                #         video_queue_str.append(dl)
                        
                #     else:
                #         video_queue_str.append(video.info_dict['title'])
                # video_queue_str = ",".join(video_queue)                    
                text3.delete(1.0, tk.END)
                all_tasks = [f"{t.get_name()}" for t in asyncio.all_tasks()]                
                all_tasks.sort()
                video_queue = []
                for dl in (_dl_queue:=list(self.queue_dl._queue)):
                    if dl in ("KILL", "KILLTK"): video_queue.append(dl)
                    else: video_queue.append(dl.info_dict['title'])
                    
                
                text3.insert(tk.END, f"{video_queue}\n{len(all_tasks)}\n{all_tasks}")
                if (len(video_queue) == 1) and (video_queue[0] == "KILLTK"):
                    break
                
                
                res = set([dl.status for dl in self.list_dl])
                if not res:
                    pass
                else:
                    if ("init" in res and not "downloading" in res): 
                        pass
                    elif (not "init" in res and not "downloading" in res):                
                        break
                    else:
                        text0.delete(1.0, tk.END)
                        text1.delete(1.0, tk.END)
                        text2.delete(1.0, tk.END)    
                        for dl in self.list_dl:
                            mens = dl.print_hookup()
                            if dl.status in ["init"]:
                                text0.insert(tk.END, mens)
                            if dl.status in ["downloading"]:                        
                                text1.insert(tk.END, mens)
                            if dl.status in ["done", "error"]:
                                text2.insert(tk.END,mens)
                            #logger.debug(mens)
                                        
                        
                await asyncio.sleep(interval)
                #time.sleep(interval)    
                
        except tk.TclError as e:
            if "application has been destroyed" not in e.args[0]:
                raise
        
        self.logger.debug("RUN TK BYE")     




    def worker_init_dl(self, i):
        #worker que lanza los Downloaders, uno por video
        
        self.logger.info(f"worker_init_dl[{i}]: launched")

        while True:
            
            num, vid = self.queue_vid.get()
            self.logger.debug(f"worker_init_dl[{i}]: get for a video")
            self.logger.info(f"worker_init_dl[{i}]: [{num}]:{vid}")
            if vid == "KILL":
                self.logger.debug(f"worker_init_dl[{i}]: finds KILL, says bye")
                break
            elif vid == "KILLANDCLEAN":
                self.logger.info(f"worker_init_dl[{i}]: finds KILLANDCLEAN, says bye")
                for _ in range(min(self.workers,self.nvideos)):
                        self.queue_dl.put_nowait("KILL")
                self.queue_dl.put_nowait("KILLTK")
                info_dl = [dl.info_dict for dl in self.list_dl]
                if info_dl:
                    time_now = datetime.now()
                    date_file = f"{time_now.strftime('%Y%m%d')}_{time_now.strftime('%H%M%S')}"
                    with open(Path(Path.home(), f"Projects/common/logs/{date_file}_lastsession.json"), "w") as f:
                        
                        json.dump(info_dl, f)                
                break
            
            else:        
                try:               
                    
                    
                    info_dict = None    
                    # if vid.get('_type') in ('url_transparent', None, 'video'):
                    #     info_dict = self.ytdl.process_ie_result(vid,download=False)
                    # elif vid.get('_type') == 'url':
                    #     info_dict = self.ytdl.extract_info(vid['url'], download=False)
                    # else:
                    #     self.logger.debug(f"Type of result not treated yet: {vid.get('_type')}")
                    #     pass
                    
                    info = self.ytdl.extract_info(vid['url'], download=False,process=False)   
                    if info:                        
                        self.logger.info(f"worker_init_dl[{i}] {info}")
                        #if info.get('_type') == 'url_transparent':
                       
                        _name = f"{(_id:=(info.get('id') or info.get('video_id')))}_{(_title:=(info.get('title') or info.get('video_title')))}".upper()
                        if _name:
                            if (vid_path:=self.files_cached.get(_title)):
                                self.list_initaldl.append({'id': _id, 'title': _title})
                                self.logger.info(f"[{_name}]: already DL")
                                time_now = datetime.now()
                                daypath = Path(Path.home(),"testing",time_now.strftime('%Y%m%d'))
                                daypath.mkdir(parents=True, exist_ok=True)
                                file_aldl = Path(daypath, Path(vid_path).name)
                                if file_aldl not in daypath.iterdir():
                                    file_aldl.symlink_to(Path(vid_path))
                                self.logger.debug(f"worker_init_dl[{i}] {_name} already DL")                            
                                continue
                                                    
                            #self.logger.debug(f"protocol: {protocol}")
                            #self.logger.debug(final_dict)
                        info_dict = self.ytdl.process_ie_result(info,download=False)
                        self.logger.info(f"worker_init_dl[{i}] {info_dict}")
                        protocol, final_dict = get_info_dl(info_dict)
                        if not final_dict.get('filesize'):
                            if (_size:=(vid.get('size') or vid.get('filesize'))):
                                final_dict.update({'filesize': _size})
                        dl = None
                        if protocol in ('http', 'https'):
                            dl = AsyncHTTPDownloader(final_dict, self.ytdl, self.parts)
                        elif protocol in ('m3u8', 'm3u8_native'):
                            dl = AsyncHLSDownloader(final_dict, self.ytdl, self.parts)
                        else:
                            self.logger.error(f"worker_init_dl[{i}] [{info_dict['id']}][{info_dict['title']}]: protocol not supported")
                            raise Exception("protocol not supported")
                            
                        if dl:
                            self.queue_dl.put_nowait(dl)
                            self.list_dl.append(dl)
                            self.logger.info(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : [{num} out of {self.nvideos}] : progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}")
                        else: 
                                
                            raise Exception("no DL init")
                    else:
                        # logger.error(f"{vid['url']}:no info dict")                
                        raise Exception("no info dict")
                except Exception as e:
                    self.list_initnok.append((vid, f"Error:{str(e)}"))
                    self.logger.error(f"worker_init_dl[{i}]: DL constructor failed for {vid} - Error:{str(e)}")       
            
        
    
    async def worker_run(self, i):
        
        self.logger.debug(f"worker_run[{i}]: launched")       
        await asyncio.sleep(1)
        
        
        while True:
            
            try:
            
                
                dl = await self.queue_dl.get()
                self.logger.debug(f"worker_run[{i}]: get for a DL")
                await asyncio.sleep(1)
                
                if dl == "KILL":
                    self.logger.debug(f"worker_run[{i}]: get KILL, bye")
                    await asyncio.sleep(1)
                    break
                
                
                else:
                    self.logger.debug(f"worker_run[{i}]: start to dl {dl.info_dict['title']}")
                    task_run = asyncio.create_task(dl.fetch_async())
                    task_run.set_name(f"worker_run[{i}][{dl.info_dict['title']}]")
                    await asyncio.wait([task_run], return_when=asyncio.ALL_COMPLETED)
            except Exception as e:
                self.logger.error(f"worker_run[{i}]: Error:{str(e)}", exc_info=True)
                
    
    async def async_ex(self, args_tk, interval):
    
        self.queue_dl = asyncio.Queue()

        nworkers = min(self.workers,self.nvideos)
        self.logger.info(f"MAX WORKERS [{nworkers}]")
        
        try:        
            loop = asyncio.get_event_loop()
            #with ThreadPoolExecutor(max_workers=nworkers) as executor:
            
                
            t1 = asyncio.create_task(self.run_tk(args_tk, interval))
            t1.set_name("tk")
            tasks_run = [asyncio.create_task(self.worker_run(i)) for i in range(nworkers)]
            for i,t in enumerate(tasks_run):
                t.set_name(f"worker_run[{i}]")

            executor = ThreadPoolExecutor(max_workers=nworkers)
            tasks_init = [loop.run_in_executor(executor, self.worker_init_dl, i) for i in range(nworkers)]
                                
            await asyncio.wait([t1] + tasks_init + tasks_run, return_when=asyncio.ALL_COMPLETED)
 
        except Exception as e:
           self.logger.error(f"[async_ex] {str(e)}", exc_info=True)
    
        asyncio.get_running_loop().stop()


@Timer(name="decorator")
def main_program(logger):

    
    args = init_argparser()
    
    asyncDL = AsyncDL(args)    
    
    #will store the DL init exit videos
   
    if asyncDL.nvideos > 0:    
            
        try:
                    
    
            #root_tk, text0_tk, text1_tk, text2_tk, root2_tk, text3_tk = init_tk(asyncDL.nvideos)
            args_tk = init_tk(asyncDL.nvideos)      
        
            res = aiorun.run(asyncDL.async_ex(args_tk, 0.25), use_uvloop=True) 
                
        except Exception as e:
            logger.error(str(e), exc_info=True)
            res = 1

    # if not list_dl: return 1
    # else:
    res = 0
    
    _videos_dl = asyncDL.list_dl
    #will store videos nok during init     
    _videos_initnok = asyncDL.list_initnok
    #will store videos already DL        
    _videos_aldl = asyncDL.list_initaldl    
    
            
    videos_okdl = []
    videos_kodl = []
    videos_kodl_str = []
    if _videos_dl:
        
        for dl in _videos_dl:
            if not dl.filename.exists():
                videos_kodl.append(f"[{dl.info_dict['id']}][{dl.info_dict['title']}]") 
                videos_kodl_str.append(f"{dl.info_dict['webpage_url']}")
            else: videos_okdl.append(f"[{dl.info_dict['id']}][{dl.info_dict['title']}]")
    
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
    
            
    
            
    logger.info(f"******* FINAL SUMMARY *******")
    logger.info(f"Init request: Request to DL: [{asyncDL.nvideos}]")
    logger.info(f"              Already DL: [{len(_videos_aldl)}]")
    logger.info(f"              ERROR init DL: [{len(videos_initnok)}]")
    logger.info(f"Videos to DL: [{len(_videos_dl)}]")
    logger.info(f"              OK DL: [{len(videos_okdl)}]")
    logger.info(f"              ERROR DL: [{len(videos_kodl)}]")
    
    logger.info(f"******* VIDEO RESULT LISTS *******")        
    logger.info(f"Videos ERROR INIT DL: {videos_initnok} \n [-u {' -u '.join(videos_initnok_str)}]")
    logger.info(f"Videos ERROR DL: {videos_kodl} \n[-u {' -u '.join(videos_kodl_str)}]")
    logger.info(f"Videos ALREADY DL: {_videos_aldl}") 
    logger.info(f"Videos DL: {videos_okdl}")
    

    
    if not res and (videos_kodl or videos_initnok): return 1             
    return res


if __name__ == "__main__":

    init_logging()
    logger = logging.getLogger("async_all")
    patch_http_connection_pool(maxsize=100)
    patch_https_connection_pool(maxsize=100)

    return_value = main_program(logger)

    logger.info(f"async_all return code: {return_value}")

    if return_value != 0:
        sys.exit(return_value)
