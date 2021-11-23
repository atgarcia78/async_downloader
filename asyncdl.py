import functools
import logging
import sys
import traceback
import json
import demjson
import tkinter as tk
import asyncio
from pathlib import Path
from tabulate import tabulate
import time
import functools
import contextvars


from utils import (    
    init_ytdl,    
    naturalsize,
    is_playlist_extractor,
    get_chain_links,
    init_aria2c
    
)

from concurrent.futures import (
    ThreadPoolExecutor
)


from yt_dlp.utils import sanitize_filename

from yt_dlp.extractor.netdna import NetDNAIE

from datetime import datetime
from operator import itemgetter
from videodownloader import VideoDownloader 


from httpx._utils import Timer

import hashlib

from textwrap import fill


logger = logging.getLogger("asyncDL")

class AsyncDL():

    
    _INTERVAL_TK = 0.2
   
    
    def __init__(self, args):
    
               
        #args
        self.args = args
        self.parts = self.args.parts
        self.workers = self.args.w        
        self.init_nworkers = self.args.winit if self.args.winit > 0 else self.args.w
        
        #youtube_dl
        self.ytdl = init_ytdl(self.args)
        
        if self.args.aria2c: init_aria2c(self.args)
    
              
        #listas con videos y queues       
        self.info_videos = {}
        
        self.list_videos = []
        self.list_initnok = []
        self.list_unsup_urls = []
        self.list_notvalid_urls = []
        self.list_urls_to_check = []
        self.list_initaldl = []
        self.list_dl = []
        self.files_cached = {}
        self.videos_to_dl = []        
        
        #self.queue_vid = Queue()        
        
        #tk control      
        self.stop_tk = False
        self.timer_tk = Timer()
        
        #contadores sobre número de workers init, workers run y workers manip
        self.count_init = 0
        self.count_run = 0        
        self.count_manip = 0 
        
        self.time_now = datetime.now()
        
        #self.lock = Lock()
        
        
    async def wait_time(self, n):
   
        _started = time.monotonic()
        while True:
            if (_t:=(time.monotonic() - _started)) >= n:
                return _t
            else:
                await asyncio.sleep(0)
                
    async def ex_in_thread(self, prefix, func, /, *args, **kwargs):
        
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        func_call = functools.partial(ctx.run, func, *args, **kwargs)
        ex = ThreadPoolExecutor(thread_name_prefix=prefix, max_workers=1)    
        return await loop.run_in_executor(ex, func_call)

    
    async def run_tk(self, args_tk):
        '''
        Run a tkinter app in an asyncio event loop.
        '''
        
       
        root, text0, text1, text2 = args_tk
        count = 0
        
        while (not self.list_dl and not self.stop_tk):
            
            await self.wait_time(self._INTERVAL_TK)
            count += 1
            if count == 10:
                count = 0
                logger.debug("[RUN_TK] Waiting for dl")
        
        logger.debug(f"[RUN_TK] End waiting. Signal stop_tk[{self.stop_tk}]")
        
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
                            mens = await dl.print_hookup()
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
                                         
                        
                await self.wait_time(self._INTERVAL_TK)
       
                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[run_tk]: error\n{'!!'.join(lines)}")
        
        logger.debug("[RUN_TK] BYE") 
    
    
    def get_videos_cached(self):        
        
        
        try:
            
            last_res = Path(Path.home(),"Projects/common/logs/files_cached.json")
            
            if self.args.nodlcaching and last_res.exists():
                
                with open(last_res,"r") as f:
                    self.files_cached = json.load(f)
                    
                logger.info(f"Total cached videos: [{len(self.files_cached)}]")
            
            else:  
            
                list_folders = [Path(Path.home(), "testing"), Path("/Volumes/Pandaext4/videos"), Path("/Volumes/T7/videos"), Path("/Volumes/Pandaext1/videos"), Path("/Volumes/WD/videos"), Path("/Volumes/WD5/videos")]
                
                _repeated = []
                _dont_exist = []
                
                for folder in list_folders:
                    
                    for file in folder.rglob('*'):
                        
                        if file.is_file() and not file.stem.startswith('.') and (file.suffix.lower() in ('.mp4', '.mkv', '.m3u8', '.zip')):

                            
                            _res = file.stem.split('_', 1)
                            if len(_res) == 2:
                                _id = _res[0]
                                _title = sanitize_filename(_res[1], restricted=True).upper()                                
                                _name = f"{_id}_{_title}"
                            else:
                                _name = sanitize_filename(file.stem, restricted=True).upper()

                            if not (_video_path_str:=self.files_cached.get(_name)): 
                                
                                self.files_cached.update({_name: str(file)})
                                
                            else:
                                _video_path = Path(_video_path_str)
                                if _video_path != file: 
                                    
                                    if not file.is_symlink() and not _video_path.is_symlink(): #only if both are hard files we have to do something, so lets report it in repeated files
                                        _repeated.append({'title':_name, 'indict': _video_path_str, 'file': str(file)})
                                    elif not file.is_symlink() and _video_path.is_symlink():
                                        
                                            _links = get_chain_links(_video_path) 
                                            
                                             
                                            if (_links[-1] == file):
                                                if len(_links) > 2:
                                                    logger.debug(f'\nfile not symlink: {str(file)}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links])}')
                                                    for _link in _links[0:-1]:
                                                        _link.unlink()
                                                        _link.symlink_to(file)
                                                        _link._accessor.utime(_link, (int(datetime.now().timestamp()), file.stat().st_mtime), follow_symlinks=False)
                                                
                                                self.files_cached.update({_name: str(file)})
                                            else:
                                                logger.warning(f'\n**file not symlink: {str(file)}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links])}')
                                                    
                                    elif file.is_symlink() and not _video_path.is_symlink():
                                        _links =  get_chain_links(file)
                                        
                                        
                                        
                                        if (_links[-1] == _video_path):
                                            if len(_links) > 2:
                                                logger.debug(f'\nfile symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links])}\nvideopath not symlink: {str(_video_path)}')
                                                for _link in _links[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_video_path)
                                                    _link._accessor.utime(_link, (int(datetime.now().timestamp()), _video_path.stat().st_mtime), follow_symlinks=False)
                                                
                                            self.files_cached.update({_name: str(_video_path)})
                                            if not _video_path.exists(): _dont_exist.append({'title': _name, 'file_not_exist': str(_video_path), 'links': [str(_l) for _l in _links[0:-1]]})
                                        else:
                                            logger.warning(f'\n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links])}\nvideopath not symlink: {str(_video_path)}')
                                            
                                        
                                    else:
                                       
                                        _links_file = get_chain_links(file) 
                                        _links_video_path = get_chain_links(_video_path)
                                        if ((_file:=_links_file[-1]) == _links_video_path[-1]):
                                            if len(_links_file) > 2:
                                                logger.debug(f'\nfile symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}')                                                
                                                for _link in _links_file[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(_link, (int(datetime.now().timestamp()), _file.stat().st_mtime), follow_symlinks=False)
                                            if len(_links_video_path) > 2:
                                                logger.debug(f'\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}')
                                                for _link in _links_video_path[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(_link, (int(datetime.now().timestamp()), _file.stat().st_mtime), follow_symlinks=False)
                                            
                                            self.files_cached.update({_name: str(_file)})
                                            if not _file.exists():  _dont_exist.append({'title': _name, 'file_not_exist': str(_file), 'links': [str(_l) for _l in (_links_file[0:-1] + _links_video_path[0:-1])]})
                                               
                                        else:
                                            logger.warning(f'\n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}') 

                
                logger.info(f"Total cached videos: [{len(self.files_cached)}]")
                
                if _repeated:
                    
                    logger.warning("Please check videos repeated in logs")
                    logger.debug(f"videos repeated: \n {_repeated}")
                    
                if _dont_exist:
                    logger.warning("Please check videos dont exist in logs")
                    logger.debug(f"videos dont exist: \n {_dont_exist}")
                    
                
                            
                prev_res = Path(Path.home(),"Projects/common/logs/prev_files_cached.json")
                
                    
                if last_res.exists():
                    if prev_res.exists(): prev_res.unlink()
                    last_res.rename(Path(last_res.parent,f"prev_files_cached.json"))
                
                with open(last_res,"w") as f:
                    json.dump(self.files_cached,f)                
        
            
            
                return self.files_cached
            
        except Exception as e:
            logger.exception(repr(e))
               

    
    
    def get_list_videos(self):
        
        try:
         
            url_list = []
            _url_list_caplinks = []
            _url_list_cli = []
            
            filecaplinks = Path(Path.home(), "Projects/common/logs/captured_links.txt")
            if self.args.caplinks and filecaplinks.exists():
                _temp = set()
                with open(filecaplinks, "r") as file:
                    for url in file:
                        _temp.add(url.strip())           
                    
                _url_list_caplinks = list(_temp)
                
                logger.info(f"video list caplinks \n{_url_list_caplinks}")
                
                for url in _url_list_caplinks:
                   
                    
                    self.info_videos[url] = {'source' : 'caplinks', 
                                             'video_info': {}, 
                                             'status': 'init', 
                                             'aldl': False, 
                                             'error': []}
                    
                
            if self.args.collection:
                
                _url_list_cli = list(set(self.args.collection))
                
                for url in _url_list_cli:
                    
                    if not self.info_videos.get(url):
                    
                        self.info_videos[url] = {'source' : 'cli', 
                                                 'video_info': {}, 
                                                 'status': 'init', 
                                                 'aldl': False, 
                                                 'error': []}
                    else: _url_list_cli.remove(url)   
            
                
            
            url_pl_list = []
            netdna_list = []
            
            url_list = _url_list_caplinks + _url_list_cli
            
            logger.info(f"url_list: {url_list}")
            
            for _url in url_list:
                
                res = is_playlist_extractor(_url, self.ytdl)
                
                self.info_videos[_url]['ie_key'] = res['ie_key']
                
                if res['is_pl']: 
                    url_pl_list.append(_url)
                    self.info_videos[_url]['type'] = 'playlist'

                else:
                    
                    if 'NetDNA' in res['ie_key']:
                        
                        netdna_list.append(_url)
                        #_entry = NetDNAIE.get_entry(_url, self.ytdl)
                    
                    else: 
                        _entry = {'_type': 'url', 'url': _url, 'ie_key': res['ie_key']}
                        self.info_videos[_url]['video_info'] = _entry
                        self.list_videos.append(_entry)
                    
            if netdna_list:
                logger.info(f"[netdna_list] {netdna_list}")
                #NetDNAIE._downloader = self.ytdl
                with ThreadPoolExecutor(thread_name_prefix="Get_netdna", max_workers=min(self.init_nworkers, len(netdna_list))) as ex:
                     
                    futures = [ex.submit(NetDNAIE.get_entry, _url_netdna, self.ytdl) for _url_netdna in netdna_list]
                    
                    
                for fut,_url_netdna in zip(futures, netdna_list):
                    try:
                        _entry_netdna = fut.result()
                        
                        if not self.info_videos.get(_url_netdna):
                            self.info_videos[_url_netdna]['video_info'] = _entry_netdna
                            self.list_videos.append(_entry_netdna)
                       
                        
                    except Exception as e:
                        logger.exception(repr(e))
                
                        
            if url_pl_list:
                
                logger.info(f"[url_playlist_list] {url_pl_list}")
                
                with ThreadPoolExecutor(thread_name_prefix="GetPlaylist", max_workers=min(self.init_nworkers, len(url_pl_list))) as ex:
                        
                    futures = [ex.submit(self.ytdl.extract_info, url_pl) for url_pl in url_pl_list]
                    
                                
                _url_pl_entries = []
                for fut,url_pl in zip(futures,url_pl_list):
                    try:
                        _url_pl_entries += (_info:=(fut.result())).get('entries')                 
                    
                        self.info_videos[url_pl].update({'video_info': _info})
                    except Exception as e:
                        logger.error(repr(e))           
                    
        
                logger.debug(f"[url_playlist_lists] entries \n{_url_pl_entries}")
                _items = {}
                for entry in _url_pl_entries:
                    _items[entry['url']] = entry
                logger.debug(f"[url_playlist_lists] entries dict \n{_items}")            
                
                
                for _url, _video_info in _items.items():
                    
                    if not _video_info.get('_type'):
                        _video_info['playlist_url'] = _video_info['webpage_url']
                        _video_info['webpage_url'] = _url
                        
                    
                    if not self.info_videos.get(_url):
                        
                        self.info_videos[_url] = {'source' : 'playlist', 
                                                  'video_info': _video_info, 
                                                  'status': 'init', 
                                                  'aldl': False, 
                                                  'error': []}
                        
                        if not (_same_video:=self._check_if_same_video(_video_info)):
                            self.list_videos.append(_video_info)
                        
                        else:
                            _same_video_url = _same_video.get('webpage_url') or _same_video.get('url')
                            self.info_videos[_url].update({'status': 'done', 'samevideo': _same_video_url})
                            logger.warning(f"{_url}: has not been added to video list because it gets same video than {_same_video_url} ")
                
                logger.debug(f"[url_playlist_lists] list videos \n{self.list_videos}") 
                
            if self.args.collection_files:
                
                def get_info_json(file):
                    try:
                        with open(file, "r") as f:
                            return json.loads(f.read())
                    except Exception as e:
                        logger.error(f"[get_list_videos] Error:{repr(e)}")
                        return {}
                        
                _file_list_videos = []
                for file in self.args.collection_files:
                    _file_list_videos += dict(get_info_json(file)).get('entries')
                
                
                for _video in _file_list_videos:
                    _url = _video.get('webpage_url') or _video.get('url')
                    if not self.info_videos.get(_url):
                        
                        self.info_videos[_url] = {'source' : 'file_cli', 
                                                           'video_info': _video, 
                                                           'status': 'init', 
                                                           'aldl': False, 
                                                           'error': []}
                        
                        if not (_same_video:=self._check_if_same_video(_video)):
                            self.list_videos.append(_video)
                        else:
                            _same_video_url = _same_video.get('webpage_url') or _same_video.get('url')
                            self.info_videos[_url].update({'status': 'done', 'samevideo': _same_video_url})
                            logger.warning(f"{_url}: has not been added to video list because it gets same video than {_same_video_url} ")
                       
            

            logger.debug(f"[get_list_videos] list videos: \n{self.list_videos}")
            

            return self.list_videos
        
        except Exception as e:            
            logger.exception(f"[get_videos]: Error {repr(e)}")
            
        
        
    def _check_if_aldl(self, info_dict):  
                    
               
        if (info_dict.get('_type') == "url_transparent"): 
            
            return False
        
        if not (_id := info_dict.get('id') ) or not ( _title := info_dict.get('title')):
            
            return False
        
        _title = sanitize_filename(_title, restricted=True).upper()
        vid_name = f"{_id}_{_title}"                    

        if not (vid_path_str:=self.files_cached.get(vid_name)):
            
            
            return False
        
        
        else: #video en local
            
            
            vid_path = Path(vid_path_str)
            logger.debug(f"[{vid_name}]: already DL: {vid_path}")
                
            self.list_initaldl.append({'title': vid_name, 'path': vid_path_str})                        
            if not self.args.nosymlinks:
                if self.args.path:
                    _folderpath = Path(self.args.path)
                else:
                    _folderpath = Path(Path.home(),"testing",self.time_now.strftime('%Y%m%d'))
                _folderpath.mkdir(parents=True, exist_ok=True)
                file_aldl = Path(_folderpath, vid_path.name)
                if file_aldl not in _folderpath.iterdir():
                    file_aldl.symlink_to(vid_path)
                    mtime = int(vid_path.stat().st_mtime)
                    file_aldl._accessor.utime(file_aldl, (int(datetime.now().timestamp()), mtime), follow_symlinks=False)
                
                
            return vid_path_str
    
    def _check_if_same_video(self, info):
        
        if not info.get('_type') and (_id:=info.get('id')) and (_title:=info.get('title')):            
            
            for video in self.list_videos:
                if not video.get('_type') and (video.get('id', "") == _id) and (video.get('title', "")) == _title:
                    return(video)
                
            
     
    def get_videos_to_dl(self): 
        
        if self.args.index: 
            if self.args.index < len(self.list_videos):
                self.list_videos = [self.list_videos[self.args.index - 1]]
            else: raise IndexError(f"index video {self.args.index} out of range [{len(self.list_videos)}]")
                
            
        elif self.args.first or self.args.last:
            if self.args.first:
                if self.args.first <= len(self.list_videos):
                    if self.args.last:
                        if self.args.last >= self.args.first:
                            _last = self.args.last - 1
                        else: raise IndexError(f"index issue with '--first {self.args.first}' and '--last {self.args.last}' options and index video range [0..{len(self.list_videos)-1}]")
                    else: _last = len(self.list_videos)
                    self.list_videos = self.list_videos[self.args.first-1:_last]
                else: raise IndexError(f"index issue with '--first {self.args.first}' and '--last {self.args.last}' options and index video range [0..{len(self.list_videos)-1}]")
            else:
                if (_last:=self.args.last) > 0: self.list_videos = self.list_videos[:_last]
            
                 
            
        for video in self.list_videos:
            
            _url = video.get('webpage_url') or video.get('url')
            if (_id:=video.get('id')):
                video['id'] = sanitize_filename(_id, restricted=True).replace('_', '').replace('-','')
                self.info_videos[video['url']]['video_info']['id'] = video['id']
            if not video.get('filesize'):
                video.update({'filesize' : 0})                
                                
            if not self.args.nodl:
                self.info_videos[video['url']].update({'todl': True})
            
            if not (_path:=self._check_if_aldl(video)): 
                self.videos_to_dl.append(video)
            else: 
                self.info_videos[video['url']].update({'aldl' : _path, 'status': 'done'})

            
            if self.args.byfilesize:
                self.videos_to_dl = sorted(self.videos_to_dl, key=itemgetter('filesize'), reverse=True)
            
        logger.debug(f"[get_videos_to_dl] videos to dl: \n{self.videos_to_dl}")
        
                

        
        
        self.totalbytes2dl = sum([vid.get('filesize') for vid in self.videos_to_dl])
        logger.info(f"Videos to DL not in local storage: [{len(self.videos_to_dl)}] Total size: [{naturalsize(self.totalbytes2dl)}]")       
        
        
        #logger.debug(f"Queue content for workers inits: \n {list(self.queue_vid.queue)}")
        
        return self.videos_to_dl
    

    async def worker_init(self, i):
        #worker que lanza la creación de los objetos VideoDownloaders, uno por video
        
        logger.debug(f"[worker_init][{i}]: launched")

        try:
        
            while True:
                
                #num, vid = self.queue_vid.get(block=True)    
                num, vid = await self.queue_vid.get()
                
                if vid == "KILL":
                    logger.debug(f"[worker_init][{i}]: finds KILL")
                    break
                elif vid == "KILLANDCLEAN":
                    logger.debug(f"[worker_init][{i}]: finds KILLANDCLEAN")
                    
                    #wait for the others workers_init to finish
                    while (self.count_init < (self.init_nworkers - 1)):
                        #time.sleep(1)
                        await asyncio.sleep(0)
                    
                    for _ in range(self.workers - 1): self.queue_run.put_nowait("KILL")
                    
                    self.queue_run.put_nowait("KILLANDCLEAN")
                    
                    if self.list_dl:
                        info_dl = {"entries": [dl.info_dict for dl in self.list_dl]}
                        
                        if info_dl:
                            #logger.info(info_dl)                   
                            _info_dl_str = demjson.encode(info_dl)
                            with open(Path(Path.home(), f"Projects/common/logs/lastsession.json"), "w") as f:                        
                                f.write(_info_dl_str)
                    else:
                        self.stop_tk = True

                    break
                
                else: 

                    logger.info(f"[worker_init][{i}]: [{num}] [{vid.get('url')}] extracting info")       
                    
                    try: 
                        
                        if "url" in vid.get('_type', ''):
                            
                            try:                                    
                                
                                info = await self.ex_in_thread(f"wkin[{i}]_ytdl", self.ytdl.extract_info, vid['url'])
                                if not info: raise Exception("no info video")
                            
                            except Exception as e:                                
                                
                                logger.info(f"[worker_init][{i}]: [{num}] [{vid['url']}] error when extracting info {repr(e)}")
                                
                                if 'unsupported url' in str(e).lower():                                    
                                    self.list_unsup_urls.append(vid)
                                    _error = 'unsupported_url'
                                    
                                elif any(_ in str(e).lower() for _ in ['not found', '404', 'flagged', '403', 'suspended', 'unavailable', 'disabled']):
                                    self.list_notvalid_urls.append(vid)
                                    _error = 'not_valid_url'
                                    
                                else: 
                                    self.list_urls_to_check.append((vid, repr(e)))
                                    _error = repr(e)
                                
                                self.list_initnok.append((vid, _error))
                                _upt_error = self.info_videos[vid['url']]['error'].append(_error)
                                self.info_videos[vid['url']].update({'status': 'initnok', 'error': _upt_error})
                                
                                logger.error(f"[worker_init][{i}]: [{num}] [{vid['url']}] INIT NOK - ERROR - {_error}") 
                                    
                                continue

                        else: info = vid
                        
                        #creo q esto no se va a dar nunca       
                        if info.get('_type') == 'playlist':
                            info = info['entries'][0]
                    
                        #sanitizamos 'id', y si no lo tiene lo forzamos a un valor basado en la url
                        if (_id:=info.get('id')):
                            
                            info['id'] = sanitize_filename(_id, restricted=True).replace('_', '').replace('-','')
                            
                        else:
                            info['id'] = str(int(hashlib.sha256(info['url'].encode('utf-8')).hexdigest(),16) % 10**8)
                            
                        
                        
                        if not info.get('release_timestamp') and (_mtime:=vid.get('release_timestamp')):
                            
                            info['release_timestamp'] = _mtime
                            info['release_date'] = vid.get('release_date')
                        
                        logger.info(f"[worker_init][{i}]: [{num}] [{vid['url']}] info extracted")
                        logger.debug(f"[worker_init][{i}]: [{num}] [{vid['url']}] info extracted\n{info}")
                        
                        if (_path:=self._check_if_aldl(info)):
                            
                            logger.info(f"[worker_init][{i}]: [{num}] [{vid['url']}] already DL")
                            self.videos_to_dl.remove(vid)
                            
                            if (_filesize:=vid.get('filesize',0)):
                                async with self.lock:
                                    self.totalbytes2dl -= _filesize
                                
                            self.info_videos[vid['url']].update({'status': 'done', 'video_info': info, 'aldl': _path})                                        
                            continue
                       
                        
                        self.info_videos[vid['url']].update({'video_info': info})                        
                        
                        
                        dl = await self.ex_in_thread(f"wkin[{i}]_vdl", VideoDownloader, info, self.ytdl, self.args)                       
                                
                        if not dl.info_dl.get('status', "") == "error":
                            
                            self.info_videos[vid['url']].update({'status': 'initok', 'filename': dl.info_dl.get('filename'), 'dl': dl})
                            
                            async with self.lock:
                                self.totalbytes2dl = self.totalbytes2dl - vid.get('filesize', 0) + dl.info_dl.get('filesize', 0)
                            self.list_dl.append(dl)
                            
                            if dl.info_dl['status'] in ("init_manipulating", "done"):
                                self.queue_manip.put_nowait(dl)
                                logger.info(f"[worker_init][{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : video parts DL, lets create it [{num} out of {len(self.videos_to_dl)}] : progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                            else:
                                self.queue_run.put_nowait(dl)
                                logger.info(f"[worker_init][{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : [{num} out of {len(self.videos_to_dl)}] : progress [initaldl:{len(self.list_initaldl)} dl:{len(self.list_dl)} initnok:{len(self.list_initnok)}]")
                                        
                                        
                        else:                                         
                            
                            raise Exception("no DL init")
                    
                        
                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        self.list_initnok.append((vid, f"Error:{repr(e)}"))
                        logger.error(f"[worker_init][{i}]: DL constructor failed for {vid['url']} - Error:{repr(e)} \n{'!!'.join(lines)}")
                        
                        if info: self.list_urls_to_check.append((info,str(e)))
                        else: self.list_urls_to_check.append((vid,str(e)))
                        
                        _upt_error = self.info_videos[vid['url']]['error'].append(f'DL constructor error:{repr(e)}')
                        self.info_videos[vid['url']].update({'status': 'initnok', 'error': _upt_error})
                        
                        if (_filesize:=vid.get('filesize',0)):
                                async with self.lock:
                                    self.totalbytes2dl -= _filesize
                        
                        continue       
        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.error(f"[worker_init][{i}]: Error:{repr(e)} \n{'!!'.join(lines)}")
                    
        finally:
            async with self.lock:
                self.count_init += 1                
            logger.debug(f"[worker_init][{i}]: BYE")
    
    
    async def worker_run(self, i):
        
        logger.debug(f"[worker_run][{i}]: launched")       
        await asyncio.sleep(0)
        
        try:
            
            while True:
            
                video_dl = await self.queue_run.get()
                logger.debug(f"[worker_run][{i}]: get for a video_DL")
                await asyncio.sleep(0)
                
                if video_dl == "KILL":
                    logger.debug(f"[worker_run][{i}]: get KILL, bye")                    
                    await asyncio.sleep(0)
                    break
                
                elif video_dl == "KILLANDCLEAN":
                    logger.debug(f"[worker_run][{i}]: get KILLANDCLEAN, bye")  
                    #nworkers = min(self.workers,len(self.videos_to_dl))
                    nworkers = self.workers
                    while (self.count_run < (nworkers - 1)):
                        await asyncio.sleep(1)
                    
                    for _ in range(nworkers):
                        self.queue_manip.put_nowait("KILL") 
                    await asyncio.sleep(0)
                    
                    break
                
                else:
                    logger.debug(f"[worker_run][{i}]: get dl: {type(video_dl)}")
                    logger.debug(f"[worker_run][{i}]: start to dl {video_dl.info_dl['title']}")
                    
                    task_run = asyncio.create_task(video_dl.run_dl())
                    await asyncio.sleep(0)
                    done, pending = await asyncio.wait([task_run])
                    
                    for d in done:
                        try:
                            d.result()
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            logger.error(f"[worker_run][{i}][{video_dl.info_dict['title']}]: Error with video DL:\n{'!!'.join(lines)}")
                            _upt_error = self.info_videos[video_dl.info_dl['webpage_url']]['error'].append(f"{str(e)}")
                            self.info_videos[video_dl.info_dl['webpage_url']].update({'error': _upt_error})
                            
                           
                    
                    if video_dl.info_dl['status'] == "init_manipulating": self.queue_manip.put_nowait(video_dl)
                    else: 
                        logger.error(f"[worker_run][{i}][{video_dl.info_dict['title']}]: error when dl video, can't go por manipulation")
                        _upt_error = self.info_videos[video_dl.info_dl['webpage_url']]['error'].append(f"error when dl video: {video_dl.info_dl['error_message']}")
                        self.info_videos[video_dl.info_dl['webpage_url']].update({'status': 'nok', 'error': _upt_error})
                      
                        
                        
                    await asyncio.sleep(0)
                                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.error(f"[worker_run][{i}]: Error: {repr(e)}\n{'!!'.join(lines)}")
        
        finally:
            async with self.lock:
                self.count_run += 1 
            logger.debug(f"[worker_run][{i}]: BYE")
        
    
    async def worker_manip(self, i):
       
        logger.debug(f"[worker_manip][{i}]: launched")       
        await asyncio.sleep(0)

        try:
            
            while True:
            
                
                video_dl = await self.queue_manip.get()                              
                logger.debug(f"[worker_manip][{i}]: get for a video_DL")
                await asyncio.sleep(0)
                
                if video_dl == "KILL":
                    logger.debug(f"[worker_manip][{i}]: get KILL, bye")                    
                    #await asyncio.sleep(0)
                    break                

                else:
                    logger.debug(f"[worker_manip][{i}]: start to manip {video_dl.info_dl['title']}")
                    task_run_manip = asyncio.create_task(video_dl.run_manip(), name=f"[worker_manip][{i}][{video_dl.info_dict['title']}]")      
                    done, _ = await asyncio.wait([task_run_manip])
                    
                    for d in done:
                        try:
                            d.result()
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            logger.error(f"[worker_manip][{i}][{video_dl.info_dict['title']}]: Error with video manipulation:\n{'!!'.join(lines)}")
                            _upt_error = self.info_videos[video_dl.info_dl['webpage_url']]['error'].append(f"\n error with video manipulation {str(e)}")
                            self.info_videos[video_dl.info_dl['webpage_url']].update({'error': _upt_error})
                            
                    if video_dl.info_dl['status'] == "done": self.info_videos[video_dl.info_dl['webpage_url']].update({'status': 'done'})
                    else: self.info_videos[video_dl.info_dl['webpage_url']].update({'status': 'nok'})
                        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.error(f"[worker_manip][{i}]: Error: {repr(e)}\n{'!!'.join(lines)}")
        finally:
            async with self.lock:
                self.count_manip += 1 
            logger.debug(f"[worker_manip][{i}]: BYE")       

 
    async def async_ex(self, args_tk):
    
        self.queue_run = asyncio.Queue()
        self.queue_manip = asyncio.Queue()
        self.lock = asyncio.Lock()
        self.queue_vid = asyncio.Queue()
        
        
        
        #preparo queue de videos para workers init
        for i, video in enumerate(self.videos_to_dl):
            self.queue_vid.put_nowait((i, video))             
        for _ in range(self.init_nworkers-1):
            self.queue_vid.put_nowait((-1, "KILL"))        
        self.queue_vid.put_nowait((-1, "KILLANDCLEAN"))

        
        logger.info(f"MAX WORKERS [{self.workers}]")
        
        try:        
          
 
            tasks_run = []
            task_tk = []
            tasks_manip = []

            #tasks_init = [asyncio.create_task(asyncio.to_thread(self.worker_init, i)) for i in range(self.init_nworkers)]
            
            tasks_init = [asyncio.create_task(self.worker_init(i)) for i in range(self.init_nworkers)]
                            
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
                    logger.error(f"[async_ex] {repr(e)}\n{'!!'.join(lines)}")

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[async_ex] {repr(e)}\n{'!!'.join(lines)}")
    
        asyncio.get_running_loop().stop()      
        

    def get_results_info(self):
        
                
        if self.args.byfilesize:
            self.videos_to_dl = sorted(self.videos_to_dl, key=itemgetter('filesize'), reverse=True)
        
        #tras descartar los que ya están en local
        _videos_2dl = self.videos_to_dl
        
        #will store videos already DL        
        _videos_aldl = self.list_initaldl   

        
        _videos_url_notsupported = [vid['url'] for vid in self.list_unsup_urls]
        
        _videos_url_notvalid = [vid['url'] for vid in self.list_notvalid_urls]
        
        _videos_url_tocheck = [f"{vid['url']}:{error}" for vid, error in self.list_urls_to_check]
          
       
        logger.debug(f'[get_result_info]\n{self.info_videos}')  
            
        videos_okdl = []
        videos_kodl = []        
        videos_koinit = []     
        

        for url, video in self.info_videos.items():
            if not video.get('type', "") == "playlist" and not video.get('aldl') and video.get('todl'):
                if video['status'] == "done":
                    videos_okdl.append(url)
                else:
                    videos_kodl.append(url)
                    if video['status'] == "initnok":
                        videos_koinit.append(url)
            
            
        self.print_list_videos()
        
        list_videosaldl_str = [[vid['video_info'].get('id'), fill(vid['video_info'].get('title', ''), 40), 
                               fill(vid['video_info'].get('webpage_url', vid['video_info'].get('url', '')), 150),
                               fill(vid['aldl'], 150)]
                                for vid in list(self.info_videos.values()) if not vid.get('type') == "playlist" and vid['aldl'] and vid.get('todl')]
        _columns = ['ID', 'Title', 'URL', 'Path']
        tab_valdl = tabulate(list_videosaldl_str, showindex=True, headers=_columns, tablefmt="grid")
        
        
        logger.info(f"******************************************************")
        logger.info(f"******************************************************")
        logger.info(f"*********** FINAL SUMMARY ****************************")
        logger.info(f"******************************************************")
        logger.info(f"******************************************************")
        logger.info(f"")
        logger.info(f"Request to DL: [{len(self.list_videos)}]")
        logger.info(f"") 
        logger.info(f"         Already DL: [{len(_videos_aldl)}]")
        logger.info(f"         Videos to DL: [{len(_videos_2dl)}]")
        logger.info(f"")                
        logger.info(f"                 OK DL: [{len(videos_okdl)}]")
        logger.info(f"                 ERROR DL: [{len(videos_kodl)}]")
        logger.info(f"                     ERROR init DL: [{len(videos_koinit)}]")
        logger.info(f"                         UNSUP URLS: [{len(_videos_url_notsupported)}]")
        logger.info(f"                         NOTVALID URLS: [{len(_videos_url_notvalid)}]")
        logger.info(f"                         TO CHECK URLS: [{len(_videos_url_tocheck)}]")
        logger.info(f"") 
        logger.info(f"*********** VIDEO RESULT LISTS **************************")    
        logger.info(f"") 
        if _videos_aldl: 
            logger.info(f"%no%Videos ALREADY DL: \n\n{tab_valdl}\n\n")
        else:
            logger.info(f"Videos ALREADY DL: []")
        if videos_okdl:    
            logger.info(f"Videos DL: \n{videos_okdl}")
        else:
            logger.info(f"Videos DL: []")            
        if videos_kodl:  
            logger.info(f"%no%Videos TOTAL ERROR DL:\n\n{videos_kodl} \n[-u {' -u '.join(videos_kodl)}]")
        else:
            logger.info(f"Videos TOTAL ERROR DL: []")
        if videos_koinit:            
            logger.info(f"%no%Videos ERROR INIT DL:\n\n{videos_koinit} \n[-u {' -u '.join(videos_koinit)}]")
        if _videos_url_notsupported:
            logger.info(f"%no%Unsupported URLS:\n\n{_videos_url_notsupported}")
        if _videos_url_notvalid:
            logger.info(f"%no%Not Valid URLS:\n\n{_videos_url_notvalid}")
        if _videos_url_tocheck:
            logger.info(f"%no%To check URLS:\n\n{_videos_url_tocheck}")
        logger.info(f"*****************************************************")
        logger.info(f"*****************************************************")
        logger.info(f"*****************************************************")
        logger.info(f"*****************************************************")
        
        logger.debug(f'\n{self.info_videos}')
        
        
        return ({'videos_req': self.list_videos, 'videos_2_dl': _videos_2dl, 'videos_al_dl': _videos_aldl, 'videos_ok_dl': videos_okdl, 'videos_error_init': videos_koinit, 'videos_error_dl': videos_kodl})

    def print_list_videos(self):
        



        list_videos_str = [[fill(vid['video_info'].get('webpage_url', vid['video_info'].get('url', '')), 250)]
                            for vid in list(self.info_videos.values()) if not vid.get('type', "") == "playlist"]
        
        list_videos2dl_str = [[vid['video_info'].get('id'), fill(vid['video_info'].get('title', ''), 50), naturalsize(vid['video_info'].get('filesize',0)),
                               fill(vid['video_info'].get('webpage_url', vid['video_info'].get('url', '')), 250)]
                                for vid in list(self.info_videos.values()) if not vid.get('type') == "playlist" and not vid['aldl'] and vid.get('todl')]
        
        
                
        logger.info(f"RESULT: Total videos [{(_tv:=len(self.list_videos))}] To DL [{(_tv2dl:=len(self.videos_to_dl))}] Already DL [{(_tval:=len(self.list_initaldl))}]")
        logger.info(f"Total bytes to DL: [{naturalsize(self.totalbytes2dl)}]")
        
        _columns = ['URL']
        tab_tv = tabulate(list_videos_str, showindex=True, headers=_columns, tablefmt="grid")
        
        _columns = ['ID', 'Title', 'Size', 'URL']
        tab_v2dl = tabulate(list_videos2dl_str, showindex=True, headers=_columns, tablefmt="grid")
                
        logger.debug(f"%no%\n\n{tab_tv}\n\n")
        logger.info(f"%no%Videos to DL: [{_tv2dl}]\n\n\n{tab_v2dl}\n\n\n")
                
        