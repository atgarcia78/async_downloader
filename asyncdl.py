import logging
import sys
import traceback
import json
import shutil
import tkinter as tk
import asyncio
from pathlib import Path
from tabulate import tabulate
import time



from utils import (    
    init_ytdl,    
    naturalsize,
    is_playlist_extractor,
    get_chain_links,
    init_aria2c,
    none_to_cero,
    kill_processes,
    async_wait_time,
    async_ex_in_thread    
)

from concurrent.futures import (
    ThreadPoolExecutor
)


from yt_dlp.utils import (
    sanitize_filename,
    std_headers,
    js_to_json
)


from datetime import datetime
from videodownloader import VideoDownloader 

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
        std_headers["User-Agent"] = args.useragent
        std_headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8" 
        std_headers["Connection"] = "keep-alive"
        std_headers["Accept-Language"] = "en,es-ES;q=0.5"
        std_headers["Accept-Encoding"] = "gzip, deflate"
        if args.headers:
            std_headers.update(json.loads(js_to_json(args.headers)))       
        
        logger.debug(f"std-headers:\n{std_headers}")
        
        #aria2c
        if self.args.aria2c: init_aria2c(self.args)
    
        #listas, dicts con videos      
        self.info_videos = {}
        self.files_cached = {}
        
        self.list_videos = []
        self.list_initnok = []
        self.list_unsup_urls = []
        self.list_notvalid_urls = []
        self.list_urls_to_check = []        
        
        self.list_dl = []
        self.videos_to_dl = []        

        #tk control      
        self.stop_tk = False
        
        self.stop_in_out = False        

        #contadores sobre número de workers init, workers run y workers manip
        self.count_init = 0
        self.count_run = 0        
        self.count_manip = 0
        
        self.totalbytes2dl = 0
        
        self.time_now = datetime.now()

    
                
    

    async def run_tk(self, args_tk):
        '''
        Run a tkinter app in an asyncio event loop.
        '''

        root, text0, text1, text2 = args_tk
        count = 0
        
        while (not self.list_dl and not self.stop_tk):
            
            await async_wait_time(self._INTERVAL_TK)
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
                                         
                        
                await async_wait_time(self._INTERVAL_TK)
       
                
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
                        
                        if file.is_file() and not file.stem.startswith('.') and (file.suffix.lower() in ('.mp4', '.mkv', '.ts', '.zip')):

                            
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
            logger.exception(f"[files_cached] {repr(e)}")
               
    def get_list_videos(self):
        
        try:
         
            url_list = []
            _url_list_caplinks = []
            _url_list_cli = []
            url_pl_list = set()
            netdna_list = set()
            
            
            filecaplinks = Path(Path.home(), "Projects/common/logs/captured_links.txt")
            if self.args.caplinks and filecaplinks.exists():
                _temp = set()
                with open(filecaplinks, "r") as file:
                    for _url in file:
                        if (_url:=_url.strip()): _temp.add(_url)           
                    
                _url_list_caplinks = list(_temp)
                
                logger.info(f"video list caplinks \n{_url_list_caplinks}")
                
                for url in _url_list_caplinks:
                   
                    is_pl, ie_key = is_playlist_extractor(url, self.ytdl)
                    
                    if not is_pl:
                        
                        if ie_key == 'NetDNA':
                            netdna_list.add(url)
                            _entry = {}
                            
                        else:
                            _entry = {'_type': 'url', 'url': url, 'ie_key': ie_key}
                            
                    
                        self.info_videos[url] = {'source' : 'caplinks', 
                                                'video_info': _entry, 
                                                'status': 'init', 
                                                'aldl': False,
                                                'ie_key': ie_key, 
                                                'error': []}
                        
                        self.list_videos.append(_entry)
                        
                    else:
                        url_pl_list.add(url)
                        
                        
                        
                
            if self.args.collection:
                
                _url_list_cli = list(set(self.args.collection))
                
                for url in _url_list_cli:
                    
                    is_pl, ie_key = is_playlist_extractor(url, self.ytdl)
                    
                    if not is_pl:
                    
                        if not self.info_videos.get(url):
                            
                            _entry = {'_type': 'url', 'url': url, 'ie_key': ie_key}
                        
                            self.info_videos[url] = {'source' : 'cli', 
                                                        'video_info': _entry, 
                                                        'status': 'init', 
                                                        'aldl': False,
                                                        'ie_key': ie_key, 
                                                        'error': []}
                            self.list_videos.append(_entry)
                            
                            if ie_key == 'NetDNA':
                                netdna_list.add(url)

                                
                    else:
                        url_pl_list.add(url)   
            

            url_list = list(self.info_videos.keys())
            
            logger.info(f"[url_list]: {url_list}")
                    
            if netdna_list:
                logger.info(f"[netdna_list]: {netdna_list}")
                #NetDNAIE._downloader = self.ytdl
                _ies_netdna = self.ytdl.get_info_extractor('NetDNA')
                 
                with ThreadPoolExecutor(thread_name_prefix="Get_netdna", max_workers=min(self.init_nworkers, len(netdna_list))) as ex:
                     
                    futures = [ex.submit(_ies_netdna.get_entry, _url_netdna) for _url_netdna in netdna_list]
                    

                    
                for fut,_url_netdna in zip(futures, netdna_list):
                    try:
                        _entry_netdna = fut.result()
                        
                        
                        self.info_videos[_url_netdna]['video_info'] = _entry_netdna
                        self.list_videos.append(_entry_netdna)
                       
                        
                    except Exception as e:
                        _upt_error = self.info_videos[_url_netdna]['error'].append(str(e))
                        self.info_videos[_url_netdna].update({'status': 'prenok', 'error': _upt_error})                        
                        
                        logger.error(repr(e))
                
                        
            if url_pl_list:
                
                logger.info(f"[url_playlist_list]: {url_pl_list}")
                
                with ThreadPoolExecutor(thread_name_prefix="GetPlaylist", max_workers=min(self.init_nworkers, len(url_pl_list))) as ex:
                        
                    futures = [ex.submit(self.ytdl.extract_info, url_pl) for url_pl in url_pl_list]

                _url_pl_entries = []
                
                for fut, url_pl in zip(futures,url_pl_list):
                    try:
                        _info = self.ytdl.sanitize_info(fut.result())                        
                        if not _info.get('_type'):
                            _info['original_url'] = url_pl                           
                            _url_pl_entries += [_info]
                        else:
                            _url_pl_entries += _info.get('entries')

                    except Exception as e:
                        logger.error(f"[url_playlist_lists][{url_pl}] {repr(e)}")           
                    
        
                logger.debug(f"[url_playlist_lists] entries \n{_url_pl_entries}")
                
                for _url_entry in _url_pl_entries:
                    
                    _type = _url_entry.get('_type', 'video')
                    if _type == 'playlist':
                        logger.warning(f"PLAYLIST IN PLAYLIST: {_url_entry}")
                        continue
                    elif _type == 'video':                        
                        _url = _url_entry.get('original_url') or _url_entry.get('url')
                        
                    else: #url, url_transparent
                        _url = _url_entry.get('url')
                    
                    if not self.info_videos.get(_url): #es decir, los nuevos videos 
                        
                        self.info_videos[_url] = {'source' : 'playlist', 
                                                    'video_info': _url_entry, 
                                                    'status': 'init', 
                                                    'aldl': False,
                                                    'ie_key': _url_entry.get('ie_key') or _url_entry.get('extractor_key'),
                                                    'error': []}
                        
                        _same_video_url = self._check_if_same_video(_url)
                        
                        if _same_video_url: 
                            
                            self.info_videos[_url].update({'samevideo': _same_video_url})
                            logger.warning(f"{_url}: has not been added to video list because it gets same video than {_same_video_url}")
                        
                        self.list_videos.append(_url_entry)


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
                    _url = _video.get('url') or _video.get('webpage_url')
                    if not self.info_videos.get(_url):
                        
                                                
                        self.info_videos[_url] = {'source' : 'file_cli', 
                                                           'video_info': _video, 
                                                           'status': 'init', 
                                                           'aldl': False, 
                                                           'error': []}
                        
                        _same_video_url = self._check_if_same_video(_url)
                        
                        if _same_video_url:
                            
                            self.info_videos[_url].update({'samevideo': _same_video_url})
                            logger.warning(f"{_url}: has not been added to video list because it gets same video than {_same_video_url}")
                        
                        self.list_videos.append(_video)
                       
            

            logger.debug(f"[get_list_videos] list videos: \n{self.list_videos}")
            

            return self.list_videos
        
        except Exception as e:            
            logger.exception(f"[get_videos]: Error {repr(e)}")
            
    
    def _check_if_aldl(self, info_dict):  
                    
               
        #if (info_dict.get('_type') == "url_transparent"):
        #    return False
        
        if not (_id := info_dict.get('id') ) or not ( _title := info_dict.get('title')):
            return False
        
        _title = sanitize_filename(_title, restricted=True).upper()
        vid_name = f"{_id}_{_title}"                    

        if not (vid_path_str:=self.files_cached.get(vid_name)):            
            return False        
        
        else: #video en local            
            
            vid_path = Path(vid_path_str)
            logger.debug(f"[{vid_name}]: already DL: {vid_path}")
                
                                   
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
    
    def _check_if_same_video(self, url_to_check):
        
        info = self.info_videos[url_to_check]['video_info']
        
        if not info.get('_type') and (_id:=info.get('id')) and (_title:=info.get('title')):            
            
            for (urlkey, video) in  self.info_videos.items():
                if urlkey != url_to_check:
                    if not video['video_info'].get('_type') and (video['video_info'].get('id', "") == _id) and (video['video_info'].get('title', "")) == _title:
                        return(urlkey)
                
    def get_videos_to_dl(self): 
        
        initial_videos = [(url, video) for url, video in self.info_videos.items()]
        
        if self.args.index: 
            if self.args.index < len(initial_videos):
                initial_videos = initial_videos[self.args.index - 1:self.args.index]
                #self.info_videos[initial_videos[self.args.index - 1][0]].update({'todl': True})
            else: raise IndexError(f"index video {self.args.index} out of range [{len(initial_videos)}]")
                
            
        elif self.args.first or self.args.last:
            if self.args.first:
                if self.args.first <= len(initial_videos):
                    if self.args.last:
                        if self.args.last >= self.args.first:
                            _last = self.args.last - 1
                        else: raise IndexError(f"index issue with '--first {self.args.first}' and '--last {self.args.last}' options and index video range [0..{len(initial_videos)-1}]")
                    else: _last = len(initial_videos)
                    initial_videos = initial_videos[self.args.first - 1: _last]
                    
                    #for (url, vid) in initial_videos[self.args.first-1:_last]: 
                    #    self.info_videos[url].update({'todl': True})
                       
                else: raise IndexError(f"index issue with '--first {self.args.first}' and '--last {self.args.last}' options and index video range [0..{len(initial_videos)-1}]")
            else:
                if (_last:=self.args.last) > 0:
                    initial_videos = initial_videos[: _last]
                    #for (url, vid) in initial_videos[:_last]: 
                    #    self.info_videos[url].update({'todl': True})
                        
        for (url, vid) in initial_videos:
            
            #if not self.args.nodl: self.info_videos[url].update({'todl': True})
            self.info_videos[url].update({'todl': True})
            if (_id:=self.info_videos[url]['video_info'].get('id')):
                self.info_videos[url]['video_info']['id'] = sanitize_filename(_id, restricted=True).replace('_', '').replace('-','')
            if not self.info_videos[url]['video_info'].get('filesize', None):
                self.info_videos[url]['video_info']['filesize'] = 0
            if (_path:=self._check_if_aldl(vid['video_info'])):  
                self.info_videos[url].update({'aldl' : _path, 'status': 'done'})            

                
        
        for url, infodict in self.info_videos.items():
            if infodict.get('todl') and not infodict.get('aldl') and not infodict.get('samevideo') and infodict.get('status') != 'prenok':
                self.totalbytes2dl += none_to_cero(infodict.get('video_info', {}).get('filesize', 0))
                self.videos_to_dl.append(url)
                
        logger.info(f"Videos to DL not in local storage: [{len(self.videos_to_dl)}] Total size: [{naturalsize(self.totalbytes2dl)}]") 
                

    
    
    async def worker_init(self, i):
        #worker que lanza la creación de los objetos VideoDownloaders, uno por video
        
        logger.debug(f"[worker_init][{i}]: launched")

        try:
        
            while True:
                
                #num, vid = self.queue_vid.get(block=True)    
                async with self.lock:
                    url_key = await self.queue_vid.get()
                    _pending = self.queue_vid.qsize() - self.init_nworkers
                
                if url_key == "KILL":
                    logger.debug(f"[worker_init][{i}]: finds KILL")
                    break
                elif url_key == "KILLANDCLEAN":
                    logger.debug(f"[worker_init][{i}]: finds KILLANDCLEAN")
                    
                    #wait for the others workers_init to finish
                    while (self.count_init < (self.init_nworkers - 1)):
                        #time.sleep(1)
                        await asyncio.sleep(0)
                    
                    for _ in range(self.workers - 1): self.queue_run.put_nowait(("", "KILL"))
                    
                    self.queue_run.put_nowait(("", "KILLANDCLEAN"))
                    
                    if not self.list_dl: 
                        self.stop_tk = True
                        self.stop_in_out = True
 
                    break
                
                else: 
                    
                    vid = self.info_videos[url_key]['video_info']
                    logger.debug(f"[worker_init][{i}]: [{url_key}] extracting info")
                    
                    try: 
                        
                        if vid.get('_type'):
                            #al no tratarse de video final vid['url'] siepre existe
                            try:                                    
                                
                                _res = await async_ex_in_thread(f"wkin[{i}]_ytdl", self.ytdl.extract_info, vid['url'])
                                if not _res: raise Exception("no info video")
                                info = self.ytdl.sanitize_info(_res)
                            
                            except Exception as e: 
                                
                                if 'unsupported url' in str(e).lower():                                    
                                    self.list_unsup_urls.append(url_key)
                                    _error = 'unsupported_url'
                                    
                                elif any(_ in str(e).lower() for _ in ['not found', '404', 'flagged', '403', 'suspended', 'unavailable', 'disabled']):
                                    _error = 'not_valid_url'
                                    self.list_notvalid_urls.append(url_key)                                    
                                    
                                else: 
                                    _error = repr(e)
                                    self.list_urls_to_check.append((url_key, _error))
                                   
                                
                                self.list_initnok.append((url_key, _error))
                                _upt_error = self.info_videos[url_key]['error'].append(_error)
                                self.info_videos[url_key].update({'status': 'initnok', 'error': _upt_error})
                                
                                logger.error(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{url_key}] init nok - {_error}")                                
                                    
                                continue

                        else: info = vid
                        

                        async def sanitise_info_for_dl(_urlkey ,_infdict, _video=None):                   
                            #sanitizamos 'id', y si no lo tiene lo forzamos a un valor basado en la url
                            if (_id:=_infdict.get('id')):
                                
                                _infdict['id'] = sanitize_filename(_id, restricted=True).replace('_', '').replace('-','')
                                
                            else:
                                _infdict['id'] = str(int(hashlib.sha256(_urlkey.encode('utf-8')).hexdigest(),16) % 10**8)                            
                            
                            
                            if _video:
                                if not _infdict.get('release_timestamp') and (_mtime:=_video.get('release_timestamp')):
                                    
                                    _infdict['release_timestamp'] = _mtime
                                    _infdict['release_date'] = _video.get('release_date')
                                
                            logger.debug(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{_infdict.get('id')}][{_infdict.get('title')}] info extracted")                        
                            logger.debug(f"[worker_init][{i}]: [{_urlkey}] info extracted\n{_infdict}")
                            
                            self.info_videos[_urlkey].update({'video_info': _infdict})
                            
                            _filesize = _video.get('filesize',0) if _video else _infdict.get('filesize', 0)
                            
                            if (_path:=self._check_if_aldl(_infdict)):
                                
                                logger.info(f"[worker_init][{i}]: [{_infdict.get('id')}][{_infdict.get('title')}] already DL")                               
                                
                                
                                if _filesize:
                                    async with self.lock:
                                        self.totalbytes2dl -= _filesize
                                    
                                self.info_videos[_urlkey].update({'status': 'done', 'aldl': _path})                                        
                                return
                            
                            if (_same_video_url:=self._check_if_same_video(_urlkey)):
                                                                
                                #self.videos_to_dl.remove(vid)                            
                                if _filesize:
                                    async with self.lock:
                                        self.totalbytes2dl -= _filesize
                                
                                self.info_videos[_urlkey].update({'samevideo': _same_video_url})
                                logger.warning(f"[{_urlkey}]: has not been added to video list because it gets same video than {_same_video_url}")
                                return                                
                            
                            
                            dl = await async_ex_in_thread(f"wkin[{i}]_vdl", VideoDownloader, self.info_videos[_urlkey]['video_info'], self.ytdl, self.args)                       
                                    
                            if not dl.info_dl.get('status', "") == "error":
                                
                                if dl.info_dl.get('filesize'):
                                    self.info_videos[_urlkey]['video_info']['filesize'] = dl.info_dl.get('filesize')
                                    async with self.lock:
                                        self.totalbytes2dl = self.totalbytes2dl - _filesize + dl.info_dl.get('filesize', 0)
                                        
                                self.info_videos[_urlkey].update({'status': 'initok', 'filename': dl.info_dl.get('filename'), 'dl': dl})
                                
                                
                                
                                self.list_dl.append(dl)
                                
                                if dl.info_dl['status'] in ("init_manipulating", "done"):
                                    self.queue_manip.put_nowait((_urlkey, dl))
                                    logger.info(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init OK, video parts DL")
                                else:
                                    self.queue_run.put_nowait((_urlkey, dl))
                                    logger.info(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init OK, ready to DL")
                            
                            else:
                                raise Exception("no DL init")
                    
                        
                        
                        if info.get('_type') == 'playlist':
                            
                            self.info_videos[url_key]['todl'] = False
                            
                            for _entry in info['entries']:
                                
                                try:
                                
                                    if (_type:=_entry.get('_type', 'video')) != 'video':
                                        logger.warning(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{url_key}] playlist of entries that are not videos")
                                    else:
                                        _url = _entry.get('original_url') or _entry.get('url')
                        
                                        if not self.info_videos.get(_url): #es decir, los nuevos videos 
                            
                                            self.info_videos[_url] = {'source' : 'playlist', 
                                                                        'video_info': _entry, 
                                                                        'status': 'init', 
                                                                        'aldl': False,
                                                                        'todl': True,
                                                                        'ie_key': _entry.get('ie_key') or _entry.get('extractor_key'),
                                                                        'error': []}
                            
                                            _same_video_url = self._check_if_same_video(_url)
                            
                                            if _same_video_url: 
                                
                                                self.info_videos[_url].update({'samevideo': _same_video_url})
                                                logger.warning(f"{_url}: has not been added to video list because it gets same video than {_same_video_url}")
                                        
                                            else:
                                                async with self.lock:
                                                    self.totalbytes2dl += none_to_cero(_entry.get('filesize', 0))
                                                await sanitise_info_for_dl(_url, _infdict=_entry)
                                
                                except Exception as e:
                                    lines = traceback.format_exception(*sys.exc_info())
                                    self.list_initnok.append((_entry, f"Error:{repr(e)}"))
                                    logger.error(f"[worker_init][{i}]: [{_url}] init nok - Error:{repr(e)} \n{'!!'.join(lines)}")
                                    
                                    self.list_urls_to_check.append((_url,repr(e)))
                                    _upt_error = self.info_videos[_url]['error'].append(f'DL constructor error:{repr(e)}')
                                    self.info_videos[_url].update({'status': 'initnok', 'error': _upt_error})
                                    
                                    if (_filesize:=_entry.get('filesize',0)):
                                        async with self.lock:
                                            self.totalbytes2dl -= _filesize
                                    continue     
                                    
                    
                        else:
                            await sanitise_info_for_dl(url_key, _infdict=info, _video=vid)
                    

                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        self.list_initnok.append((vid, f"Error:{repr(e)}"))
                        logger.error(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{url_key}] init nok - Error:{repr(e)} \n{'!!'.join(lines)}")
                        
                        self.list_urls_to_check.append((url_key,repr(e)))
                        _upt_error = self.info_videos[url_key]['error'].append(f'DL constructor error:{repr(e)}')
                        self.info_videos[url_key].update({'status': 'initnok', 'error': _upt_error})
                        
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
            
                url_key, video_dl = await self.queue_run.get()
                logger.debug(f"[worker_run][{i}]: get for a video_DL")
                await asyncio.sleep(0)
                
                if video_dl == "KILL":
                    logger.debug(f"[worker_run][{i}]: get KILL, bye")                    
                    await asyncio.sleep(0)
                    break
                
                elif video_dl == "KILLANDCLEAN":
                    logger.debug(f"[worker_run][{i}]: get KILLANDCLEAN, bye")  
                    
                    nworkers = self.workers
                    while (self.count_run < (nworkers - 1)):
                        await asyncio.sleep(1)
                    
                    for _ in range(nworkers):
                        self.queue_manip.put_nowait(("", "KILL")) 
                    await asyncio.sleep(0)
                    self.stop_in_out = True
                    break
                
                else:
                    
                    logger.debug(f"[worker_run][{i}]: start to dl {video_dl.info_dl['title']}")
                    
                    task_run = asyncio.create_task(video_dl.run_dl())
                    await asyncio.sleep(0)
                    done, pending = await asyncio.wait([task_run])
                    
                    for d in done:
                        try:
                            d.result()
                        except Exception as e:
                            lines = traceback.format_exception(*sys.exc_info())
                            logger.error(f"[worker_run][{i}][{url_key}]: Error with video DL: {repr(e)}\n{'!!'.join(lines)}")
                            _upt_error = self.info_videos[url_key]['error'].append(f"{str(e)}")
                            self.info_videos[url_key].update({'error': _upt_error})
                            
                           
                    
                    if video_dl.info_dl['status'] == "init_manipulating": self.queue_manip.put_nowait((url_key, video_dl))
                    else: 
                        logger.error(f"[worker_run][{i}][{url_key}]: error when dl video, can't go por manipulation")
                        _upt_error = self.info_videos[url_key]['error'].append(f"error when dl video: {video_dl.info_dl['error_message']}")
                        self.info_videos[url_key].update({'status': 'nok', 'error': _upt_error})
                        
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
            
                
                url_key, video_dl = await self.queue_manip.get()                              
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
                            _upt_error = self.info_videos[url_key]['error'].append(f"\n error with video manipulation {str(e)}")
                            self.info_videos[url_key].update({'error': _upt_error})
                            
                    if video_dl.info_dl['status'] == "done": self.info_videos[url_key].update({'status': 'done'})
                    else: self.info_videos[url_key].update({'status': 'nok'})
                        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.error(f"[worker_manip][{i}]: Error: {repr(e)}\n{'!!'.join(lines)}")
        finally:
            async with self.lock:
                self.count_manip += 1 
            logger.debug(f"[worker_manip][{i}]: BYE")       

    async def wait_for_stop(self):
        while True:
            if self.stop_in_out:
                break
            else: 
                await asyncio.sleep(0)
        return
    
    async def get_line(self, _reader):
        res = (await _reader.readline()).strip()                
        return res
    
    async def in_out(self):
        
        
                
        try:
            loop = asyncio.get_running_loop()
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            await loop.connect_read_pipe(lambda: protocol, sys.stdin)
            # w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
            # writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
        

            if not self.stop_in_out: logger.info(f"[in_out]: INIT")
            while True:
                logger.info(f"[in_out]: waiting for input")
                
                tasks = [asyncio.create_task(self.get_line(reader)), asyncio.create_task(self.wait_for_stop())]                
                don, pen = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if pen:
                    logger.info(f"[in_out] pending {pen}")
                    for p in pen: p.cancel()
                    try:
                        await asyncio.gather(*pen, return_exceptions=True)
                    except Exception as e:
                        logger.error(str(e))
                
                
                               
                if don:
                    logger.info(f"[in_out] done {don}")
                    for d in don: 
                        try:
                            res = d.result()
                        except Exception as e:
                            logger.error(str(e))
                
                if self.stop_in_out: break
                
                logger.info(f"[in_out]: {res}")                    
                
                res2 = list(bytearray(res))
                
                if res == b'exit':
                    break
                if len(res2) > 1:
                    if res2[0] == 207 and res2[1] == 128:# b'\xcf\x80':
                        logger.info("Evento pause")
                        if len(res2) == 2:
                            if self.list_dl:
                                for dl in self.list_dl:                            
                                    dl.pause()
                        else:
                            _index = res2[2] - 48 - 1
                            self.list_dl[_index].pause()
                            
                    elif res2[0] == 194 and res2[1] == 174: #res == b'\xc2\xae':
                        logger.info("Evento resume")
                        if self.list_dl:
                            for dl in self.list_dl:                            
                                dl.resume()
                
            
            logger.info(f"[in_out]: BYE")
        except Exception as e:
            logger.exception(f"[in_out] {repr(e)}")
        #finally:
            #writer.close()
    
    async def async_ex(self, args_tk):
    
        await self.print_list_videos() 
        
        self.queue_run = asyncio.Queue()
        self.queue_manip = asyncio.Queue()
        self.lock = asyncio.Lock()
        self.queue_vid = asyncio.Queue()
        

        #preparo queue de videos para workers init
        for url in self.videos_to_dl:
            self.queue_vid.put_nowait(url)             
        for _ in range(self.init_nworkers-1):
            self.queue_vid.put_nowait("KILL")        
        self.queue_vid.put_nowait("KILLANDCLEAN")
        
        self.num_videos_to_check = len(self.videos_to_dl)
        
        logger.info(f"MAX WORKERS [{self.workers}]")
        
        try:
            
            tasks_run = []
            task_tk = []
            tasks_manip = []
            task_in_out = []

            tasks_init = [asyncio.create_task(self.worker_init(i)) for i in range(self.init_nworkers)]
                            
            if not self.args.nodl:
            
                task_tk = [asyncio.create_task(self.run_tk(args_tk))] 
                tasks_run = [asyncio.create_task(self.worker_run(i)) for i in range(self.workers)]                  
                tasks_manip = [asyncio.create_task(self.worker_manip(i)) for i in range(self.workers)]
                #task_in_out = [asyncio.create_task(self.in_out())]
                
            done, _ = await asyncio.wait(tasks_init + task_tk + tasks_run + tasks_manip + task_in_out)
            
            for d in done:
                try:
                    d.result()
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())                
                    logger.error(f"[async_ex] {repr(e)}\n{'!!'.join(lines)}")
            
            res = await self.get_results_info()

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[async_ex] {repr(e)}\n{'!!'.join(lines)}")
    
        asyncio.get_running_loop().stop()
        return res      
        
    async def get_results_info(self):       
       

        _videos_url_notsupported = self.list_unsup_urls
        _videos_url_notvalid = self.list_notvalid_urls
        _videos_url_tocheck = [_url for _url, _ in self.list_urls_to_check] if self.list_urls_to_check else []
        _videos_url_tocheck_str = [f"{_url}:{_error}" for _url, _error in self.list_urls_to_check] if self.list_urls_to_check else []       
       
        logger.debug(f'[get_result_info]\n{self.info_videos}')  
            
        videos_okdl = []
        videos_kodl = []        
        videos_koinit = []     
        

        for url, video in self.info_videos.items():
            if not video.get('aldl') and not video.get('samevideo') and video.get('todl'):
                if video['status'] == "done":
                    videos_okdl.append(url)
                else:                    
                    if video['status'] == "initnok" or video['status'] == "prenok":
                        videos_kodl.append(url)
                        videos_koinit.append(url)
                    elif video['status'] == "initok":
                        if self.args.nodl: videos_okdl.append(url)
                    else: videos_kodl.append(url)
            
            
        info_dict = await self.print_list_videos()
        
        info_dict.update({'videosokdl': {'urls': videos_okdl}, 'videoskodl': {'urls': videos_kodl}, 'videoskoinit': {'urls': videos_koinit}, 
                          'videosnotsupported': {'urls': _videos_url_notsupported}, 'videosnotvalid': {'urls': _videos_url_notvalid},
                          'videos2check': {'urls': _videos_url_tocheck, 'str': _videos_url_tocheck_str}})
        
        _columnsaldl = ['ID', 'Title', 'URL', 'Path']
        tab_valdl = tabulate(info_dict['videosaldl']['str'], showindex=True, headers=_columnsaldl, tablefmt="simple") if info_dict['videosaldl']['str'] else None
        _columnssamevideo = ['ID', 'Title', 'URL', 'Same URL']
        tab_vsamevideo = tabulate(info_dict['videossamevideo']['str'], showindex=True, headers=_columnssamevideo, tablefmt="simple") if info_dict['videossamevideo']['str'] else None
        
        try:
            
            logger.info("******************************************************")
            logger.info("******************************************************")
            logger.info("*********** FINAL SUMMARY ****************************")
            logger.info("******************************************************")
            logger.info("******************************************************")
            logger.info("")
            logger.info(f"Request to DL: [{len(info_dict['videos']['urls'])}]")
            logger.info("") 
            logger.info(f"         Already DL: [{len(info_dict['videosaldl']['urls'])}]")
            logger.info(f"         Same requests: [{len(info_dict['videossamevideo']['urls'])}]")
            logger.info(f"         Videos to DL: [{len(info_dict['videos2dl']['urls'])}]")
            logger.info(f"")                
            logger.info(f"                 OK DL: [{len(videos_okdl)}]")
            logger.info(f"                 ERROR DL: [{len(videos_kodl)}]")
            logger.info(f"                     ERROR init DL: [{len(videos_koinit)}]")
            logger.info(f"                         UNSUP URLS: [{len(_videos_url_notsupported)}]")
            logger.info(f"                         NOTVALID URLS: [{len(_videos_url_notvalid)}]")
            logger.info(f"                         TO CHECK URLS: [{len(_videos_url_tocheck)}]")
            logger.info("") 
            logger.info("*********** VIDEO RESULT LISTS **************************")    
            logger.info("") 
            if tab_valdl: 
                logger.info("Videos ALREADY DL:")
                logger.info(f"%no%\n\n{tab_valdl}\n\n")
               
            else:
                logger.info("Videos ALREADY DL: []")
            if tab_vsamevideo: 
                logger.info("SAME requests:")
                logger.info(f"%no%\n\n{tab_vsamevideo}\n\n")
                time.sleep(1)
            else:
                logger.info("SAME requests: []")
            if videos_okdl:    
                logger.info(f"Videos DL: \n{videos_okdl}")
            else:
                logger.info("Videos DL: []")            
            if videos_kodl:  
                logger.info("Videos TOTAL ERROR DL:")
                logger.info(f"%no%\n\n{videos_kodl} \n[-u {' -u '.join(videos_kodl)}]")
            else:
                logger.info(f"Videos TOTAL ERROR DL: []")
            if videos_koinit:            
                logger.info(f"Videos ERROR INIT DL:")
                logger.info(f"%no%\n\n{videos_koinit} \n[-u {' -u '.join(videos_koinit)}]")
            if _videos_url_notsupported:
                logger.info(f"Unsupported URLS:")
                logger.info(f"%no%\n\n{_videos_url_notsupported}")
            if _videos_url_notvalid:
                logger.info(f"Not Valid URLS:")
                logger.info(f"%no%\n\n{_videos_url_notvalid}")
            if _videos_url_tocheck:
                logger.info(f"To check URLS:")
                logger.info(f"%no%\n\n{_videos_url_tocheck}")
            logger.info(f"*****************************************************")
            logger.info(f"*****************************************************")
            logger.info(f"*****************************************************")
            logger.info(f"*****************************************************")
        except Exception as e:
            logger.exception(f"[get_results] {repr(e)}")
        
        logger.debug(f'\n{self.info_videos}')
        
        
        return info_dict

    
    async def print_list_videos(self):
        
        col = shutil.get_terminal_size().columns
        
        list_videos = [url for url, vid in self.info_videos.items() if vid.get('todl')]
        
        list_videos_str = [[fill(url, col//2)] for url in list_videos] if list_videos else []
        
        list_videos2dl = [url for url, vid in self.info_videos.items() if not vid.get('aldl') and not vid.get('samevideo') and vid.get('todl') and vid.get('status') != "prenok"]
        
        list_videos2dl_str = [[fill(vid['video_info'].get('id', ''),col//5), fill(vid['video_info'].get('title', ''), col//5), naturalsize(none_to_cero(vid['video_info'].get('filesize',0))), fill(url, col//3)] for url, vid in self.info_videos.items() if not vid.get('aldl') and not vid.get('samevideo') and vid.get('todl') and vid.get('status') != "prenok"] if list_videos2dl else []
        
        list_videosaldl = [url for url, vid in self.info_videos.items() if vid['aldl'] and vid.get('todl')]
        list_videosaldl_str = [[fill(vid['video_info'].get('id', ''),col//5), fill(vid['video_info'].get('title', ''), col//5), fill(url, col//3), fill(vid['aldl'], col//3)] for url, vid in self.info_videos.items() if vid['aldl'] and vid.get('todl')] if list_videosaldl else []
        
        list_videossamevideo = [url for url, vid in self.info_videos.items() if vid.get('samevideo')]
        list_videossamevideo_str = [[fill(vid['video_info'].get('id', ''),col//5), fill(vid['video_info'].get('title', ''), col//5), fill(url, col//3), fill(vid['samevideo'], col//3)] for url, vid in self.info_videos.items() if vid.get('samevideo')] if list_videossamevideo else []
        
        
        logger.info(f"Total videos [{(_tv:=len(list_videos))}]\nTo DL [{(_tv2dl:=len(list_videos2dl))}]\nAlready DL [{(_tval:=len(list_videosaldl))}]\nSame requests [{(_tval:=len(list_videossamevideo))}]")
        logger.info(f"Total bytes to DL: [{naturalsize(self.totalbytes2dl)}]")
        
        _columns = ['URL']
        tab_tv = tabulate(list_videos_str, showindex=True, headers=_columns, tablefmt="simple") if list_videos_str else None
        
        _columns = ['ID', 'Title', 'Size', 'URL']
        tab_v2dl = tabulate(list_videos2dl_str, showindex=True, headers=_columns, tablefmt="simple") if list_videos2dl_str else None
                
        logger.debug(f"%no%\n\n{tab_tv}\n\n")
        try:
            if tab_v2dl:
                logger.info(f"Videos to DL: [{_tv2dl}]")
                logger.info(f"%no%\n\n\n{tab_v2dl}\n\n\n")
            else:
                logger.info(f"Videos to DL: []")
        
        except Exception as e:
            logger.exception(f"[print_videos] {repr(e)}")    
        
        return {'videos': {'urls': list_videos, 'str': list_videos_str}, 'videos2dl': {'urls': list_videos2dl, 'str': list_videos2dl_str},
                'videosaldl': {'urls': list_videosaldl, 'str': list_videosaldl_str}, 'videossamevideo': {'urls': list_videossamevideo, 'str': list_videossamevideo_str}}
    
    
    def exit(self):
        
        #ies_to_close = ['NakedSwordScene', 'NetDNA', 'GayBeeg', 'GayBeegPlaylist', 'GayBeegPlaylistPage', 'BoyFriendTVEmbed', 'BoyFriendTV']
        ies = self.ytdl._ies_instances
        #for ie in ies_to_close:
        for ie, ins in ies.items():
            #if (_ie:=ies.get(ie)):
            if (close:=getattr(ins, 'close', None)):
                try:
                    close()
                    logger.info(f"[{ie}] closed ok")
                except Exception:
                    pass
        try:        
            kill_processes(logger=logger, rpcport=self.args.rpcport) 
        except Exception as e:
            logger.exception(f"[exit] {repr(e)}")