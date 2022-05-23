import asyncio
import hashlib
import json
import logging
import shutil
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from textwrap import fill

from tabulate import tabulate
from yt_dlp.utils import js_to_json, sanitize_filename, std_headers

from utils import (async_ex_in_executor, async_wait_time, get_chain_links,
                   init_aria2c, init_gui_console, init_gui_root,
                   init_ytdl, is_playlist_extractor, kill_processes,
                   naturalsize, none_to_cero, sg, wait_time, try_get)
from videodownloader import VideoDownloader

logger = logging.getLogger("asyncDL")

class AsyncDL():

    _INTERVAL_GUI = 0.2
   
    def __init__(self, args):
    
        #args
        self.args = args
        self.parts = self.args.parts
        self.workers = self.args.w        
        self.init_nworkers = self.args.winit if self.args.winit > 0 else self.args.w
        
        #youtube_dl
        self.ytdl = init_ytdl(self.args)
        std_headers.update(self.ytdl.params.get('http_headers'))

        if args.headers:
            std_headers.update(json.loads(js_to_json(args.headers)))       
        
        logger.debug(f"std_headers:\n{std_headers}")
        
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
        self.window_console = None
        self.window_root = None      
        self.stop_root = False        
        self.stop_console = True
        
        self.wkinit_stop = False
        self.pasres_repeat = False
        self.console_dl_status = False

        #contadores sobre número de workers init, workers run y workers manip
        self.count_init = 0
        self.count_run = 0        
        self.count_manip = 0

        self.totalbytes2dl = 0
        self.time_now = datetime.now()
        
        self.loop = None
        self.main_task = None
        self.ex_winit = ThreadPoolExecutor(thread_name_prefix="ex_wkinit")

    async def gui_root(self):
        '''
        Run a tkinter app in an asyncio event loop.
        '''

        while (not self.list_dl and not self.stop_root):
            
            await async_wait_time(self._INTERVAL_GUI)
            
        logger.debug(f"[gui_root] End waiting. Signal stop_self.gui_root{self.stop_root}]")
        
        if not self.stop_root:
            #self.window_root, self.window_console = init_gui()
            self.window_root = init_gui_root()
            self.stop_console = False
            await asyncio.sleep(0)
            
            text0 = self.window_root['-ML0-'].TKText
            text1 = self.window_root['-ML1-'].TKText
            text2 = self.window_root['-ML2-'].TKText

            try:
                list_init_old = []
                list_done_old = []
                while not self.stop_root:
                    
                    if self.list_dl: 
                        res = set([dl.info_dl['status'] for dl in self.list_dl])

                        _res = sorted(list(res))
                        if (_res == ["done", "error", "stop"] or _res == ["done", "error"] or _res == ["error"] or _res == ["done"] or _res == ["stop"]) and (self.count_init == self.init_nworkers):                        
                                break
                        else:

                            list_downloading = []
                            list_manip = []
                            list_init = []
                            list_done = []   
                            for i, dl in enumerate(self.list_dl):
                                mens = f"[{i+1}]{await async_ex_in_executor(self.ex_winit, dl.print_hookup)}"
                                if dl.info_dl['status'] in ["init"]:
                                    #text0.insert(sg.tk.END, mens)
                                    list_init.append(mens)
                                if dl.info_dl['status'] in ["init_manipulating", "manipulating"]:
                                    list_manip.append(mens) 
                                if dl.info_dl['status'] in ["downloading"]:
                                    list_downloading.append(mens)  
                                if dl.info_dl['status'] in ["done", "error", "stop"]:
                                    #text2.insert(sg.tk.END,mens)
                                    list_done.append(mens)
                                    
                            
                            # if list_downloading or list_manip:
                            #     text1.delete('1.0', sg.tk.END)                
                            
                            text1.delete('1.0', sg.tk.END) 
                            if list_downloading:
                                text1.insert(sg.tk.END, "\n\n-------DOWNLOADING VIDEO------------\n\n")
                                text1.insert(sg.tk.END, ''.join(list_downloading))
                                if self.console_dl_status:
                                    sg.cprint('"\n\n-------STATUS DL----------------\n\n"')
                                    sg.cprint(''.join(list_downloading))
                                    sg.cprint('"\n\n-------END STATUS DL------------\n\n"')
                                    self.console_dl_status = False                                
                            if list_manip:
                                text1.insert(sg.tk.END, "\n\n-------CREATING FILE------------\n\n")
                                text1.insert(sg.tk.END, ''.join(list_manip))
                                
                            
                            if (list_init != list_init_old):
                                text0.delete('1.0', sg.tk.END)
                                text0.insert(sg.tk.END, ''.join(list_init))
                                
                            list_init_old = list_init
                            
                            if (list_done != list_done_old):
                                text2.delete('1.0', sg.tk.END)
                                text2.insert(sg.tk.END, '\n'.join(list_done))
                                
                            list_done_old = list_done

                            
                    await async_wait_time(self._INTERVAL_GUI)
        
                    
            except Exception as e:
                lines = traceback.format_exception(*sys.exc_info())                
                logger.error(f"[gui_root]: error: {repr(e)}\n{'!!'.join(lines)}")
            finally:           
                logger.debug("[gui_root] BYE")
                try:
                    if self.window_root: 
                        self.window_root.close()
                except Exception as e:
                    logger.exception(f"[gui_root]: error: {repr(e)}")
                finally:
                    del self.window_root
                    

    def pasres_periodic(self):
        logger.info('[pasres_periodic] START')
        while(self.pasres_repeat):
            
            # for dl in self.list_dl:                            
            #     dl.pause()
            self.window_console.write_event_value('Pause', {'-IN-': ''})
            wait_time(2)
            # for dl in self.list_dl:
            #     dl.resume()
            self.window_console.write_event_value('Resume', {'-IN-': ''})
            if not self.pasres_repeat:
                break
            wait_time(20)
        logger.info('[pasres_periodic] END')
        
    def cancel_all_tasks(self):
        if self.loop:
            pending_tasks = asyncio.all_tasks(loop=self.loop)
            logger.debug(f"[cancell_all_tasks] {pending_tasks}")
            if pending_tasks:
                pending_tasks.remove(self.main_task)
                pending_tasks.remove(self.console_task)
                for task in pending_tasks:
                    task.cancel()
                # try:
                #     self.loop.run_until_complete(asyncio.gather(*pending_tasks, loop=self.loop, return_exceptions=True))
                #     logger.info(f"[async_ex] tasks after cancellation: {asyncio.all_tasks(loop=self.loop)}")
                # except Exception as e:
                #     logger.info(f"[cancel_all_tasks] {repr(e)}")
            self.window_console.write_event_value('-EXIT-', {'-IN-': ''})
           
    async def gui_console(self):
        
        try:
            
            while self.stop_console:
                #await async_wait_time(self._INTERVAL_GUI)
                await asyncio.sleep(0)
            
            logger.debug(f"[gui_console] End waiting. Signal stop_console[{self.stop_console}] stop_root[{self.stop_root}]")
            
            self.window_console = init_gui_console()
            
            while not self.stop_root:             
                
                await async_wait_time(self._INTERVAL_GUI/2)
                event, values = self.window_console.read(timeout=0)
                if event == sg.TIMEOUT_KEY:
                    continue
                sg.cprint(event, values)
                if event  == sg.WIN_CLOSED:
                    break
                elif event in ['Exit']:
                    self.window_console.perform_long_operation(self.cancel_all_tasks, end_key='-CANCELALLTASKS-')
                elif event in ['-CANCELALLTASKS-']:
                    logger.info(f'[windows_console] event cancelalltasks') 
                    break
                elif event in ['-EXIT-']:
                    logger.info(f'[windows_console] event -exit-') 
                    break      
                elif event in ['-WKINIT-']:
                    self.wkinit_stop = not self.wkinit_stop
                    sg.cprint(f'Worker inits: BLOCKED') if self.wkinit_stop else sg.cprint(f'Worker inits: RUNNING')
                elif event in ['-PASRES-']:
                    self.pasres_repeat = not self.pasres_repeat
                    if self.pasres_repeat:
                        #no espera, lanza en otro thread la llamada 
                        self.window_console.perform_long_operation(self.pasres_periodic, end_key='-PASRES-STOP-')
                elif event in ['-DL-STATUS']:
                    if not self.console_dl_status:
                        self.console_dl_status = True
                elif event in ['NumVideoWorkers']:
                    if not values['-IN-']:
                        sg.cprint('Please enter number')
                    else:
                        if not values['-IN-'].isdecimal():
                            sg.cprint('not an integer')
                        else:
                            _nvidworkers = int(values['-IN-'])
                            if _nvidworkers == 0:
                                sg.cprint('must be > 0')
                                
                            else:
                                if self.list_dl:
                                    for dl in self.list_dl:
                                        dl.change_numvidworkers(_nvidworkers)
                                else: sg.cprint('DL list empty')
                                
                               
                elif event in ['ToFile', 'Info', 'Pause', 'Resume', 'Reset', 'Stop']:
                    if not values['-IN-']:
                        if event in ['Reset', 'Stop']:
                            sg.cprint('Needs to select a DL')                                                   
                        else:
                            if self.list_dl:
                                info = []
                                for dl in self.list_dl:                            
                                    if event == 'Pause': dl.pause()
                                    elif event == 'Resume': dl.resume()
                                    elif event in ['Info', 'ToFile']: info.append(json.dumps(dl.info_dict))
                                    
                                
                                logger.debug(f"[gui_console] info for print\n{info}")
                                if info:
                                    sg.cprint(f"[{', '.join(info)}]")                                    
                                    if event == 'ToFile':
                                        with open(Path(Path.home(), "testing", _file:=f"{self.time_now.strftime('%Y%m%d_%H%M')}.json"), "w") as f:
                                            f.write(f'{{"entries": [{", ".join(info)}]}}')
                                        sg.cprint(f"saved to file: {_file}")
                                        
                            else: sg.cprint('DL list empty')
                    else:
                        if any(not el.isdecimal() for el in values['-IN-'].split(',')):
                            sg.cprint('not an integer')
                        else:
                            
                            if self.list_dl:
                                for el in values['-IN-'].split(','):
                                    _index = int(el)
                                    if 0 < _index <= len(self.list_dl):                               
                            
                                        if event == 'Pause': self.list_dl[_index-1].pause()
                                        if event == 'Resume': self.list_dl[_index-1].resume()
                                        if event == 'Reset': self.list_dl[_index-1].reset()
                                        if event == 'Stop': self.list_dl[_index-1].stop()
                                        if event == 'Info': sg.cprint(self.list_dl[_index-1].info_dict)
                                    else: sg.cprint('DL index doesnt exist')
                            else: sg.cprint('DL list empty')
                            
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[gui_console]: error: {repr(e)}\n{'!!'.join(lines)}")
        finally:           
            logger.info("[gui_console] BYE")
            try:                
                if self.window_console:
                    self.window_console.close()                    
            except Exception as e:
                logger.exception(f"[gui_console]: error: {repr(e)}")
            finally:
                del self.window_console
    
    def get_videos_cached(self):
        
        try:
            current_res = Path(Path.home(),"Projects/common/logs/current_res.json")
            
            if current_res.exists():
                logger.info(f"[videos_cached] waiting for other asyncdl already scanning")
                
                while (current_res.exists()):
                    time.sleep(1)
                
                Path(Path.home(),"Projects/common/logs/files_cached.json")
                with open(Path(Path.home(),"Projects/common/logs/files_cached.json"),"r") as f:
                    self.files_cached = json.load(f)
                    
                logger.info(f"[videos_cached] Total cached videos (existing files_cached): [{len(self.files_cached)}]")
                
                return self.files_cached                
            
            last_res = Path(Path.home(),"Projects/common/logs/files_cached.json")
            if self.args.nodlcaching and last_res.exists():
                
                                
                with open(last_res,"r") as f:
                    self.files_cached = json.load(f)
                    
                logger.info(f"[videos_cached] Total cached videos: [{len(self.files_cached)}]")
                
                return self.files_cached
            
            else:  
                
                with open(current_res, 'w') as f:
                    f.write("WORKING")
            
                #list_folders = [Path(Path.home(), "testing"), Path("/Volumes/WD5/videos"), Path("/Volumes/Pandaext4/videos"), Path("/Volumes/T7/videos"), Path("/Volumes/Pandaext1/videos"), Path("/Volumes/WD/videos")]
                list_folders = [Path(Path.home(), "testing"), Path("/Volumes/WD5/videos"), Path("/Volumes/Pandaext4/videos")]
                
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
                                                    logger.debug(f'[videos_cached] \nfile not symlink: {str(file)}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links])}')
                                                    for _link in _links[0:-1]:
                                                        _link.unlink()
                                                        _link.symlink_to(file)
                                                        _link._accessor.utime(_link, (int(datetime.now().timestamp()), file.stat().st_mtime), follow_symlinks=False)
                                                
                                                self.files_cached.update({_name: str(file)})
                                            else:
                                                logger.warning(f'[videos_cached] \n**file not symlink: {str(file)}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links])}')
                                                    
                                    elif file.is_symlink() and not _video_path.is_symlink():
                                        _links =  get_chain_links(file)
                                        if (_links[-1] == _video_path):
                                            if len(_links) > 2:
                                                logger.debug(f'[videos_cached] \nfile symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links])}\nvideopath not symlink: {str(_video_path)}')
                                                for _link in _links[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_video_path)
                                                    _link._accessor.utime(_link, (int(datetime.now().timestamp()), _video_path.stat().st_mtime), follow_symlinks=False)
                                                
                                            self.files_cached.update({_name: str(_video_path)})
                                            if not _video_path.exists(): _dont_exist.append({'title': _name, 'file_not_exist': str(_video_path), 'links': [str(_l) for _l in _links[0:-1]]})
                                        else:
                                            logger.warning(f'[videos_cached] \n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links])}\nvideopath not symlink: {str(_video_path)}')

                                    else:
                                        _links_file = get_chain_links(file) 
                                        _links_video_path = get_chain_links(_video_path)
                                        if ((_file:=_links_file[-1]) == _links_video_path[-1]):
                                            if len(_links_file) > 2:
                                                logger.debug(f'[videos_cached] \nfile symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}')                                                
                                                for _link in _links_file[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(_link, (int(datetime.now().timestamp()), _file.stat().st_mtime), follow_symlinks=False)
                                            if len(_links_video_path) > 2:
                                                logger.debug(f'[videos_cached] \nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}')
                                                for _link in _links_video_path[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(_link, (int(datetime.now().timestamp()), _file.stat().st_mtime), follow_symlinks=False)
                                            
                                            self.files_cached.update({_name: str(_file)})
                                            if not _file.exists():  _dont_exist.append({'title': _name, 'file_not_exist': str(_file), 'links': [str(_l) for _l in (_links_file[0:-1] + _links_video_path[0:-1])]})
                                               
                                        else:
                                            logger.warning(f'[videos_cached] \n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}') 

                
                logger.info(f"[videos_cached] Total cached videos: [{len(self.files_cached)}]")
                prev_res = Path(Path.home(),"Projects/common/logs/prev_files_cached.json")                    
                if last_res.exists():
                    if prev_res.exists(): prev_res.unlink()
                    last_res.rename(Path(last_res.parent,f"prev_files_cached.json"))
                
                with open(last_res,"w") as f:
                    json.dump(self.files_cached,f)    
                
                if _repeated:
                    
                    logger.warning("[videos_cached] Please check videos repeated in logs")
                    logger.debug(f"[videos_cached] videos repeated: \n {_repeated}")
                    
                if _dont_exist:
                    logger.warning("[videos_cached] Please check videos dont exist in logs")
                    logger.debug(f"[videos_cached] videos dont exist: \n {_dont_exist}")
                    
                current_res.unlink() 
            
                return self.files_cached
            
        except Exception as e:
            logger.exception(f"[videos_cached] {repr(e)}")
               
    def get_list_videos(self):
        
        try:
         
            url_list = []
            _url_list_caplinks = []
            _url_list_cli = []
            url_pl_list = set()
            netdna_list = set()
            _url_list = {}
            
            filecaplinks = Path(Path.home(), "Projects/common/logs/captured_links.txt")
            if self.args.caplinks and filecaplinks.exists():
                _temp = set()
                with open(filecaplinks, "r") as file:
                    for _url in file:
                        if (_url:=_url.strip()): _temp.add(_url)           
                    
                _url_list_caplinks = list(_temp)
                
                logger.info(f"video list caplinks \n{_url_list_caplinks}")
                shutil.copy("/Users/antoniotorres/Projects/common/logs/captured_links.txt", "/Users/antoniotorres/Projects/common/logs/prev_captured_links.txt")
                with open(filecaplinks, "w") as file:
                    file.write("")
                    
                _url_list['caplinks'] = _url_list_caplinks
            
            if self.args.collection:                
                _url_list_cli = list(set(self.args.collection)) 
                logger.info(f"video list cli \n{_url_list_cli}")
                
                _url_list['cli'] = _url_list_cli   
            
                
            for _source, _ulist in _url_list.items():
                
                for _elurl in _ulist:
                
                    is_pl, ie_key = is_playlist_extractor(_elurl, self.ytdl)
                
                    if not is_pl:
                        
                        if ie_key == 'NetDNA':
                            netdna_list.add(_elurl)

                        _entry = {'_type': 'url', 'url': _elurl, 'ie_key': ie_key}
                        
                        if not self.info_videos.get(_elurl):
                                
                            self.info_videos[_elurl] = {'source' : _source, 
                                                    'video_info': _entry, 
                                                    'status': 'init', 
                                                    'aldl': False,
                                                    'ie_key': ie_key, 
                                                    'error': []}
                    
                            self.list_videos.append(_entry)
                
                    else:
                        url_pl_list.add(_elurl)

            

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
                        self.info_videos[_url_netdna]['error'].append(str(e))
                        self.info_videos[_url_netdna]['status'] = 'prenok'                      
                        
                        logger.error(repr(e))
                
                        
            if url_pl_list:
                
                logger.info(f"[url_playlist_list]: {url_pl_list}")
                
                self._url_pl_entries = []
                
                def custom_callback(fut):
                    _info = self.ytdl.sanitize_info(fut.result())
                    if _info:
                        if _info.get('_type', 'video') == 'video':
                            _info['original_url'] = futures[fut]
                            self._url_pl_entries += [_info]
                        else:
                             self._url_pl_entries += _info.get('entries')
                
                
                with ThreadPoolExecutor(thread_name_prefix="GetPlaylist", max_workers=min(self.init_nworkers, len(url_pl_list))) as ex:
                        
                    self.futures = {ex.submit(self.ytdl.extract_info, url, download=False): url for url in url_pl_list}
                    for fut in self.futures: fut.add_done_callback(custom_callback)
                              
        
                logger.debug(f"[url_playlist_lists] entries \n{self._url_pl_entries}")
                
                for _url_entry in self._url_pl_entries:
                    
                    _type = _url_entry.get('_type', 'video')
                    if _type == 'playlist':
                        logger.warning(f"PLAYLIST IN PLAYLIST: {_url_entry}")
                        continue
                    elif _type == 'video':                        
                        _url = _url_entry.get('original_url') or _url_entry.get('url')
                        
                    else: #url, url_transparent
                        _url = _url_entry.get('original_url') or _url_entry.get('url')
                    
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
                            return json.loads(js_to_json(f.read()))
                    except Exception as e:
                        logger.error(f"[get_list_videos] Error:{repr(e)}")
                        return {}
                        
                _file_list_videos = []
                for file in self.args.collection_files:
                    _file_list_videos += dict(get_info_json(file)).get('entries')
                
                
                for _vid in _file_list_videos:
                    _url = _vid.get('webpage_url')
                    if not self.info_videos.get(_url):
                        
                                                
                        self.info_videos[_url] = {'source' : 'file_cli', 
                                                    'video_info': _vid, 
                                                    'status': 'init', 
                                                    'aldl': False, 
                                                    'error': []}
                        
                        _same_video_url = self._check_if_same_video(_url)
                        
                        if _same_video_url:
                            
                            self.info_videos[_url].update({'samevideo': _same_video_url})
                            logger.warning(f"{_url}: has not been added to video list because it gets same video than {_same_video_url}")
                        
                        else:
                            self.list_videos.append(_vid)
                       
            

            logger.debug(f"[get_list_videos] list videos: \n{self.list_videos}")
            

            return self.list_videos
        
        except Exception as e:            
            logger.exception(f"[get_videos]: Error {repr(e)}")
            
    def _check_if_aldl(self, info_dict):  
                    

        
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
        
        if info.get('_type', 'video') == 'video' and (_id:=info.get('id')) and (_title:=info.get('title')):            
            
            for (urlkey, _vid) in  self.info_videos.items():
                if urlkey != url_to_check:
                    if _vid['video_info'].get('_type', 'video') == 'video' and (_vid['video_info'].get('id', "") == _id) and (_vid['video_info'].get('title', "")) == _title:
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
                
                url_key = await self.queue_vid.get()
                async with self.lock:
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
                    
                    self.print_list_videos()
                    for _ in range(self.workers - 1): self.queue_run.put_nowait(("", "KILL"))
                    
                    self.queue_run.put_nowait(("", "KILLANDCLEAN"))
                    
                    if not self.list_dl: 
                        self.stop_root = True
                        self.stop_console = False
                    
                    #self.ies_close(client=False)     
 
                    break
                
                else: 
                    
                    vid = self.info_videos[url_key]['video_info']
                    logger.debug(f"[worker_init][{i}]: [{url_key}] extracting info")
                    
                    try: 
                        if self.wkinit_stop:
                            logger.info(f"[worker_init][{i}]: BLOCKED")
                            
                            while self.wkinit_stop:
                                await async_wait_time(5)
                                logger.debug(f"[worker_init][{i}]: BLOCKED")
                                
                            logger.info(f"[worker_init][{i}]: UNBLOCKED")
                        
                        if not vid.get('_type', 'video') == 'video':
                            #al no tratarse de video final vid['url'] siempre existe
                            try:                                    
                                
                                
                                _ext_info = try_get(vid.get('original_url'), lambda x: {'original_url': x} if x else {})
                                _res = await async_ex_in_executor(self.ex_winit, self.ytdl.extract_info, vid['url'], download=False, extra_info=_ext_info)
                                #_res = await asyncio.to_thread(self.ytdl.extract_info, vid['url'])
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
                                self.info_videos[url_key]['error'].append(_error)
                                self.info_videos[url_key]['status'] = 'initnok'
                                
                                logger.error(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{url_key}] init nok - {_error}")                                
                                    
                                continue

                        else: 
                            info = vid
                        
                        

                        async def go_for_dl(urlkey ,infdict, extradict=None):                   
                            #sanitizamos 'id', y si no lo tiene lo forzamos a un valor basado en la url
                            if (_id:=infdict.get('id')):
                                
                                infdict['id'] = sanitize_filename(_id, restricted=True).replace('_', '').replace('-','')
                                
                            else:
                                infdict['id'] = str(int(hashlib.sha256(urlkey.encode('utf-8')).hexdigest(),16) % 10**8)                            
                            
                            
                            if extradict:
                                if not infdict.get('release_timestamp') and (_mtime:=extradict.get('release_timestamp')):
                                    
                                    infdict['release_timestamp'] = _mtime
                                    infdict['release_date'] = extradict.get('release_date')
                                
                            logger.debug(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{infdict.get('id')}][{infdict.get('title')}] info extracted")                        
                            logger.debug(f"[worker_init][{i}]: [{urlkey}] info extracted\n{infdict}")
                            
                            self.info_videos[urlkey].update({'video_info': infdict})
                            
                            #_filesize = extradict.get('filesize',0) if extradict else infdict.get('filesize', 0)
                            
                            _filesize = none_to_cero(extradict.get('filesize', 0)) if extradict else none_to_cero(infdict.get('filesize', 0))
                            
                            if (_path:=self._check_if_aldl(infdict)):
                                
                                logger.info(f"[worker_init][{i}]: [{infdict.get('id')}][{infdict.get('title')}] already DL")                               
                                
                                
                                if _filesize:
                                    async with self.lock:
                                        self.totalbytes2dl -= _filesize
                                    
                                self.info_videos[urlkey].update({'status': 'done', 'aldl': _path})                                        
                                return False
                            
                            if (_same_video_url:=self._check_if_same_video(urlkey)):
                                                                
                                #self.videos_to_dl.remove(vid)                            
                                if _filesize:
                                    async with self.lock:
                                        self.totalbytes2dl -= _filesize
                                
                                self.info_videos[urlkey].update({'samevideo': _same_video_url})
                                logger.warning(f"[{urlkey}]: has not been added to video list because it gets same video than {_same_video_url}")
                                return False                             
                            
                            return True
                    
                        async def get_dl(urlkey ,infdict, extradict=None):
                            
                            if await go_for_dl(urlkey ,infdict, extradict) and not self.args.nodl:
                                
                                dl = await async_ex_in_executor(self.ex_winit, VideoDownloader, self.info_videos[urlkey]['video_info'], self.ytdl, self.args)
                                #dl = await asyncio.to_thread(VideoDownloader, self.info_videos[urlkey]['video_info'], self.ytdl, self.args)                       
                                _filesize = none_to_cero(extradict.get('filesize', 0)) if extradict else none_to_cero(infdict.get('filesize', 0))       
                                if not dl.info_dl.get('status', "") == "error":
                                    
                                    if dl.info_dl.get('filesize'):
                                        self.info_videos[urlkey]['video_info']['filesize'] = dl.info_dl.get('filesize')
                                        async with self.lock:
                                            self.totalbytes2dl = self.totalbytes2dl - _filesize + dl.info_dl.get('filesize', 0)
                                            
                                    self.info_videos[urlkey].update({'status': 'initok', 'filename': dl.info_dl.get('filename'), 'dl': dl})

                                    self.list_dl.append(dl)
                                    
                                    if dl.info_dl['status'] in ("init_manipulating", "done"):
                                        self.queue_manip.put_nowait((urlkey, dl))
                                        logger.info(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init OK, video parts DL")
                                    else:
                                        self.queue_run.put_nowait((urlkey, dl))
                                        logger.info(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init OK, ready to DL")
                                
                                else:
                                    async with self.lock:
                                        self.totalbytes2dl -= _filesize
                                             
                                    raise Exception("no DL init")
                        
                        
                        if (_type:=info.get('_type', 'video')) == 'video': 
                            #_filesize = none_to_cero(vid.get('filesize', 0))
                            if self.wkinit_stop:
                                logger.info(f"[worker_init][{i}]: BLOCKED")
                            
                                while self.wkinit_stop:
                                    await async_wait_time(5)
                                    logger.debug(f"[worker_init][{i}]: BLOCKED")
                                
                                logger.info(f"[worker_init][{i}]: UNBLOCKED")
                                
                            await get_dl(url_key, infdict=info, extradict=vid)
                            await asyncio.sleep(0)
                            continue
                        
                        elif _type == 'playlist':
                            
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
                                                #_filesize = none_to_cero(_entry.get('filesize', 0))
                                                async with self.lock:
                                                    #self.totalbytes2dl += _filesize
                                                    self.totalbytes2dl += none_to_cero(_entry.get('filesize', 0))
                                                if self.wkinit_stop:
                                                    logger.info(f"[worker_init][{i}]: BLOCKED")
                            
                                                    while self.wkinit_stop:
                                                        await async_wait_time(5)
                                                        logger.debug(f"[worker_init][{i}]: BLOCKED")
                                
                                                    logger.info(f"[worker_init][{i}]: UNBLOCKED")
                                                await get_dl(_url, infdict=_entry)
                                
                                except Exception as e:
                                    lines = traceback.format_exception(*sys.exc_info())
                                    self.list_initnok.append((_entry, f"Error:{repr(e)}"))
                                    logger.error(f"[worker_init][{i}]: [{_url}] init nok - Error:{repr(e)} \n{'!!'.join(lines)}")
                                    
                                    self.list_urls_to_check.append((_url,repr(e)))
                                    self.info_videos[_url]['error'].append(f'DL constructor error:{repr(e)}')
                                    self.info_videos[_url]['status'] = 'initnok'
                                    
                                    
                                    continue

                    except Exception as e:
                        lines = traceback.format_exception(*sys.exc_info())
                        self.list_initnok.append((vid, f"Error:{repr(e)}"))
                        logger.error(f"[worker_init][{i}]: [{self.num_videos_to_check - _pending}/{self.num_videos_to_check}] [{url_key}] init nok - Error:{repr(e)} \n{'!!'.join(lines)}")
                        
                        self.list_urls_to_check.append((url_key,repr(e)))
                        self.info_videos[url_key]['error'].append(f'DL constructor error:{repr(e)}')
                        self.info_videos[url_key]['status'] = 'initnok'

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
                    
                    self.stop_root = True
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
                            logger.debug(f"[worker_run][{i}][{url_key}]: Error with video DL: {repr(e)}\n{'!!'.join(lines)}")
                            self.info_videos[url_key]['error'].append(f"{str(e)}")
                            
                            
                           
                    
                    if video_dl.info_dl['status'] == "init_manipulating": self.queue_manip.put_nowait((url_key, video_dl))
                    elif video_dl.info_dl['status'] == "stop":
                        logger.info(f"[worker_run][{i}][{url_key}]: STOPPED")
                        self.info_videos[url_key]['error'].append(f"dl stopped")
                        self.info_videos[url_key]['status'] = 'nok'
                        
                    elif video_dl.info_dl['status'] == "error":
                    
                        logger.error(f"[worker_run][{i}][{url_key}]: error when dl video, can't go por manipulation")
                        self.info_videos[url_key]['error'].append(f"error when dl video: {video_dl.info_dl['error_message']}")
                        self.info_videos[url_key]['status'] = 'nok'
                        
                    else:
                        
                        logger.error(f"[worker_run][{i}][{url_key}]: STATUS NOT EXPECTED: {video_dl.info_dl['status']}")
                        self.info_videos[url_key]['error'].append(f"error when dl video: {video_dl.info_dl['error_message']}")
                        self.info_videos[url_key]['status'] = 'nok'
                        
                        
                    await asyncio.sleep(0)
                                
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.debug(f"[worker_run][{i}]: Error: {repr(e)}\n{'!!'.join(lines)}")
        
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
                            logger.debug(f"[worker_manip][{i}][{video_dl.info_dict['title']}]: Error with video manipulation:\n{'!!'.join(lines)}")
                            self.info_videos[url_key]['error'].append(f"\n error with video manipulation {str(e)}")
                            
                            
                    if video_dl.info_dl['status'] == "done": self.info_videos[url_key].update({'status': 'done'})
                    else: self.info_videos[url_key].update({'status': 'nok'})
                        
        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())
            logger.debug(f"[worker_manip][{i}]: Error: {repr(e)}\n{'!!'.join(lines)}")
        finally:
            async with self.lock:
                self.count_manip += 1 
            logger.debug(f"[worker_manip][{i}]: BYE")       

    async def async_ex(self):
    
        self.print_list_videos() 
        
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
            task_gui_root = []
            tasks_manip = []
            task_gui_console = []

            tasks_init = [asyncio.create_task(self.worker_init(i)) for i in range(self.init_nworkers)]
                            
            if not self.args.nodl:                

                task_gui_root = [asyncio.create_task(self.gui_root())] 
                tasks_run = [asyncio.create_task(self.worker_run(i)) for i in range(self.workers)]                  
                tasks_manip = [asyncio.create_task(self.worker_manip(i)) for i in range(self.workers)]
                self.console_task = asyncio.create_task(self.gui_console())
                task_gui_console = [self.console_task]
            
                
            done, _ = await asyncio.wait(tasks_init + task_gui_root + tasks_run + tasks_manip + task_gui_console)
            
            for d in done:
                try:
                    d.result()
                except Exception as e:
                    lines = traceback.format_exception(*sys.exc_info())                
                    logger.error(f"[async_ex] {repr(e)}\n{'!!'.join(lines)}")
            
            

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[async_ex] {repr(e)}\n{'!!'.join(lines)}")
            
          
       
              
    def get_results_info(self):

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
            
            
        info_dict = self.print_list_videos()
        
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
        
        
        
        videos_ko = list(set(info_dict['videoskodl']['urls'] + info_dict['videoskoinit']['urls']))
                    
        if videos_ko: videos_ko_str = "\n".join(videos_ko)        
        else: videos_ko_str = ""
            
        with open("/Users/antoniotorres/Projects/common/logs/error_links.txt", "w") as file:
            file.write(videos_ko_str) 
        
        
        return info_dict

    def print_list_videos(self):
        
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
    
    def ies_close(self, client=True):
        
        ies = self.ytdl._ies_instances
        
        if not ies: return
                
        for ie, ins in ies.items():
            
            if (close:=getattr(ins, 'close', None)):
                try:
                    close(client=client)
                    logger.info(f"[close][{ie}] closed ok")
                    break
                except Exception as e:
                    logger.exception(f"[close][{ie}] {repr(e)}")
    
    def close(self):
        try:        
            self.ies_close()
        except Exception as e:
            logger.exception(f"[close] {repr(e)}")
        try:
            kill_processes(logger=logger, rpcport=self.args.rpcport) 
        except Exception as e:
            logger.exception(f"[close] {repr(e)}")
        