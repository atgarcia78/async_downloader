import asyncio
import hashlib
import json
import logging
import shutil
import sys
import time
import traceback
from concurrent.futures import (
    ThreadPoolExecutor,
    wait
)
from datetime import datetime
from pathlib import Path
from textwrap import fill

from tabulate import tabulate

from utils import (
    perform_long_operation,
    async_ex_in_executor,
    async_wait_time,
    get_chain_links,
    init_aria2c,
    init_proxies, 
    init_gui_console,
    init_gui_root,
    init_gui_log,
    init_ytdl, 
    is_playlist_extractor, 
    kill_processes,
    naturalsize, 
    none_to_cero, 
    sg, 
    wait_time, 
    try_get,
    traverse_obj,
    js_to_json,
    sanitize_filename,
    print_tasks,
    LocalStorage,
    PATH_LOGS,
    get_domain,
    run_proxy_http,
    CONF_PROXIES_MAX_N_GR_HOST, 
    CONF_PROXIES_N_GR_VIDEO    
)

from videodownloader import VideoDownloader

from threading import Lock
from codetiming import Timer

from multiprocess import (
    Process as MPProcess,
    Queue as MPQueue
)

from itertools import zip_longest


logger = logging.getLogger("asyncDL")


class AsyncDL():

    _INTERVAL_GUI = 0.2
    
   
    def __init__(self, args):
    
        #args
        self.args = args
        self.workers = self.args.w        
        self.init_nworkers = self.args.winit if self.args.winit > 0 else self.args.w
        
        #youtube_dl
        self.ytdl = init_ytdl(self.args)
        
            
        #listas, dicts con videos      
        self.info_videos = {}
        self.videos_cached = {}
        
        self.list_videos = []
        self.list_initnok = []
        self.list_unsup_urls = []
        self.list_notvalid_urls = []
        self.list_urls_to_check = []        
        
        self.list_dl = []
        self.videos_to_dl = []

        self.num_videos_to_check = 0
        self.num_videos_pending = 0
        self.getlistvid_done = False
                

        #tk control
        self.window_console = None
        self.window_root = None      
        self.stop_root = False        
        self.stop_console = False
        
        self.wkinit_stop = False
        self.pasres_repeat = True
        self.console_dl_status = False
        
        self.list_pasres = set()
        self.pasres_time_from_resume_to_pause = 5
        self.pasres_active = False

        #contadores sobre número de workers init, workers run y workers manip
        self.count_init = 0
        self.count_run = 0        
        self.count_manip = 0
        self.hosts_downloading = {}

        self.totalbytes2dl = 0
        self.launch_time = datetime.now()
        
        self.loop = None
        self.main_task = None
        self.ex_winit = ThreadPoolExecutor(thread_name_prefix="ex_wkinit")
        self.lock = Lock()
        
        self.t1 = Timer("execution", text="Time spent with data preparation for the init workers: {:.2f}", logger=logger.info)
        self.t2 = Timer("execution", text="Time spent with DL: {:.2f}", logger=logger.info)
        self.t3 = Timer("execution", text="Time spent by init workers: {:.2f}", logger=logger.info)
        
        self.reset = False
        
        self.get_videos_cached()        
        
        
    def get_videos_cached(self):                
        self.queue = MPQueue()
        self.p1 = MPProcess(target=self.load_videos_cached, args=(self.queue,))
        self.p1.start()
        self.videos_cached = self.queue.get()
        
        logger.info(f"[videos_cached] Total cached videos: [{len(self.videos_cached )}]")        
        
        try:        
            self.p1.join()
            self.p1.close()
        except Exception as e:
            pass
    
    #run in process
    def load_videos_cached(self, _queue):
        
        ''' 
        
        In local storage, files aee saved wihtin the file files.cached.json in 5 groups each in different volumnes.
        If any of the volumes can't be accesed in real time, the local storage info of that volume will be used.    
        
        '''

        local_storage = LocalStorage()       

        logger.info(f"[videos_cached] start scanning - dlnocaching [{self.args.nodlcaching}]")
        
        videos_cached = {}        
        last_time_sync = {}        
        
        try:            
            
            with local_storage.lock:
            
                local_storage.load_info()
                
                list_folders_to_scan = {}
                videos_cached = {}            
                last_time_sync = local_storage._last_time_sync
                            
                if self.args.nodlcaching:
                    for _vol,_folder in local_storage.config_folders.items():
                        if _vol != "local":                            
                            videos_cached.update(local_storage._data_from_file.get(_vol))
                        else:
                            list_folders_to_scan.update({_folder: _vol})

                else:
                    
                    for _vol,_folder in local_storage.config_folders.items():
                        if not _folder.exists(): #comm failure
                            logger.error(f"Fail to connect to [{_vol}], will use last info")
                            videos_cached.update(local_storage._data_from_file.get(_vol))
                        else:
                            list_folders_to_scan.update({_folder: _vol})

                _repeated = []
                _dont_exist = []
                    
                for folder in list_folders_to_scan:
                    
                    try:
                        
                        files = [file for file in folder.rglob('*') 
                                if file.is_file() and not file.stem.startswith('.') and (file.suffix.lower() in ('.mp4', '.mkv', '.zip'))]
                        
                        for file in files:                        

                            _res = file.stem.split('_', 1)
                            if len(_res) == 2:
                                _id = _res[0]
                                _title = sanitize_filename(_res[1], restricted=True).upper()                                
                                _name = f"{_id}_{_title}"
                            else:
                                _name = sanitize_filename(file.stem, restricted=True).upper()

                            if not (_video_path_str:=videos_cached.get(_name)): 
                                
                                videos_cached.update({_name: str(file)})
                                
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
                                                        _link._accessor.utime(_link, (int(self.launch_time().timestamp()), file.stat().st_mtime), follow_symlinks=False)
                                                
                                                videos_cached.update({_name: str(file)})
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
                                                    _link._accessor.utime(_link, (int(self.launch_time().timestamp()), _video_path.stat().st_mtime), follow_symlinks=False)
                                                
                                            videos_cached.update({_name: str(_video_path)})
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
                                                    _link._accessor.utime(_link, (int(self.launch_time().timestamp()), _file.stat().st_mtime), follow_symlinks=False)
                                            if len(_links_video_path) > 2:
                                                logger.debug(f'[videos_cached] \nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}')
                                                for _link in _links_video_path[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(_link, (int(self.launch_time().timestamp()), _file.stat().st_mtime), follow_symlinks=False)
                                            
                                            videos_cached.update({_name: str(_file)})
                                            if not _file.exists():  _dont_exist.append({'title': _name, 'file_not_exist': str(_file), 'links': [str(_l) for _l in (_links_file[0:-1] + _links_video_path[0:-1])]})
                                                
                                        else:
                                            logger.warning(f'[videos_cached] \n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}') 

                    except Exception as e:
                        logger.error(f"[videos_cached][{list_folders_to_scan[folder]}] {repr(e)}")

                    else:
                        last_time_sync.update({list_folders_to_scan[folder]: str(self.launch_time)})
                    
                
                logger.info(f"[videos_cached] Total videos cached: [{len(videos_cached)}]")
                

                try:
                    
                    local_storage.dump_info(videos_cached, last_time_sync)           
                
                    if _repeated:                                                
                        logger.warning("[videos_cached] Please check videos repeated in logs")
                        logger.debug(f"[videos_cached] videos repeated: \n {_repeated}")
                    
                    if _dont_exist:
                        logger.warning("[videos_cached] Please check videos dont exist in logs")
                        logger.debug(f"[videos_cached] videos dont exist: \n {_dont_exist}")

                except Exception as e:
                    logger.exception(f"[videos_cached] {repr(e)}")
                finally:
                        _queue.put_nowait(videos_cached)

    
        except Exception as e:
            logger.exception(f"[videos_cached] {repr(e)}")
      
               
    def pasres_periodic(self, event):
        
        logger.debug('[pasres_periodic] START')
        
        try:        
            
            self.pasres_active = True
            
            while not event.is_set():
                
                if self.pasres_repeat and (_list:= list(self.list_pasres)):
                    for _index in _list:
                        self.list_dl[_index-1].pause()
                    wait_time(0.5)
            
                    for _index in _list:
                        self.list_dl[_index-1].resume()
                
                    wait_time(self.pasres_time_from_resume_to_pause)
                
                else:
                    wait_time(self._INTERVAL_GUI)
                    
                if self.stop_console:
                    break

        except Exception as e:
            lines = traceback.format_exception(*sys.exc_info())                
            logger.error(f"[pasres_periodic]: error: {repr(e)}\n{'!!'.join(lines)}")
        finally:
            self.pasres_active = False
            logger.info('[pasres_periodic] END')
      
            
    async def cancel_all_tasks(self):
        
        if self.loop:
            pending_tasks = asyncio.all_tasks(loop=self.loop)
            logger.debug(f"[cancell_all_tasks] {pending_tasks}")
            logger.info(f"[cancel_all_tasks]\n{print_tasks(pending_tasks)}")
            if pending_tasks:
                pending_tasks.remove(self.main_task)
                pending_tasks.remove(self.console_task)
                pending_tasks.remove(self.task_gui_root)
                for task in pending_tasks:
                    task.cancel()
            await asyncio.wait(pending_tasks)
        
            
    async def print_pending_tasks(self):
        if self.loop:
            pending_tasks = asyncio.all_tasks(loop=self.loop)
            logger.debug(f"[pending_all_tasks] {pending_tasks}")
            logger.info(f"[pending_all_tasks]\n{print_tasks(pending_tasks)}")
            sg.cprint(f"[pending_all_tasks]\n{print_tasks(pending_tasks)}")
            
            logger.info(f"[queue_vid] {self.queue_vid._queue}")
            logger.info(f"[run_vid] {self.queue_run._queue}")
            logger.info(f"[manip_vid] {self.queue_manip._queue}")
      
    
    async def gui_log(self):
        
        try:            
            
            self.logwin = init_gui_log()            
            await async_wait_time(self._INTERVAL_GUI)
            
            while not self.stop_console:
                
                await async_wait_time(self._INTERVAL_GUI/2)
                event, values = self.window_console.read(timeout=0)
                if event == sg.TIMEOUT_KEY:
                    continue
                if event  == sg.WIN_CLOSED:
                    break
                
        except BaseException as e:
            if not isinstance(e, asyncio.CancelledError):           
                logger.error(f"[gui_console] Error:{repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
            
        finally:    
            
            self.logwin.close()
            
    
        
    async def gui_console(self):
        
        try:
                        
            logger.debug(f"[gui_console] End waiting. Signal stop_console[{self.stop_console}] stop_root[{self.stop_root}]")
            
            self.window_console = init_gui_console()
            
            await async_wait_time(self._INTERVAL_GUI)
                        
            sg.cprint(f"[pause-resume autom] {self.list_pasres}")
            
            daemon, stop_event = perform_long_operation(self.pasres_periodic)
            
            while(not self.pasres_active):
                await asyncio.sleep(0)
            
            while True:             
                
                await async_wait_time(self._INTERVAL_GUI/2)
                if self.stop_console:
                    self.pasres_repeat = False
                    break                    
                event, values = self.window_console.read(timeout=0)
                if event == sg.TIMEOUT_KEY:
                    continue
                sg.cprint(event, values)
                if event  == sg.WIN_CLOSED:
                    self.pasres_repeat = False
                    break
                elif event in ['Exit']:
                    self.pasres_repeat = False
                    await self.cancel_all_tasks()
                elif event in ['-EXIT-']:
                    self.pasres_repeat = False
                    logger.info(f'[windows_console] event -exit-') 
                    break      
                elif event in ['-WKINIT-']:
                    self.wkinit_stop = not self.wkinit_stop
                    sg.cprint(f'Worker inits: BLOCKED') if self.wkinit_stop else sg.cprint(f'Worker inits: RUNNING')                    
                elif event in ['-PASRES-']:
                    if not values['-PASRES-']: 
                        self.pasres_repeat = False
                    else:
                        self.pasres_repeat = True

                elif event in ['-DL-STATUS']:
                    await self.print_pending_tasks()
                    if not self.console_dl_status:
                        self.console_dl_status = True
                elif event  in  ['IncWorkerRun']:
                    n  =  len(self.extra_tasks_run) + self.workers
                    self.extra_tasks_run.append(self.loop.create_task(self.worker_run(n)))
                    sg.cprint(f'Run Workers: {n} to {len(self.extra_tasks_run) + self.workers}')
                elif event in ['TimePasRes']:
                    if not values['-IN-']:
                        sg.cprint('Please enter number')
                    else:
                        if not values['-IN-'].isdecimal():
                            sg.cprint('not an integer ')
                        else:
                            _time = int(values['-IN-'])
                            if _time <= 0:
                                sg.cprint('must be > 0')
                            else:
                                self.pasres_time_from_resume_to_pause = _time
                                sg.cprint(f'[pasres time to resume] {self.pasres_time_from_resume_to_pause}')

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
                                self.args.parts = _nvidworkers
                                if self.list_dl:
                                    for dl in self.list_dl:
                                        dl.change_numvidworkers(_nvidworkers)
                                else: sg.cprint('DL list empty')
                                
                               
                elif event in ['ToFile', 'Info', 'Pause', 'Resume', 'Reset', 'Stop','+PasRes', '-PasRes']:
                    if not values['-IN-']:
                        if event in ['Reset', 'Stop', '+PasRes', '-PasRes']:
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
                                        with open(Path(Path.home(), "testing", _file:=f"{self.launch_time.strftime('%Y%m%d_%H%M')}.json"), "w") as f:
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
                                        
                                        if event == '+PasRes': 
                                            self.list_pasres.add(_index)
                                            sg.cprint(f"[pause-resume autom] {self.list_pasres}")
                                        if event == '-PasRes': 
                                            self.list_pasres.discard(_index)
                                            sg.cprint(f"[pause-resume autom] {self.list_pasres}")
                                        if event == 'Pause': self.list_dl[_index-1].pause()
                                        if event == 'Resume': self.list_dl[_index-1].resume()
                                        if event == 'Reset': self.list_dl[_index-1].reset()
                                        if event == 'Stop': self.list_dl[_index-1].stop()
                                        if event == 'Info': sg.cprint(self.list_dl[_index-1].info_dict)
                                    else: sg.cprint('DL index doesnt exist')
                            else: sg.cprint('DL list empty')
                            
        except BaseException as e:
            if not isinstance(e, asyncio.CancelledError):           
                logger.error(f"[gui_console] Error:{repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
            
        finally:           
            logger.info("[gui_console] BYE")
            self.window_console.close()  
            self.stop_root = True
            self.pasres_repeat = False
            stop_event.set()
            daemon.join()            
            

    async def gui_root(self):
        '''
        Run a tkinter app in an asyncio event loop.
        '''
                    
        try:
            self.window_root = init_gui_root()
            
            while (not self.list_dl and not self.stop_root):
                
                await async_wait_time(self._INTERVAL_GUI)
                
            logger.debug(f"[gui_root] End waiting. Signal stop: stop_root[{self.stop_root}] {self.stop_root}]")
            
            if not self.stop_root:
                
                #self.window_root = init_gui_root()
                #self.stop_console = False
                await asyncio.sleep(0)
                
                text0 = self.window_root['-ML0-'].TKText
                text1 = self.window_root['-ML1-'].TKText
                text2 = self.window_root['-ML2-'].TKText

                
                list_init_old = []
                list_done_old = []
                
                while True:
                    
                    if self.stop_root:
                        break
                    
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
        
                    
        except BaseException as e:
            if not isinstance(e, asyncio.CancelledError):           
                logger.error(f"[gui_root] Error:{repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:           
            logger.info("[gui_root] BYE")            
            self.window_root.close()
                

    def get_list_videos(self):
        
        
        try:
         
            url_list = []
            _url_list_caplinks = []
            _url_list_cli = []
            self.url_pl_list = {}
            netdna_list = set()
            _url_list = {}
            
            filecaplinks = Path(Path.home(), "Projects/common/logs/captured_links.txt")
            if self.args.caplinks and filecaplinks.exists():
                _temp = set()
                with open(filecaplinks, "r") as file:
                    for _url in file:
                        if (_url:=_url.strip()): _temp.add(_url)           
                    
                _url_list_caplinks = list(_temp)
                logger.info(f"[video list caplinks]\n{_url_list_caplinks}")
                
                
                shutil.copy("/Users/antoniotorres/Projects/common/logs/captured_links.txt", 
                            "/Users/antoniotorres/Projects/common/logs/prev_captured_links.txt")
                with open(filecaplinks, "w") as file:
                    file.write("")
                    
                _url_list['caplinks'] = _url_list_caplinks
            
            if self.args.collection:                
                _url_list_cli = list(set(self.args.collection)) 
                logger.info(f"[video list cli]\n{_url_list_cli}")
                
                _url_list['cli'] = _url_list_cli   
            
            
            logger.info(f"[get_list_videos] Initial # urls:\n\tCLI[{len(_url_list_cli )}]\n\tCAP[{len(_url_list_caplinks)}]")
            
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
                                                    'todl':True,
                                                    'ie_key': ie_key, 
                                                    'error': []}

                            self._prepare_for_dl(_elurl)
                            self.list_videos.append(_entry)
                
                    else:
                        if not self.url_pl_list.get(_elurl):
                            self.url_pl_list[_elurl] = {'source': _source}

            

            url_list = list(self.info_videos.keys())
            
            logger.info(f"[url_list] Initial number of urls not pl [{len(url_list)}]")
            logger.debug(f"[url_list] {url_list}")
                    
            if netdna_list:
                logger.info(f"[netdna_list]: {netdna_list}")
                _ies_netdna = self.ytdl.get_info_extractor('NetDNA')
                 
                with ThreadPoolExecutor(thread_name_prefix="Get_netdna", max_workers=min(self.init_nworkers, len(netdna_list))) as ex:
                     
                    futures = [ex.submit(_ies_netdna.get_entry, _url_netdna) for _url_netdna in netdna_list]

                for fut,_url_netdna in zip(futures, netdna_list):
                    try:
                        _entry_netdna = fut.result()                        
                        
                        self.info_videos[_url_netdna]['video_info'] = _entry_netdna
                        self._prepare_for_dl(_url_netdna)
                        self.list_videos.append(self.info_videos[_url_netdna]['video_info'])
                       
                        
                    except Exception as e:
                        self.info_videos[_url_netdna]['error'].append(str(e))
                        self.info_videos[_url_netdna]['status'] = 'prenok'
                        self.info_videos[_url_netdna]['todl'] = True                        
                        
                        logger.error(repr(e))
                
                        
            if self.url_pl_list:
                
                logger.info(f"[url_playlist_list] Initial number of urls that are pl [{len(self.url_pl_list)}]")
                logger.debug(f"[url_playlist_list]\n{self.url_pl_list}")                 
                self._url_pl_entries = []                
                self._count_pl = 0                
                self.futures = {}
                self.futures2 = {}
                
                if len(self.url_pl_list) == 1 and self.args.use_path_pl:
                    _get_name = True
                else:
                    _get_name = False

                def process_playlist(_url, _get):                        
                    
                    try:
                        
                        if self.reset: 
                            raise Exception("reset")
                        with self.lock:
                            self._count_pl += 1
                            logger.info(f"[url_playlist_list][{self._count_pl}/{len(self.futures) + len(self.futures2)}] processing {_url}")
                        try:
                            _errormsg = None
                            _info = self.ytdl.extract_info(_url, download=False, process=False)
                        except Exception as e:
                            logger.error(repr(e))
                            _info = None
                            _errormsg = repr(e)
                        if not _info:
                            _info = {'_type': 'error', 'url': _url, 'error': _errormsg or 'no video entry'}
                            self._prepare_entry_pl_for_dl(_info)
                            self._url_pl_entries += [_info]
                        elif _info:                                
                                
                            if _info.get('_type', 'video') != 'playlist': #caso generic que es playlist default, pero luego puede ser url, url_trans
                                _info = self.ytdl.sanitize_info(self.ytdl.process_ie_result(_info, download=False))
                                if not _info.get('original_url'): _info.update({'original_url': _url})
                                
                                self._prepare_entry_pl_for_dl(_info)
                                self._url_pl_entries += [_info]
                            else:   
                                if _get and not self.args.path:                             
                                    _name = f"{_info.get('title')}{_info.get('extractor_key')}{_info.get('id')}"
                                    self.args.path = str(Path(Path.home(), 'testing', _name))
                                    logger.info(f"[path for pl] {self.args.path}")
                                
                                if isinstance(_info.get('entries'), list):
                                    _info = self.ytdl.process_ie_result(_info, download=False)
                                    if (_info.get('extractor_key') in ('GVDBlogPost','GVDBlogPlaylist')):
                                        _temp_aldl = [] 
                                        _temp_nodl = []
                                        for _ent in _info.get('entries'):
                                            if not self._check_if_aldl(_ent, test=True): 
                                                _temp_nodl.append(_ent)
                                            else: _temp_aldl.append(_ent)
                                    
                                        def get_list_interl(res):
                                            if not res:
                                                return []
                                            if len(res) < 3:
                                                return res
                                            _dict = {}
                                            for ent in res:
                                                _key = get_domain(ent['url'])
                                                if not _dict.get(_key): _dict[_key] = [ent]
                                                else: _dict[_key].append(ent)       
                                            logger.info(f'[url_playlist_list][{_url}] gvdblogplaylist entries interleave: {len(list(_dict.keys()))} different hosts, longest with {len(max(list(_dict.values()), key=len))} entries')                                        
                                            _interl = []
                                            for el in list(zip_longest(*list(_dict.values()))):
                                                _interl.extend([_el for _el in el if _el])
                                            return _interl 
                                                                            
                                        _info['entries'] = get_list_interl(_temp_nodl) + _temp_aldl
                                   
                                
                                for _ent in _info.get('entries'):                                    
                                    
                                    if not isinstance(_info.get('entries'), list):
                                        _ent = self.ytdl.process_ie_result(_ent, download=False)
                                    
                                    _ent = self.ytdl.sanitize_info(_ent)
                                    if _ent.get('_type', 'video') == 'video':
                                        if not _ent.get('original_url'): 
                                            _ent.update({'original_url': _url})
                                        if ((_ent.get('extractor') == 'generic') or (_ent.get('ie_key') == 'Generic'))  and (_ent.get('n_entries',0) <= 1):
                                                _ent.pop("playlist","")
                                                _ent.pop("playlist_index","")
                                                _ent.pop("n_entries","")
                                                _ent.pop("playlist", "")
                                                _ent.pop('playlist_id',"")
                                                _ent.pop('playlist_title','')
                                        
                                        if ((_wurl:=_ent['webpage_url']) == _ent['original_url']):
                                            if _ent.get('n_entries', 0) > 1:
                                                _ent.update({'webpage_url': f"{_wurl}?id={_ent['playlist_index']}"})
                                                
                                        self._prepare_entry_pl_for_dl(_ent)
                                        self._url_pl_entries += [_ent]
                                    else:    
                                        try:
                                            is_pl, ie_key = is_playlist_extractor(_ent['url'], self.ytdl)
                                            _error = _ent.get('error')
                                            if not is_pl or _error:
                                                if not _ent.get('original_url'): _ent.update({'original_url': _url})
                                                if _error: _ent['_type'] = "error"
                                                self._prepare_entry_pl_for_dl(_ent)
                                                self._url_pl_entries.append(_ent)
                                            else:

                                                self.futures2.update({self.ex_pl.submit(process_playlist, _ent['url'], False): _ent['url']})

                                        except Exception as e:
                                            logger.error(f"[url_playlist_list][{_url}]:{_ent['url']} no video entries - {repr(e)}")
                   
                    except BaseException as e:
                        logger.exception(f"[url_playlist_list] {repr(e)}")
                        if isinstance(e, KeyboardInterrupt):
                            raise
                
                if self.reset: raise Exception("reset")
                
                with ThreadPoolExecutor(thread_name_prefix="GetPlaylist", max_workers=self.init_nworkers) as self.ex_pl:
                
                    for url in self.url_pl_list:    
                        if self.reset: raise Exception("reset")
                        self.futures.update({self.ex_pl.submit(process_playlist, url, _get_name): url}) 
                
                    logger.info(f"[url_playlist_list] initial playlists: {len(self.futures)}")
                
                    wait(list(self.futures))
                
                    logger.info(f"[url_playlist_list] playlists from initial playlists: {len(self.futures2)}")
                
                    if self.reset: raise Exception("reset")
                    
                    if self.futures2:
                        wait(list(self.futures2))
                
                
                if self.reset: raise Exception("reset")
                
                logger.info(f"[url_playlist_list] entries from playlists: {len(self._url_pl_entries)}")
                logger.debug(f"[url_playlist_list] {self._url_pl_entries}")
                

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
                                                    'todl': True,
                                                    'error': []}
                        
                        _same_video_url = self._check_if_same_video(_url)
                        
                        if _same_video_url:
                            
                            self.info_videos[_url].update({'samevideo': _same_video_url})
                            logger.warning(f"{_url}: has not been added to video list because it gets same video than {_same_video_url}")
                            self._prepare_for_dl(_url)
                        
                        else:
                            self._prepare_for_dl(_url)
                            self.list_videos.append(self.info_videos[_url]['video_info'])


            logger.debug(f"[get_list_videos] list videos: \n{self.list_videos}")
            logger.debug(f"[get_list_videos] status de info_videos: \n{self.info_videos}")
            
        
        except BaseException as e:            
            logger.error(f"[get_videos]: Error {repr(e)}")
            
        finally:            
            for _ in range(self.init_nworkers - 1):
                self.queue_vid.put_nowait("KILL")        
            self.queue_vid.put_nowait("KILLANDCLEAN")
            self.getlistvid_done = True
            self.t1.stop()
                
 
    def _check_if_aldl(self, info_dict, test=False):  
                    

        
        if not (_id := info_dict.get('id') ) or not ( _title := info_dict.get('title')):
            return False
        
        _title = sanitize_filename(_title, restricted=True).upper()
        vid_name = f"{_id}_{_title}"                    

        if not (vid_path_str:=self.videos_cached.get(vid_name)):            
            return False        
        
        else: #video en local            
            
            if test: return True
                
            vid_path = Path(vid_path_str)
            logger.debug(f"[{vid_name}] already DL: {vid_path}")
                
                                   
            if not self.args.nosymlinks:
                if self.args.path:
                    _folderpath = Path(self.args.path)
                else:
                    _folderpath = Path(Path.home(), "testing", self.launch_time.strftime('%Y%m%d'))
                _folderpath.mkdir(parents=True, exist_ok=True)
                file_aldl = Path(_folderpath, vid_path.name)
                if file_aldl not in _folderpath.iterdir():
                    file_aldl.symlink_to(vid_path)
                    try:
                        mtime = int(vid_path.stat().st_mtime)
                        file_aldl._accessor.utime(file_aldl, (int(datetime.timestamp(datetime.now())), mtime), follow_symlinks=False)
                    except Exception as e:
                        logger.debug(f'[check_if_aldl] [{str(file_aldl)}] -> [{str(vid_path)}] error when copying times {repr(e)}')
                
                
            return vid_path_str
    
    
    def _check_if_same_video(self, url_to_check):
        
        info = self.info_videos[url_to_check]['video_info']
        
        if info.get('_type', 'video') == 'video' and (_id:=info.get('id')) and (_title:=info.get('title')):            
            
            for (urlkey, _vid) in  self.info_videos.items():
                if urlkey != url_to_check:
                    if _vid['video_info'].get('_type', 'video') == 'video' and (_vid['video_info'].get('id', "") == _id) and (_vid['video_info'].get('title', "")) == _title:
                        return(urlkey)
        
    def _prepare_for_dl(self, url, put=True):
        self.info_videos[url].update({'todl': True})
        if (_id:=self.info_videos[url]['video_info'].get('id')):
            self.info_videos[url]['video_info']['id'] = sanitize_filename(_id, restricted=True).replace('_', '').replace('-','')
        if (_title:=self.info_videos[url]['video_info'].get('title')):
            self.info_videos[url]['video_info']['title'] = sanitize_filename(_title[:150], restricted=True)
        if not self.info_videos[url]['video_info'].get('filesize', None):
            self.info_videos[url]['video_info']['filesize'] = 0
        if (_path:=self._check_if_aldl(self.info_videos[url]['video_info'])):  
            self.info_videos[url].update({'aldl' : _path, 'status': 'done'})
            logger.debug(f"[prepare_for_dl] [{self.info_videos[url]['video_info'].get('id')}][{self.info_videos[url]['video_info'].get('title')}] already DL")            

        if self.info_videos[url].get('todl') and not self.info_videos[url].get('aldl') and not self.info_videos[url].get('samevideo') and self.info_videos[url].get('status') != 'prenok':
            with self.lock:
                self.totalbytes2dl += none_to_cero(self.info_videos[url].get('video_info', {}).get('filesize', 0))
                self.videos_to_dl.append(url)
                if put: self.queue_vid.put_nowait(url)
                self.num_videos_to_check += 1
                self.num_videos_pending += 1
            
    def _prepare_entry_pl_for_dl(self, entry):
        
        _type = entry.get('_type', 'video')
        if _type == 'playlist':
            logger.warning(f"[prepare_entry_pl_for_dl] PLAYLIST IN PLAYLIST: {entry}")
            return
        elif _type == 'error':
            _errorurl = entry.get('url')
            if _errorurl and not self.info_videos.get(_errorurl):
                
                self.info_videos[_errorurl] = {'source' : self.url_pl_list.get(_errorurl,{}).get('source') or 'playlist',
                                            'video_info': {}, 
                                            'status': 'prenok',
                                            'todl': True,                                                         
                                            'error': [entry.get('error', 'no video entry')]}
                if any(_ in str(entry.get('error', 'no video entry')).lower() for _ in ['not found', '404', 'flagged', '403', '410', 'suspended', 'unavailable', 'disabled']): self.list_notvalid_urls.append(_errorurl)                                    
                else: self.list_urls_to_check.append((_errorurl, entry.get('error', 'no video entry')))
                self.list_initnok.append((_errorurl, entry.get('error', 'no video entry')))
            return
            
        elif _type == 'video':                        
            _url = entry.get('webpage_url') or entry.get('url')
            
        else: #url, url_transparent
            _url = entry.get('url')
        
        if not self.info_videos.get(_url): #es decir, los nuevos videos 
            
            self.info_videos[_url] = {'source' : 'playlist', 
                                        'video_info': entry, 
                                        'status': 'init', 
                                        'aldl': False,
                                        'todl': True,
                                        'ie_key': entry.get('ie_key') or entry.get('extractor_key'),
                                        'error': []}
            
            _same_video_url = self._check_if_same_video(_url)
            
            if _same_video_url: 
                
                self.info_videos[_url].update({'samevideo': _same_video_url})
                
                logger.warning(f"[prepare_entry_pl_for_dl] {_url}: has not been added to video list because it gets same video than {_same_video_url}")
                
                self._prepare_for_dl(_url)
            else:
                self._prepare_for_dl(_url)
                self.list_videos.append(self.info_videos[_url]['video_info'])
        else:
            logger.warning(f"[prepare_entry_pl_for_dl] {_url}: has not been added to info_videos because it is already")
            
    async def worker_init(self, i):
        #worker que lanza la creación de los objetos VideoDownloaders, uno por video
        
        logger.debug(f"[worker_init][{i}]: launched")
        await asyncio.sleep(0)

        try:
            
            while True:
                if self.getlistvid_done: 
                    break
                if self.queue_vid.qsize() < 2:
                    await asyncio.sleep(0)
                else: break

            while True:

                
                url_key = await self.queue_vid.get()

                if url_key == "KILL":
                    logger.debug(f"[worker_init][{i}]: finds KILL")
                    break
                elif url_key == "KILLANDCLEAN":
                    logger.debug(f"[worker_init][{i}]: finds KILLANDCLEAN")
                    
                    while True:
                        async with self.alock:
                            if (_val:=self.count_init) == (self.init_nworkers - 1):
                                break
                            logger.debug(f"[worker_init][{i}]: bucle while [count_init] {self.count_init} [init_workers] {self.init_workers} [val] {_val}")
                        
                        await asyncio.sleep(5)
                    
                              
                    
                    async with self.alock:
                        for _ in range(self.workers - 1):
                            self.queue_run.put_nowait(("", "KILL"))
                        
                        self.queue_run.put_nowait(("", "KILLANDCLEAN"))
                    
                    self.t3.stop()          
                    
                    if not self.list_dl: 
                        #self.stop_root = True
                        self.stop_console = False
                        self.pasres_repeat = False
                        #await asyncio.sleep(0)
                    
 
                    break
                
                else: 
                    async with self.alock:
                        _pending = self.num_videos_pending
                        _to_check = self.num_videos_to_check
                    
                    vid = self.info_videos[url_key]['video_info']
                    logger.debug(f"[worker_init][{i}]: [{url_key}] extracting info\n{vid}")
                    
                    try: 
                        if self.wkinit_stop:
                            logger.info(f"[worker_init][{i}]: BLOCKED")
                            
                            while self.wkinit_stop:
                                await async_wait_time(5)
                                logger.debug(f"[worker_init][{i}]: BLOCKED")
                                
                            logger.info(f"[worker_init][{i}]: UNBLOCKED")
                        
                        if vid.get('_type', 'video') != 'video':
                            #al no tratarse de video final vid['url'] siempre existe
                            try:                                    
 
                                _ext_info = try_get(vid.get('original_url'), lambda x: {'original_url': x}) or {}
                                logger.debug(f"[worker_init][{i}]: [{url_key}] extra_info={_ext_info or vid}")
                                _res = await async_ex_in_executor(self.ex_winit, self.ytdl.extract_info, vid['url'], download=False, extra_info=_ext_info)
                                if not _res: raise Exception("no info video")
                                info = self.ytdl.sanitize_info(_res)
                                logger.debug(f"[worker_init][{i}]: [{url_key}] info extracted\n{info}")
                            
                            except Exception as e: 
                                
                                if 'unsupported url' in str(e).lower():                                    
                                    self.list_unsup_urls.append(url_key)
                                    _error = 'unsupported_url'
                                    
                                elif any(_ in str(e).lower() for _ in ['not found', '404', 'flagged', '403', '410', 'suspended', 'unavailable', 'disabled']):
                                    _error = 'not_valid_url'
                                    self.list_notvalid_urls.append(url_key)                                    
                                    
                                else: 
                                    _error = repr(e)
                                    self.list_urls_to_check.append((url_key, _error))
                                   
                                
                                self.list_initnok.append((url_key, _error))
                                self.info_videos[url_key]['error'].append(_error)
                                self.info_videos[url_key]['status'] = 'initnok'
                                
                                logger.error(f"[worker_init][{i}]: [{_pending}/{_to_check}] [{url_key}] init nok - {_error}")                                
                                    
                                continue

                        else: 
                            info = vid
                        
                        

                        async def go_for_dl(urlkey ,infdict, extradict=None):                   
                            #sanitizamos 'id', y si no lo tiene lo forzamos a un valor basado en la url
                            if (_id:=infdict.get('id')):
                                
                                infdict['id'] = sanitize_filename(_id, restricted=True).replace('_', '').replace('-','')
                                
                            else:
                                infdict['id'] = str(int(hashlib.sha256(urlkey.encode('utf-8')).hexdigest(),16) % 10**8)

                            
                            if (_title:=infdict.get('title')):
                                _title = sanitize_filename(_title[:150], restricted=True)
                                infdict['title'] = _title
                                
                            if extradict:
                                if not infdict.get('release_timestamp') and (_mtime:=extradict.get('release_timestamp')):
                                    
                                    infdict['release_timestamp'] = _mtime
                                    infdict['release_date'] = extradict.get('release_date')
                                
                            logger.debug(f"[worker_init][{i}]: [{_pending}/{_to_check}] [{infdict.get('id')}][{infdict.get('title')}] info extracted")                        
                            logger.debug(f"[worker_init][{i}]: [{urlkey}] info extracted\n{infdict}")
                            
                            self.info_videos[urlkey].update({'video_info': infdict})
 
                            _filesize = none_to_cero(extradict.get('filesize', 0)) if extradict else none_to_cero(infdict.get('filesize', 0))
                            
                            if (_path:=self._check_if_aldl(infdict)):
                                
                                logger.debug(f"[worker_init][{i}][{infdict.get('id')}][{infdict.get('title')}] already DL")                               
                                
                                
                                if _filesize:
                                    async with self.alock:
                                        self.totalbytes2dl -= _filesize
                                    
                                self.info_videos[urlkey].update({'status': 'done', 'aldl': _path})                                        
                                return False
                            
                            if (_same_video_url:=self._check_if_same_video(urlkey)):
                                                                
                                if _filesize:
                                    async with self.alock:
                                        self.totalbytes2dl -= _filesize
                                
                                self.info_videos[urlkey].update({'samevideo': _same_video_url})
                                logger.warning(f"[{urlkey}]: has not been added to video list because it gets same video than {_same_video_url}")
                                return False                             
                            
                            return True
                    
                        async def get_dl(urlkey ,infdict, extradict=None):

                            if (await go_for_dl(urlkey ,infdict, extradict)) and not self.args.nodl:
                                
                                                                    
                                dl = await async_ex_in_executor(self.ex_winit, VideoDownloader, self.info_videos[urlkey]['video_info'], self.ytdl, self.args, self.hosts_downloading, self.alock, self.hosts_alock)
                                
                                logger.debug(f"[worker_init][{i}]: [{dl.info_dict['id']}][{dl.info_dict['title']}]: {dl.info_dl}")
                                
                                _filesize = none_to_cero(extradict.get('filesize', 0)) if extradict else none_to_cero(infdict.get('filesize', 0))       
                                
                                if not dl.info_dl.get('status', "") == "error":
                                    
                                    if dl.info_dl.get('filesize'):
                                        self.info_videos[urlkey]['video_info']['filesize'] = dl.info_dl.get('filesize')
                                        async with self.alock:
                                            self.totalbytes2dl = self.totalbytes2dl - _filesize + dl.info_dl.get('filesize', 0)
                                            
                                    self.info_videos[urlkey].update({'status': 'initok', 'filename': dl.info_dl.get('filename'), 'dl': dl})

                                    async with self.alock:
                                        self.list_dl.append(dl)
                                        
                                    
                                    if dl.info_dl['status'] in ("init_manipulating", "done"):
                                        self.queue_manip.put_nowait((urlkey, dl))
                                        logger.info(f"[worker_init][{i}][{dl.info_dict['id']}][{dl.info_dict['title']}] init OK, video parts DL")
                                    
                                    else:
                                        logger.debug(f"[worker_init][{i}][{dl.info_dict['id']}][{dl.info_dict['title']}] init OK, lets put in run queue")
                                        async with self.alock:
                                            self.queue_run.put_nowait((urlkey, dl))
                                        _msg = ''
                                        async with self.alock:
                                            if dl.info_dl.get('auto_pasres'):
                                                _index_in_dl = len(self.list_dl)
                                                self.list_pasres.add(_index_in_dl)
                                                _msg = f', add this dl[{_index_in_dl}] to auto_pasres{list(self.list_pasres)}'
                                                if self.window_console and not self.stop_root: sg.cprint(f"[pause-resume autom] {self.list_pasres}")
                                                                            
                                        logger.debug(f"[worker_init][{i}][{dl.info_dict['id']}][{dl.info_dict['title']}] init OK, ready to DL{_msg}")
                                
                                else:
                                    async with self.alock:
                                        self.totalbytes2dl -= _filesize
                                    
                                            
                                    raise Exception("no DL init")

                        if (_type:=info.get('_type', 'video')) == 'video': 
                            
                            if self.wkinit_stop:
                                logger.info(f"[worker_init][{i}]: BLOCKED")
                            
                                while self.wkinit_stop:
                                    await async_wait_time(5)
                                    logger.debug(f"[worker_init][{i}]: BLOCKED")
                                
                                logger.info(f"[worker_init][{i}]: UNBLOCKED")
                                
                            await get_dl(url_key, infdict=info, extradict=vid)

                        
                        elif _type == 'playlist':
                            
                            logger.warning(f"[worker_init][{i}]: [{url_key}] playlist en worker_init")
                            self.info_videos[url_key]['todl'] = False
                            
                            for _entry in info['entries']:
                                
                                try:
                                
                                    if (_type:=_entry.get('_type', 'video')) != 'video':
                                        logger.warning(f"[worker_init][{i}]: [{url_key}] playlist of entries that are not videos")
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
                                                self._prepare_for_dl(_url,put=False)
                                        
                                            else:
                                                
                                                try:
                                                    self._prepare_for_dl(_url, put=False)
                                                    if self.wkinit_stop:
                                                        logger.info(f"[worker_init][{i}]: BLOCKED")
                                
                                                        while self.wkinit_stop:
                                                            await async_wait_time(5)
                                                            logger.debug(f"[worker_init][{i}]: BLOCKED")
                                    
                                                        logger.info(f"[worker_init][{i}]: UNBLOCKED")
                                                    await get_dl(_url, infdict=_entry)
                                                    
                                                    
                                                except Exception as e:
                                                    raise
                                                finally:
                                                    async with self.alock:
                                                        self.num_videos_pending -= 1
                                                    
                                
                                except Exception as e:
                                    
                                    self.list_initnok.append((_entry, f"Error:{repr(e)}"))
                                    logger.error(f"[worker_init][{i}]: [{_url}] init nok - Error:{repr(e)}")
                                    
                                    self.list_urls_to_check.append((_url,repr(e)))
                                    self.info_videos[_url]['error'].append(f'DL constructor error:{repr(e)}')
                                    self.info_videos[_url]['status'] = 'initnok'                                    
                                
                                finally:
                                    await asyncio.sleep(0)
                                    continue
                                    

                    except Exception as e:
                        
                        self.list_initnok.append((vid, f"Error:{repr(e)}"))
                        logger.error(f"[worker_init][{i}]: [{_pending}/{_to_check}] [{url_key}] init nok - Error:{repr(e)}")
                        
                        self.list_urls_to_check.append((url_key,repr(e)))
                        self.info_videos[url_key]['error'].append(f'DL constructor error:{repr(e)}')
                        self.info_videos[url_key]['status'] = 'initnok'


                    finally:
                        async with self.alock:
                            self.num_videos_pending -= 1
                        await asyncio.sleep(0)
                        continue
                               
        
        except BaseException as e:           
            logger.error(f"[worker_init][{i}]: Error:{repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
                    
        finally:
            async with self.alock:
                self.count_init += 1                
            logger.debug(f"[worker_init][{i}]: BYE")
            await asyncio.sleep(0)
    
    
    async def worker_run(self, i):

        logger.debug(f"[worker_run][{i}]: launched")       

        try:
            
            while True:
                await asyncio.sleep(0)
                url_key, video_dl = await self.queue_run.get()
                logger.debug(f"[worker_run][{i}]: get for a video_DL")
                
                if video_dl == "KILL":
                    logger.debug(f"[worker_run][{i}]: get KILL, bye")                    
                    break
                
                elif video_dl == "KILLANDCLEAN":
                    logger.debug(f"[worker_run][{i}]: get KILLANDCLEAN, bye")  
                    
                    async with self.alock:
                        nworkers  =  self.workers
                        if  (_inc:=(len(self.extra_tasks_run) + self.workers  - nworkers)) > 0:
                            logger.info(f"[worker_run][{i}] nworkers[{nworkers}] inc[{_inc}]")
                            for  _  in  range(_inc):
                                self.queue_run.put_nowait(("", "KILL"))
                            nworkers  +=  _inc
                        
                    logger.debug(f"[worker_run][{i}]: countrun[{self.count_run}] nworkers[{nworkers}]") 
                     
                    while True:
                        async with self.alock:
                            
                            if (_val:=self.count_run) == (nworkers - 1):
                                break
                        
                        logger.debug(f"[worker_run][{i}]: bucle while [count_run] {_val} [nworkers] {nworkers}")
                    
                        await asyncio.sleep(5)
                        
                        async with self.alock:
                        
                            if  (_inc:=(len(self.extra_tasks_run) + self.workers  - nworkers)) > 0:
                                logger.info(f"[worker_run][{i}] nworkers[{nworkers}] inc[{_inc}]")
                                for  _  in  range(_inc):
                                    self.queue_run.put_nowait(("", "KILL"))
                                nworkers  +=  _inc
                                
                        logger.debug(f"[worker_run][{i}]: countrun[{self.count_run}] nworkers[{nworkers}]")
                        
                      
                    for _ in range(self.workers):
                        self.queue_manip.put_nowait(("", "KILL")) 
                                            
                    self.stop_console = True
                    self.pasres_repeat = False
                    
                    break
                
                else:
                    
                    logger.debug(f"[worker_run][{i}][{video_dl.info_dl['id']}] start to dl {video_dl.info_dl['title']}")                    
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
                                
        except BaseException as e:            
            logger.exception(f"[worker_run][{i}]: Error:{repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
        
        finally:
            async with self.alock:
                self.count_run += 1 
            logger.debug(f"[worker_run][{i}]: BYE")
            await asyncio.sleep(0)

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
                        
        except BaseException as e:
            logger.error(f"[worker_manip][{i}]: Error:{repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:
            async with self.alock:
                self.count_manip += 1 
            logger.debug(f"[worker_manip][{i}]: BYE")
            await asyncio.sleep(0)  

    async def async_ex(self):
    
        self.queue_run = asyncio.Queue()
        self.queue_manip = asyncio.Queue()
        self.alock = asyncio.Lock()
        self.hosts_alock = asyncio.Lock()
        
        self.queue_vid = asyncio.Queue()

        try:

            self.t1.start()
            self.t2.start()
            self.t3.start()
            self.loop  =  asyncio.get_running_loop()
            self.loop.call_soon_threadsafe
            
            tasks_gui = []
            self.extra_tasks_run = []
            self.proc_gost = []
            self.stop_proxy = None
            
            tasks_to_wait = {}

            tasks_to_wait.update({asyncio.create_task(async_ex_in_executor(self.ex_winit, self.get_list_videos)): 'task_get_videos'})
            tasks_to_wait.update({asyncio.create_task(self.worker_init(i)): f'task_worker_init_{i}' for i in range(self.init_nworkers)})
                            
            if not self.args.nodl:                

                #aria2c
                
                if self.args.aria2c:             
                    init_aria2c(self.args)
                    if self.args.proxy != 0:
                        self.proc_gost = init_proxies(CONF_PROXIES_MAX_N_GR_HOST, CONF_PROXIES_N_GR_VIDEO)                
                        self.stop_proxy = run_proxy_http() #launch as thread daemon proxy helper in dl of aria2
                
                self.task_gui_root = asyncio.create_task(self.gui_root())
                self.console_task = asyncio.create_task(self.gui_console())
                tasks_gui = [self.task_gui_root, self.console_task] 
                                
                tasks_to_wait.update({asyncio.create_task(self.worker_run(i)):f'task_worker_run_{i}' for i in range(self.workers)})   
                tasks_to_wait.update({asyncio.create_task(self.worker_manip(i)): f'task_worker_manip_{i}' for i in range(self.workers)})
                


            done, _ = await asyncio.wait(tasks_to_wait)          
            for d in done:
                try:
                    d.result()
                except BaseException as e:                                   
                    logger.error(f"[async_ex][{tasks_to_wait[d]}] {repr(e)}")
                    if isinstance(e, KeyboardInterrupt):
                        raise
            
            if self.extra_tasks_run:
                        
                done, _ = await asyncio.wait(self.extra_tasks_run)
                for d in done:
                    try:
                        d.result()
                    except BaseException as e:                                   
                        logger.error(f"[async_ex][{d}] {repr(e)}")
                        if isinstance(e, KeyboardInterrupt):
                            raise
            
                                  

        except BaseException as e:                            
            logger.exception(f"[async_ex] {repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:

            self.stop_console = True
            await asyncio.sleep(0)
            self.stop_root = True 
            await asyncio.sleep(0)          
            done, _ = await asyncio.wait(tasks_gui)
            if any(isinstance(_e, KeyboardInterrupt) for _e in [d.exception() for d in done]):
                raise KeyboardInterrupt
            
    def get_results_info(self):
            
        def _getter(url, vid):
            webpageurl = vid.get('video_info',{}).get('webpage_url')
            originalurl = vid.get('video_info', {}).get('original_url')
            playlist = vid.get('video_info', {}).get('playlist')
            if not webpageurl and not originalurl and not playlist:
                return url
            if playlist and vid['video_info']['n_entries'] > 1:
                return f"{playlist}:[{vid['video_info']['playlist_index']}]:{url}"
            else:
                return(originalurl or webpageurl)

        def _print_list_videos():
            try:
                
                col = shutil.get_terminal_size().columns
                
                list_videos = [_getter(url, vid) for url, vid in self.info_videos.items() if vid.get('todl')]
                
                if list_videos:
                    list_videos_str = [[fill(url, col//2)] for url in list_videos]
                else:
                    list_videos_str = []
                
                list_videos2dl = [_getter(url, vid) for url, vid in self.info_videos.items() if not vid.get('aldl') and not vid.get('samevideo') and vid.get('todl') and vid.get('status') != "prenok"]
                
                list_videos2dl_str = [[fill(vid['video_info'].get('id', ''),col//5), fill(vid['video_info'].get('title', ''), col//5), naturalsize(none_to_cero(vid['video_info'].get('filesize',0))), fill(_getter(url, vid), col//3)] for url, vid in self.info_videos.items() if not vid.get('aldl') and not vid.get('samevideo') and vid.get('todl') and vid.get('status') != "prenok"] if list_videos2dl else []
                
                list_videosaldl = [_getter(url, vid) for url, vid in self.info_videos.items() if vid.get('aldl') and vid.get('todl')]
                list_videosaldl_str = [[fill(vid['video_info'].get('id', ''),col//5), fill(vid['video_info'].get('title', ''), col//5), fill(_getter(url, vid), col//3), fill(vid['aldl'], col//3)] for url, vid in self.info_videos.items() if vid.get('aldl') and vid.get('todl')] if list_videosaldl else []
                
                list_videossamevideo = [_getter(url, vid) for url, vid in self.info_videos.items() if vid.get('samevideo')]
                list_videossamevideo_str = [[fill(vid['video_info'].get('id', ''),col//5), fill(vid['video_info'].get('title', ''), col//5), fill(_getter(url, vid), col//3), fill(vid['samevideo'], col//3)] for url, vid in self.info_videos.items() if vid.get('samevideo')] if list_videossamevideo else []
                
                
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
            except Exception as e:
                logger.exception(repr(e))

        
        _videos_url_notsupported = self.list_unsup_urls
        _videos_url_notvalid = self.list_notvalid_urls
        _videos_url_tocheck = [_url for _url, _ in self.list_urls_to_check] if self.list_urls_to_check else []
        _videos_url_tocheck_str = [f"{_url}:{_error}" for _url, _error in self.list_urls_to_check] if self.list_urls_to_check else []       
       
        logger.debug(f'[get_result_info]\n{self.info_videos}')  
            
        videos_okdl = []
        videos_kodl = []        
        videos_koinit = []     
        
        local_storage = Path(Path.home(),"Projects/common/logs/files_cached.json")
        
        with open(local_storage,"r") as f:
            _temp = json.load(f)
        

        for url, video in self.info_videos.items():
            if not video.get('aldl') and not video.get('samevideo') and video.get('todl'):
                if video['status'] == "done":
                    videos_okdl.append(_getter(url, video))
                    _temp['local'].update({f"{traverse_obj(video, ('video_info', 'id'))}_{traverse_obj(video, ('video_info', 'title')).upper()}": str(traverse_obj(video, 'filename'))})
                else:                    
                    if ((video['status'] == "initnok") or (video['status'] == "prenok")):
                        videos_kodl.append(_getter(url, video))
                        videos_koinit.append(_getter(url, video))
                    elif video['status'] == "initok":
                        if self.args.nodl: videos_okdl.append(_getter(url, video))
                    else: videos_kodl.append(_getter(url, video))
        
        with open(local_storage, "w") as f:
            json.dump(_temp, f)
            
            
        info_dict = _print_list_videos()
        
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
 
    def ies_close(self):
        
        ies = self.ytdl._ies_instances
        
        if not ies: return
                
        for ie, ins in ies.items():
            
            if (close:=getattr(ins, 'close', None)):
                try:
                    close()
                    logger.info(f"[close][{ie}] closed ok")                    
                except Exception as e:
                    logger.exception(f"[close][{ie}] {repr(e)}")
    
    def close(self):
        
        try:
            self.t2.stop()
        except Exception as e:
            pass        
        try:        
            self.ies_close()
        except Exception as e:
            logger.exception(f"[close] {repr(e)}")
        
        if self.proc_gost:
            for proc in self.proc_gost:
                try:
                    proc.kill()
                except Exception as e:
                    logger.exception(f"[close] {repr(e)}")
        
        if self.stop_proxy:
            try:                
                self.stop_proxy.set()
                time.sleep(2)
            except Exception as e:
                logger.exception(f"[close] {repr(e)}")       
            
        try:
            kill_processes(logger=logger, rpcport=self.args.rpcport) 
        except Exception as e:
            logger.exception(f"[close] {repr(e)}")
            
        

    def clean(self):
        
        self.p1.join()
                
        try:
            current_res = Path(Path.home(),"Projects/common/logs/current_res.json")
            if current_res.exists():
                current_res.unlink()
        except Exception as e:
            logger.exception(f"[clean] {repr(e)}")
          
    