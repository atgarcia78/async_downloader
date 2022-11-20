import asyncio
import hashlib
import json
import logging
import shutil
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from itertools import zip_longest
from pathlib import Path
from statistics import median
from textwrap import fill
from threading import Lock

import psutil
from codetiming import Timer
from tabulate import tabulate

from utils import (CONF_INTERVAL_GUI, CONF_PROXIES_MAX_N_GR_HOST,
                   CONF_PROXIES_N_GR_VIDEO, EMA, PATH_LOGS, LocalStorage,
                   _for_print, _for_print_videos, async_ex_in_executor,
                   async_wait_time, get_chain_links, get_domain, init_aria2c,
                   init_gui_console, init_gui_root, init_proxies, init_ytdl,
                   is_playlist_extractor, js_to_json, kill_processes,
                   long_operation_in_process, long_operation_in_thread,
                   naturalsize, none_to_zero, print_tasks, run_proxy_http,
                   sanitize_filename, sg, traverse_obj, try_get, wait_time)
from videodownloader import VideoDownloader

logger = logging.getLogger("asyncDL")


class AsyncDL():

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

        
        self.wkinit_stop = False
        self.pasres_repeat = False
        self.console_dl_status = False
        
        self.list_pasres = set()
        self.pasres_time_from_resume_to_pause = 50
        self.pasres_time_in_pause = 10

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
        
        self.t1 = Timer("execution", text="[timers] Time spent with data preparation for the init workers: {:.2f}", logger=logger.info)
        self.t2 = Timer("execution", text="[timers] Time spent with DL: {:.2f}", logger=logger.info)
        self.t3 = Timer("execution", text="[timers] Time spent by init workers: {:.2f}", logger=logger.info)
        
                
    
        self.p1, self.mpqueue = self.get_videos_cached()        
        

    
    
    @long_operation_in_process
    def get_videos_cached(self, *args, **kwargs):        
        
        """
        In local storage, files are saved wihtin the file files.cached.json in 5 groups each in different volumnes.
        If any of the volumes can't be accesed in real time, the local storage info of that volume will be used.    
        """

        queue = kwargs.get('queue')

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
                    queue.put_nowait(videos_cached)
                    
        except Exception as e:
            logger.exception(f"[videos_cached] {repr(e)}")
      
    def update_window(self, status, list_old):
        list_upt = {}
        if isinstance(status, str): _status = (status,)
        else: _status = status
        for i,dl in enumerate(self.list_dl):
            if not dl.index:
                dl.index = i
            if dl.info_dl['status'] in _status:                
                list_upt.update({i: dl.print_hookup()})
            
        if list_upt != list_old and self.window_root:
            self.window_root.write_event_value(_status[0], list_upt)
        
        return list_upt
    
    @long_operation_in_thread
    def upt_window_periodic(self, *args, **kwargs):
        
        stop_event = kwargs["stop_event"]

        try:
            
            list_init_old = {}
            list_dl_old = {}
            list_manip_old = {}
            self.list_nwmon = []
            #io = psutil.net_io_counters(pernic=True)['en1']
            self.ema_s = EMA(smoothing=0.01)
            io = psutil.net_io_counters()
            init_bytes_recv = io.bytes_recv
            bytes_recv = io.bytes_recv
            wait_time(CONF_INTERVAL_GUI/2)            
            while not stop_event.is_set():
                if self.list_dl:
                    list_init_old = self.update_window("init", list_init_old)
                    list_dl_old = self.update_window("downloading", list_dl_old)
                    list_manip_old = self.update_window(("manipulating", "init_manipulating"), list_manip_old)
                    #io_2 = psutil.net_io_counters(pernic=True)['en1']
                    #io_2 = psutil.net_io_counters()
                    _recv = psutil.net_io_counters().bytes_recv
                    #ds = (io_2.bytes_recv - bytes_recv) / CONF_INTERVAL_GUI
                    ds = (_recv - bytes_recv) / (CONF_INTERVAL_GUI/2)
                    self.list_nwmon.append((datetime.now(), ds))                    
                    if self.window_root:
                        msg = f"RECV: {naturalsize(_recv - init_bytes_recv,True)}  DL: {naturalsize(self.ema_s(ds),True)}ps"
                        self.window_root.write_event_value("nwmon", msg)
                    #bytes_recv = io_2.bytes_recv
                    bytes_recv = _recv  
                
                if not wait_time(CONF_INTERVAL_GUI/2, event=stop_event):
                    break
                
                
                
        except Exception as e:
            logger.exception(f"[upt_window_periodic]: error: {repr(e)}")
        finally:
            if self.list_nwmon: 
                logger.info(f"DL MEDIA: {naturalsize(median([el[1] for el in self.list_nwmon]),True)}ps")
                _str_nwmon = ', '.join([f'{el[0].strftime("%H:%M:")}{(el[0].second + (el[0].microsecond / 1000000)):06.3f}' for el in self.list_nwmon])
                logger.debug(f"[upt_window_periodic] nwmon {len(self.list_nwmon)}]\n{_str_nwmon}")
            logger.debug("[upt_window_periodic] BYE")
        
    @long_operation_in_thread          
    def pasres_periodic(self, *args, **kwargs):
        
        logger.debug('[pasres_periodic] START')
        
        stop_event = kwargs["stop_event"]
        
        try:
            while not stop_event.is_set():
                
                if self.pasres_repeat and (_list:= list(self.list_pasres)):
                    for _index in _list:
                        self.list_dl[_index-1].pause()
                    
                    wait_time(self.pasres_time_in_pause, event=stop_event)
            
                    for _index in _list:
                        self.list_dl[_index-1].resume()
                
                    wait_time(self.pasres_time_from_resume_to_pause, event=stop_event)
                
                else:
                    wait_time(CONF_INTERVAL_GUI, event=stop_event)

        except Exception as e:
            logger.exception(f"[pasres_periodic]: error: {repr(e)}")
        finally:            
            logger.debug('[pasres_periodic] BYE')
      
    async def cancel_all_tasks(self):
        self.STOP.set()
        await asyncio.sleep(0)
        try_get(self.ytdl.params['stop'], lambda x: x.set())
        if self.list_dl:
            for dl in self.list_dl:
                dl.stop()
        await asyncio.sleep(0)

        
    async def print_pending_tasks(self):
        try:
            if self.loop:
                pending_tasks = asyncio.all_tasks(loop=self.loop)
                logger.debug(f"[pending_all_tasks] {pending_tasks}")
                logger.debug(f"[pending_all_tasks]\n{print_tasks(pending_tasks)}")
                if self.window_console: 
                    sg.cprint(f"[pending_all_tasks]\n{print_tasks(pending_tasks)}")
                
                logger.debug(f"[queue_vid] {self.queue_vid._queue}")
                logger.debug(f"[run_vid] {self.queue_run._queue}")
                logger.debug(f"[manip_vid] {self.queue_manip._queue}")
        except Exception as e:
            logger.exception(f"[print_pending_tasks]: error: {repr(e)}")

    async def gui_console(self):
        
        try:
            
            self.window_console = init_gui_console()
            
            await async_wait_time(CONF_INTERVAL_GUI)
                        
            while True:             
                
                if not await async_wait_time(CONF_INTERVAL_GUI/2, events=[self.stop_console]):
                    break

                event, values = self.window_console.read(timeout=0)

                if event == sg.TIMEOUT_KEY:
                    continue
                sg.cprint(event, values)
                if event  == sg.WIN_CLOSED:
                    break
                elif event in ['Exit']:
                    logger.info(f'[gui_console] event Exit')
                    await self.cancel_all_tasks()
                    break
                elif event in ['-EXIT-']:
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
                        timers = [timer.strip() for timer in values['-IN-'].split(',')]
                        if len(timers) > 2:
                            sg.cprint('max 2 timers')
                        else:

                            if any([(not timer.isdecimal() or int(timer) < 0) for timer in timers]):
                                sg.cprint('not an integer, or negative')
                            else:
                                if len(timers) == 2:
                                    self.pasres_time_from_resume_to_pause = int(timers[0])
                                    self.pasres_time_in_pause = int(timers[1])
                                else:
                                    self.pasres_time_from_resume_to_pause = int(timers[0])
                                    self.pasres_time_in_pause = int(timers[0])

                                sg.cprint(f'[time to resume] {self.pasres_time_from_resume_to_pause} [time in pause] {self.pasres_time_in_pause}')



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
                                    #sg.cprint(f"[{', '.join(info)}]")                                    
                                    _thr = getattr(dl.info_dl['downloaders'][0], 'throttle', None)
                                    sg.cprint(f"throttle [{_thr}]")
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
            logger.debug("[gui_console] BYE")
            self.window_console.close()  
            #self.stop_root.set()
            self.window_console = None
                
    async def gui_root(self):
                    
        try:
            self.window_root = init_gui_root()

            await asyncio.sleep(0)

            list_init = {}
            list_downloading = {}
            list_manipulating = {}
            list_finish = {}
                
            while True:                
                
                if not await async_wait_time(CONF_INTERVAL_GUI/2, events=[self.stop_root]):
                    break

                event, value = self.window_root.read(timeout=0)
                if event == sg.TIMEOUT_KEY:
                    continue
                #logger.debug(f"{event}:{value}")
            
                if "kill" in event or event == sg.WIN_CLOSED: break
                elif event == "nwmon":
                    self.window_root['ST'].update(value['nwmon'])
                elif event == "init":
                    list_init = value['init']
                    if list_init: 
                        upt = "\n\n" + ''.join(list(list_init.values()))
                    else: upt = ""
                    self.window_root['-ML0-'].update(upt)
                elif event == "downloading":
                    list_downloading = value['downloading']
                   
                    _text = ["\n\n-------DOWNLOADING VIDEO------------\n\n"]
                    if list_downloading:
                        _text.extend(list(list_downloading.values()))
                 
                    _upt = ''.join(_text)                        
                    self.window_root['-ML1-'].update(_upt)
                    
                    if self.console_dl_status:
                        sg.cprint('"\n\n-------STATUS DL----------------\n\n"')
                        sg.cprint('\n'.join(list_downloading.values()))
                        sg.cprint('"\n\n-------END STATUS DL------------\n\n"')
                        self.console_dl_status = False                           
                    
                elif event in ("manipulating", "init_manipulating"):
                    list_manipulating = value[event]
                    
                    _text = []    

                    if list_manipulating:
                        _text.extend(["\n\n-------CREATING FILE------------\n\n"])
                        _text.extend(list(list_manipulating.values()))                    
                    if _text: _upt = ''.join(_text)
                    else: _upt = ''
                    self.window_root['-ML3-'].update(_upt)
                elif event in ("error", "done", "stop"):
                    list_finish.update(value[event])
                    
                    if list_finish:
                        _upt = "\n\n" + ''.join(list(list_finish.values()))
                    else:
                        _upt = ''                        
                    
                    self.window_root['-ML2-'].update(_upt)
                        

        except BaseException as e:
            if not isinstance(e, asyncio.CancelledError):           
                logger.exception(f"[gui_root]\nlist_init: {list_init}\nlist_dl: {list_downloading}\nlist_manip: {list_manipulating}\nlist_finish: {list_finish}\nError:{repr(e)} ")
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:           
            logger.debug("[gui_root] BYE")                       
            self.window_root.close()
            self.window_root = None
                
    def get_list_videos(self):

        try:
            
            self.videos_cached = self.mpqueue.get(timeout=60)

            url_list = []
            _url_list_caplinks = []
            _url_list_cli = []
            self.url_pl_list = {}
            netdna_list = set()
            _url_list = {}
            
            filecaplinks = Path(Path.home(), "Projects/common/logs/captured_links.txt")
            if self.args.caplinks and filecaplinks.exists():
                if self.STOP.is_set(): raise Exception("STOP")
                _temp = set()
                with open(filecaplinks, "r") as file:
                    for _url in file:
                        if (_url:=_url.strip()): _temp.add(_url)           
                    
                _url_list_caplinks = list(_temp)
                logger.info(f"[get_videos] video list caplinks:\n{_url_list_caplinks}")
                
                
                shutil.copy("/Users/antoniotorres/Projects/common/logs/captured_links.txt", 
                            "/Users/antoniotorres/Projects/common/logs/prev_captured_links.txt")
                with open(filecaplinks, "w") as file:
                    file.write("")
                    
                _url_list['caplinks'] = _url_list_caplinks
            
            if self.args.collection:                
                if self.STOP.is_set(): raise Exception("STOP")
                _url_list_cli = list(set(self.args.collection)) 
                logger.info(f"[get_videos] video list cli:\n{_url_list_cli}")
                _url_list['cli'] = _url_list_cli   
            
            
            logger.debug(f"[get_videos] Initial # urls:\n\tCLI[{len(_url_list_cli )}]\n\tCAP[{len(_url_list_caplinks)}]")
            
            if _url_list:

                for _source, _ulist in _url_list.items():
                    
                    if self.STOP.is_set(): raise Exception("STOP")
                    
                    for _elurl in _ulist:
                    
                        if self.STOP.is_set(): raise Exception("STOP")
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
                
                logger.debug(f"[url_list] Initial number of urls not pl [{len(url_list)}]")
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
                    
                    logger.debug(f"[url_playlist_list] Initial number of urls that are pl [{len(self.url_pl_list)}]")
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
                            
                            if self.STOP.is_set(): 
                                raise Exception("STOP")
                            with self.lock:
                                self._count_pl += 1
                                logger.info(f"[get_videos][url_playlist_list][{self._count_pl}/{len(self.url_pl_list) + len(self.futures2)}] processing {_url}")
                            try:
                                _errormsg = None
                                #_info = self.ytdl.extract_info(_url, download=False, process=False)
                                _info = self.ytdl.extract_info(_url, download=False, process=False)
                            except Exception as e:
                                logger.warning(f"[url_playlist_list] {_url} {type(e).__name__}")
                                logger.debug(f"[url_playlist_list] {_url} {repr(e)}")
                                _info = None
                                _errormsg = repr(e)
                            if not _info:
                                _info = {'_type': 'error', 'url': _url, 'error': _errormsg or 'no video entry'}
                                self._prepare_entry_pl_for_dl(_info)
                                self._url_pl_entries += [_info]
                            elif _info:                                
                                    
                                if _info.get('_type', 'video') != 'playlist': #caso generic que es playlist default, pero luego puede ser url, url_trans
                                    #_info = self.ytdl.sanitize_info(self.ytdl.process_ie_result(_info, download=False))
                                    #_info = self.ytdl.sanitize_info(_info)
                                    
                                    _info = self.ytdl.sanitize_info(self.ytdl.process_ie_result(_info, download=False))

                                    if not _info.get('original_url'): _info.update({'original_url': _url})
                                    
                                    self._prepare_entry_pl_for_dl(_info)
                                    self._url_pl_entries += [_info]
                                else:   
                                    if _get and not self.args.path:                             
                                        _name = f"{sanitize_filename(_info.get('title'), restricted=True)}{_info.get('extractor_key')}{_info.get('id')}"
                                        self.args.path = str(Path(Path.home(), 'testing', _name))
                                        logger.debug(f"[url_playlist_list] path for playlist {_url}:\n{self.args.path}")
                                    
                                    if isinstance(_info.get('entries'), list):                                    


                                        _temp_error = []

                                        
                                        for _ent in _info.get('entries'):
                                            if _ent.get('error'):
                                                _ent['_type'] = "error"
                                                self._prepare_entry_pl_for_dl(_ent)
                                                self._url_pl_entries.append(_ent)                                                    
                                                _temp.error.append(_ent)
                                                del _ent

                                        _info = self.ytdl.sanitize_info(self.ytdl.process_ie_result(_info, download=False))
                                        
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
                                        
                                        if self.STOP.is_set(): 
                                            raise Exception("STOP")                                        

                                        if _ent.get('_type', 'video') == 'video' and not _ent.get('error'):
                                            
                                            _ent = self.ytdl.sanitize_info(self.ytdl.process_ie_result(_ent, download=False))

                                            if not _ent.get('original_url'): 
                                                _ent.update({'original_url': _url})
                                            elif _ent['original_url'] != _url:
                                                _ent['initial_url'] = _url
                                            if ((_ent.get('extractor_key') == 'Generic') or (_ent.get('ie_key') == 'Generic'))  and (_ent.get('n_entries',0) <= 1):
                                                    _ent.pop("playlist","")
                                                    _ent.pop("playlist_index","")
                                                    _ent.pop("n_entries","")
                                                    _ent.pop("playlist", "")
                                                    _ent.pop('playlist_id',"")
                                                    _ent.pop('playlist_title','')
                                            
                                            if ((_wurl:=_ent['webpage_url']) == _ent['original_url']):
                                                if _ent.get('n_entries', 0) > 1:
                                                    _ent.update({'webpage_url': f"{_wurl}?index={_ent['playlist_index']}"})
                                                    logger.warning(f"[url_playlist_list][{_url}][{_ent['playlist_index']}]: playlist, nentries > 1, webpage_url == original_url: {_wurl}")
                                                    
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
                    
                    if self.STOP.is_set(): raise Exception("STOP")
                    
                    with ThreadPoolExecutor(thread_name_prefix="GetPlaylist", max_workers=self.init_nworkers) as self.ex_pl:
                    
                        for url in self.url_pl_list:    
                            if self.STOP.is_set(): raise Exception("STOP")
                            self.futures.update({self.ex_pl.submit(process_playlist, url, _get_name): url}) 
                    
                        logger.debug(f"[url_playlist_list] initial playlists: {len(self.futures)}")
                    
                        wait(list(self.futures))
                    
                        logger.debug(f"[url_playlist_list] playlists from initial playlists: {len(self.futures2)}")
                    
                        if self.STOP.is_set(): raise Exception("STOP")
                        
                        if self.futures2:
                            wait(list(self.futures2))
                            
                    
                    
                    if self.STOP.is_set(): raise Exception("STOP")
                    
                    logger.info(f"[get_videos] entries from playlists: {len(self._url_pl_entries)}")
                    logger.debug(f"[url_playlist_list] {_for_print_videos(self._url_pl_entries)}")
                

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
                    info_video = get_info_json(file)
                    if info_video.get('_type', 'video') != 'playlist':
                        _file_list_videos.append(info_video)
                    else:
                        _file_list_videos.extend(info_video.get('entries'))


                  
                
                
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


            logger.debug(f"[get_list_videos] list videos: \n{_for_print_videos(self.list_videos)}")            
            
        
        except BaseException as e:            
            logger.exception(f"[get_videos]: Error {repr(e)}")
            
        finally:            
            for _ in range(self.init_nworkers - 1):
                self.queue_vid.put_nowait("KILL")        
            self.queue_vid.put_nowait("KILLANDCLEAN")
            self.getlistvid_done = True
            if not self.STOP.is_set(): self.t1.stop()
                
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
                self.totalbytes2dl += none_to_zero(self.info_videos[url].get('video_info', {}).get('filesize', 0))
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
                elif 'unsupported url' in str(entry.get('error', 'no video entry')).lower():                                    
                    self.list_unsup_urls.append(url_key)                                                    
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

            while not self.STOP.is_set():

                done, pending = await asyncio.wait([asyncio.create_task(self.STOP.wait()), asyncio.create_task(self.queue_vid.get())], return_when=asyncio.FIRST_COMPLETED)
                try_get(list(pending), lambda x: x[0].cancel())
                if self.STOP.is_set(): break
                url_key = try_get(list(done), lambda x: x[0].result())

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
                    
                    #if not self.list_dl: 
                        
                        #self.stop_console = False
                        #self.pasres_repeat = False
 
                    break
                
                else: 
                    async with self.alock:
                        _pending = self.num_videos_pending
                        _to_check = self.num_videos_to_check
                    
                    vid = self.info_videos[url_key]['video_info']
                    logger.debug(f"[worker_init][{i}]: [{url_key}] extracting info")
                    
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
                                logger.debug(f"[worker_init][{i}]: [{url_key}] info extracted\n{_for_print(info)}")
                            
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
                            logger.debug(f"[worker_init][{i}]: [{urlkey}] info extracted\n{_for_print(infdict)}")
                            
                            self.info_videos[urlkey].update({'video_info': infdict})
 
                            _filesize = none_to_zero(extradict.get('filesize', 0)) if extradict else none_to_zero(infdict.get('filesize', 0))
                            
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
                                
                                                                    
                                dl = await async_ex_in_executor(self.ex_winit, VideoDownloader, self.window_root, self.info_videos[urlkey]['video_info'], self.ytdl, self.args, self.hosts_downloading, self.alock, self.hosts_alock)
                                
                                logger.debug(f"[worker_init][{i}]: [{dl.info_dict['id']}][{dl.info_dict['title']}]: {dl.info_dl}")
                                
                                _filesize = none_to_zero(extradict.get('filesize', 0)) if extradict else none_to_zero(infdict.get('filesize', 0))       
                                
                                if not dl.info_dl.get('status', "") == "error":
                                    
                                    if dl.info_dl.get('filesize'):
                                        self.info_videos[urlkey]['video_info']['filesize'] = dl.info_dl.get('filesize')
                                        async with self.alock:
                                            self.totalbytes2dl = self.totalbytes2dl - _filesize + dl.info_dl.get('filesize', 0)
                                            
                                    self.info_videos[urlkey].update({'status': 'initok', 'filename': str(dl.info_dl.get('filename')), 'dl': str(dl)})

                                    async with self.alock:
                                        dl.index = len(self.list_dl)
                                        self.list_dl.append(dl)
                                    
                                    #dl.write_window()
                                        
                                    
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
                                                self.list_pasres.add(dl.index + 1)
                                                _msg = f', add this dl[{dl.index + 1}] to auto_pasres{list(self.list_pasres)}'
                                                if self.window_console: sg.cprint(f"[pause-resume autom] {self.list_pasres}")
                                                                            
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
                                
                            if not self.STOP.is_set(): await get_dl(url_key, infdict=info, extradict=vid)

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
                                                    if not self.STOP.is_set(): await get_dl(_url, infdict=_entry)
                                                    
                                                    
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
        await asyncio.sleep(0)
        try:
            
            while not self.STOP.is_set():
               
                done, pending = await asyncio.wait([asyncio.create_task(self.STOP.wait()), asyncio.create_task(self.queue_run.get())], return_when=asyncio.FIRST_COMPLETED)
                try_get(list(pending), lambda x: x[0].cancel())
                if self.STOP.is_set(): break
                #url_key, video_dl = await self.queue_run.get()                
                
                url_key, video_dl = try_get(list(done), lambda x: x[0].result())
                 
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
                                            
                    #OJO!!!!
                    #self.stop_console = True OJO
                    #self.pasres_repeat = False 
                    
                    
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
                        logger.debug(f"[worker_run][{i}][{url_key}]: STOPPED")
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
            
            while not self.STOP.is_set():

                done, pending = await asyncio.wait([asyncio.create_task(self.STOP.wait()), asyncio.create_task(self.queue_manip.get())], return_when=asyncio.FIRST_COMPLETED)
                try_get(list(pending), lambda x: x[0].cancel())
                if self.STOP.is_set(): break
                
                url_key, video_dl = try_get(list(done), lambda x: x[0].result())
                #logger.debug(f"[worker_manip][{i}]: get for a video_DL")
                
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
    
        self.STOP = asyncio.Event()        

        self.stop_console = asyncio.Event()
        self.stop_root = asyncio.Event()
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
                        
            tasks_gui = []
            self.extra_tasks_run = []
            self.proc_gost = []
            self.proc_aria2c = None
            self.routing_table = {}
            self.stop_proxy = None
            self.stop_upt_window = None
            self.stop_pasres = None
            
            
            tasks_to_wait = {}

            self.task_gui_root = asyncio.create_task(self.gui_root())
            self.console_task = asyncio.create_task(self.gui_console())
            tasks_gui = [self.task_gui_root, self.console_task] 
            
            tasks_to_wait.update({asyncio.create_task(async_ex_in_executor(self.ex_winit, self.get_list_videos)): 'task_get_videos'})
            tasks_to_wait.update({asyncio.create_task(self.worker_init(i)): f'task_worker_init_{i}' for i in range(self.init_nworkers)})
                            
            if not self.args.nodl:                

                self.stop_upt_window = self.upt_window_periodic()
                self.stop_pasres = self.pasres_periodic()
                if self.args.aria2c:             
                    self.proc_aria2c = init_aria2c(self.args)
                    if self.args.proxy != 0:
                        self.proc_gost, self.routing_table = init_proxies(CONF_PROXIES_MAX_N_GR_HOST, CONF_PROXIES_N_GR_VIDEO)
                        self.ytdl.params['routing_table'] = self.routing_table
                        #if not self.ytdl.params.get('proxy'): self.ytdl.params['proxy'] = f'http://127.0.0.1:{list(self.routing_table)[-1]}'
                        logger.debug(f"[async_ex] ytdl_params:\n{self.ytdl.params}")                
                        self.stop_proxy = run_proxy_http() #launch as thread daemon proxy helper in dl of aria2

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
            if isinstance(e, KeyboardInterrupt):
                print("")
            logger.error(f"[async_ex] {repr(e)}")
            self.STOP.set()
            await asyncio.sleep(0)
            try_get(self.ytdl.params['stop'], lambda x: x.set())
            if self.list_dl:
                for dl in self.list_dl:
                    dl.stop_event.set()
            await asyncio.sleep(0)
            raise
        
        finally:

            self.stop_upt_window.set()
            await asyncio.sleep(0)
            self.stop_pasres.set()
            await asyncio.sleep(0)
            self.stop_console.set()
            await asyncio.sleep(0)
            self.stop_root.set() 
            await asyncio.sleep(0)          
            done, _ = await asyncio.wait(tasks_gui)
            self.ex_winit.shutdown(wait=False, cancel_futures=True)
            logger.info(f"[async_ex] BYE")
            if any(isinstance(_e, KeyboardInterrupt) for _e in [d.exception() for d in done]):
                raise KeyboardInterrupt
            
    def get_results_info(self):
            
        def _getter(url, vid):
            webpageurl = traverse_obj(vid, ('video_info', 'webpage_url'))
            originalurl = traverse_obj(vid, ('video_info', 'original_url'))
            playlist = traverse_obj(vid, ('video_info', 'playlist'))
            if not webpageurl and not originalurl and not playlist:
                return url
            if playlist and traverse_obj(vid, ('video_info','n_entries')) > 1:
                #return f"{playlist}:[{traverse_obj(vid, ('video_info','playlist_index'))}]:{originalurl or webpageurl}:{url}"
                return f"[{traverse_obj(vid, ('video_info','playlist_index'))}]:{originalurl or webpageurl}:{url}"
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
                
                list_videos2dl_str = [[fill(vid['video_info'].get('id', ''),col//6), fill(vid['video_info'].get('title', ''), col//6), naturalsize(none_to_zero(vid['video_info'].get('filesize',0))), fill(_getter(url, vid), col//2)] for url, vid in self.info_videos.items() if not vid.get('aldl') and not vid.get('samevideo') and vid.get('todl') and vid.get('status') != "prenok"] if list_videos2dl else []
                
                list_videosaldl = [_getter(url, vid) for url, vid in self.info_videos.items() if vid.get('aldl') and vid.get('todl')]
                list_videosaldl_str = [[fill(vid['video_info'].get('id', ''),col//6), fill(vid['video_info'].get('title', ''), col//6), fill(_getter(url, vid), col//3), fill(vid['aldl'], col//3)] for url, vid in self.info_videos.items() if vid.get('aldl') and vid.get('todl')] if list_videosaldl else []
                
                list_videossamevideo = [_getter(url, vid) for url, vid in self.info_videos.items() if vid.get('samevideo')]
                list_videossamevideo_str = [[fill(vid['video_info'].get('id', ''),col//6), fill(vid['video_info'].get('title', ''), col//6), fill(_getter(url, vid), col//3), fill(vid['samevideo'], col//3)] for url, vid in self.info_videos.items() if vid.get('samevideo')] if list_videossamevideo else []
                
                
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
       
        #logger.debug(f'[get_result_info]\n{self.info_videos}')  
            
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
        
        if self.args.path:
            _path_str = f"--path {self.args.path} "
        else:
            _path_str = ""
        
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
                logger.info(f"%no%\n\n{videos_kodl} \n[{_path_str}-u {' -u '.join(videos_kodl)}]")
            else:
                logger.info(f"Videos TOTAL ERROR DL: []")
            if videos_koinit:            
                logger.info(f"Videos ERROR INIT DL:")
                logger.info(f"%no%\n\n{videos_koinit} \n[{_path_str}-u {' -u '.join(videos_koinit)}]")
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
        
        logger.debug(f'\n{_for_print_videos(self.info_videos)}')
        

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
                    logger.debug(f"[close][{ie}] closed ok")                    
                except Exception as e:
                    logger.exception(f"[close][{ie}] {repr(e)}")
    
    def close(self):
        
        try:

            logger.info("[close] start to close")
            
            try:
                self.p1.join()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")  

                       
            try:
                if not self.STOP.is_set(): self.t2.stop()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")        
            
            try:        
                if self.proc_aria2c: 
                    logger.info("[close] aria2c")
                    self.proc_aria2c.kill()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")

            try:        
                self.ies_close()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")
            
            if self.proc_gost:
                logger.info("[close] gost")
                for proc in self.proc_gost:
                    try:
                        proc.kill()
                    except BaseException as e:
                        logger.exception(f"[close] {repr(e)}")
            
            stops = [self.stop_proxy]
            logger.info("[close] proxy")
            for _stop in stops:
                try:                
                    if _stop:
                        _stop.set()
                        wait_time(5)
                except BaseException as e:
                    logger.exception(f"[close] {_stop} {repr(e)}")       
                
            try:
                logger.info("[close] kill processes")
                kill_processes(logger=logger, rpcport=self.args.rpcport)
                                
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")
        
        except BaseException as e:
            logger.exception(f"[close] {repr(e)}")
            raise
