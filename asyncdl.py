import asyncio
import hashlib
import json
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import zip_longest

from pathlib import Path

import os as syncos

from statistics import median
from textwrap import fill
from threading import Lock, Event
import xattr

import psutil
from codetiming import Timer
from tabulate import tabulate

from utils import (
    CONF_INTERVAL_GUI,
    PATH_LOGS,
    LocalStorage,
    _for_print,
    _for_print_videos,
    sync_to_async,
    async_wait_time,
    async_waitfortasks,
    get_chain_links,
    get_domain,
    init_aria2c,
    init_gui_console,
    init_gui_root,
    TorGuardProxies,
    init_ytdl,
    js_to_json,
    kill_processes,
    long_operation_in_thread,
    naturalsize,
    none_to_zero,
    print_tasks,
    sanitize_filename,
    sg,
    traverse_obj,
    try_get,
    wait_time,
    ProgressTimer,
    SpeedometerMA,
    Union,
    async_lock
)

import proxy
from videodownloader import VideoDownloader
from collections import deque

logger = logging.getLogger("asyncDL")


class WorkersRun:
    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.max = self.asyncdl.workers
        self.running = set()
        self.waiting = deque()
        self.tasks = {}
        self.logger = logging.getLogger("WorkersRun")
        self.exit = asyncio.Event()
        self.alock = asyncio.Lock()

    @property
    def running_count(self):
        return len(self.running)

    def add_worker(self):
        self.max += 1
        if self.waiting:
            if self.running_count < self.max:
                dl, url = self.waiting.popleft()
                self._start_task(dl, url)

    def del_worker(self):
        if self.max > 0:
            self.max -= 1

    async def add_dl(self, dl, url_key):

        async with self.alock:
            self.logger.debug(f"[{url_key}] add dl to worker run. Running[{self.running_count}] Waiting[{len(self.waiting)}]")
            if self.running_count >= self.max:
                self.waiting.append((dl, url_key))
            else:
                self._start_task(dl, url_key)

    def _start_task(self, dl, url_key):
        self.running.add((dl, url_key))
        self.tasks.update({asyncio.create_task(self._task(dl, url_key)): dl})
        self.logger.debug(f"[{url_key}] task ok {self.tasks}")

    async def _task(self, dl, url_key):
        try:

            if dl.info_dl["status"] not in ("init_manipulating", "done"):
                await async_waitfortasks(dl.run_dl())

            await self.asyncdl.run_callback(dl, url_key)

            async with self.alock:
                self.running.remove((dl, url_key))
                if self.waiting:
                    if self.running_count < self.max:
                        dl2, url2 = self.waiting.popleft()
                        self._start_task(dl2, url2)

        except Exception as e:
            self.logger.exception(f"[{url_key}] error {repr(e)}")
        finally:
            self.logger.debug(f"[{url_key}] end task worker run")
            if not self.waiting and not self.running:
                self.logger.debug(f"[{url_key}] end tasks worker run: exit")
                self.exit.set()


class WorkersInit:
    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.max = self.asyncdl.init_nworkers
        self.running = set()
        self.waiting = deque()
        self.tasks = {}
        self.lock = Lock()
        self.exit = asyncio.Event()
        self.logger = logging.getLogger("WorkersInit")

    @property
    def running_count(self):
        return len(self.running)

    def add_init(self, url_key):

        with self.lock:
            self.logger.debug(f"[{url_key}] init. Running[{self.running_count}] Waiting[{len(self.waiting)}]")
            if self.running_count >= self.max:
                self.waiting.append(url_key)
            else:
                self._start_task(url_key)

    def _start_task(self, url_key):

        self.running.add(url_key)
        self.tasks.update({self.asyncdl.loop.create_task(self._task(url_key)): url_key})
        self.logger.debug(f"[{url_key}] task ok {self.tasks}")

    async def _task(self, url_key):
        try:
            if url_key == "KILL":
                async with async_lock(self.lock):
                    self.running.remove(url_key)
                while self.running_count:
                    await asyncio.sleep(0)
                self.logger.debug(f"[{url_key}] end tasks worker init: exit")
                self.asyncdl.t3.stop()
                self.exit.set()
            else:
                await asyncio.wait([asyncio.create_task(self.asyncdl.init_callback(url_key))])
                async with async_lock(self.lock):
                    self.running.remove(url_key)
                    if self.waiting:
                        if self.running_count < self.max:
                            url = self.waiting.popleft()
                            self._start_task(url)

        except Exception as e:
            self.logger.exception(f"[{url_key}] error {repr(e)}")


class FrontEndGUI:
    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger("FEgui")
        self.list_finish = {}
        self.console_dl_status = False
        self.pasres_repeat = False
        self.pasres_time_from_resume_to_pause = 20
        self.pasres_time_in_pause = 1
        self.reset_repeat = False
        self.list_all_old = {"init": {}, "downloading": {}, "manip": {}, "finish": {}}
        self.dl_media_str = None
        self.stop_upt_window = self.upt_window_periodic()
        self.stop_pasres = self.pasres_periodic()

    async def gui_root(self, event, values):

        try:
            if "kill" in event or event == sg.WIN_CLOSED:
                return "break"
            elif event == "nwmon":
                self.window_root["ST"].update(values["nwmon"])
            elif event == "all":
                self.window_root["ST"].update(values["all"]["nwmon"])
                if "init" in values["all"]:
                    list_init = values["all"]["init"]
                    if list_init:
                        upt = "\n\n" + "".join(list(list_init.values()))
                    else:
                        upt = ""
                    self.window_root["-ML0-"].update(value=upt)
                if "downloading" in values["all"]:
                    list_downloading = values["all"]["downloading"]
                    _text = ["\n\n-------DOWNLOADING VIDEO------------\n\n"]
                    if list_downloading:
                        _text.extend(list(list_downloading.values()))
                    upt = "".join(_text)
                    self.window_root["-ML1-"].update(value=upt)
                    if self.console_dl_status:
                        upt = "\n".join(list_downloading.values())
                        sg.cprint(
                            f"\n\n-------STATUS DL----------------\n\n{upt}\n\n-------END STATUS DL------------\n\n"
                        )
                        self.console_dl_status = False
                if "manipulating" in values["all"]:
                    list_manipulating = values["all"]["manipulating"]
                    _text = []
                    if list_manipulating:
                        _text.extend(["\n\n-------CREATING FILE------------\n\n"])
                        _text.extend(list(list_manipulating.values()))
                    if _text:
                        upt = "".join(_text)
                    else:
                        upt = ""
                    self.window_root["-ML3-"].update(value=upt)

                if "finish" in values["all"]:
                    self.list_finish.update(values["all"]["finish"])

                    if self.list_finish:
                        upt = "\n\n" + "".join(list(self.list_finish.values()))
                    else:
                        upt = ""

                    self.window_root["-ML2-"].update(value=upt)

            elif event in ("error", "done", "stop"):
                self.list_finish.update(values[event])

                if self.list_finish:
                    upt = "\n\n" + "".join(list(self.list_finish.values()))
                else:
                    upt = ""

                self.window_root["-ML2-"].update(value=upt)

        except Exception as e:
            self.logger.exception(f"[gui_root] {repr(e)}")

    async def gui_console(self, event, values):

        sg.cprint(event, values)
        if event == sg.WIN_CLOSED:
            return "break"
        elif event in ["Exit"]:
            self.logger.info("[gui_console] event Exit")
            await self.asyncdl.cancel_all_tasks()
        elif event in ["-WKINIT-"]:
            self.asyncdl.wkinit_stop = not self.asyncdl.wkinit_stop
            sg.cprint(
                "Worker inits: BLOCKED"
            ) if self.asyncdl.wkinit_stop else sg.cprint("Worker inits: RUNNING")
        elif event in ["-PASRES-"]:
            if not values["-PASRES-"]:
                self.pasres_repeat = False
            else:
                self.pasres_repeat = True
        elif event in ["-RESETREP-"]:
            if not values["-RESETREP-"]:
                self.reset_repeat = False
            else:
                self.reset_repeat = True
        elif event in ["-DL-STATUS"]:
            await self.asyncdl.print_pending_tasks()
            if not self.console_dl_status:
                self.console_dl_status = True
        elif event in ["IncWorkerRun"]:
            self.asyncdl.WorkersRun.add_worker()
            sg.cprint(
                f"Workers: {self.asyncdl.WorkersRun.max}"
            )
        elif event in ["DecWorkerRun"]:
            self.asyncdl.WorkersRun.del_worker()
            sg.cprint(
                f"Workers: {self.asyncdl.WorkersRun.max}"
            )
        elif event in ["TimePasRes"]:
            if not values["-IN-"]:
                sg.cprint("[pause-resume autom] Please enter number")
                sg.cprint(f"[pause-resume autom] {list(self.asyncdl.list_pasres)}")
            else:
                timers = [timer.strip() for timer in values["-IN-"].split(",")]
                if len(timers) > 2:
                    sg.cprint("max 2 timers")
                else:

                    if any(
                        [
                            (not timer.isdecimal() or int(timer) < 0)
                            for timer in timers
                        ]
                    ):
                        sg.cprint("not an integer, or negative")
                    else:
                        if len(timers) == 2:
                            self.pasres_time_from_resume_to_pause = int(
                                timers[0]
                            )
                            self.pasres_time_in_pause = int(timers[1])
                        else:
                            self.pasres_time_from_resume_to_pause = int(
                                timers[0]
                            )
                            self.pasres_time_in_pause = int(timers[0])

                        sg.cprint(
                            f"[time to resume] {self.pasres_time_from_resume_to_pause} [time in pause] {self.pasres_time_in_pause}"
                        )

        elif event in ["NumVideoWorkers"]:
            if not values["-IN-"]:
                sg.cprint("Please enter number")
            else:
                if not values["-IN-"].isdecimal():
                    sg.cprint("not an integer")
                else:
                    _nvidworkers = int(values["-IN-"])
                    if _nvidworkers == 0:
                        sg.cprint("must be > 0")

                    else:
                        self.asyncdl.args.parts = _nvidworkers
                        if self.asyncdl.list_dl:
                            for _, dl in self.asyncdl.list_dl.items():
                                await dl.change_numvidworkers(_nvidworkers)
                        else:
                            sg.cprint("DL list empty")

        elif event in [
            "ToFile",
            "Info",
            "Pause",
            "Resume",
            "Reset",
            "Stop",
            "+PasRes",
            "-PasRes",
        ]:
            if not self.asyncdl.list_dl:
                sg.cprint("DL list empty")

            else:
                _index_list = []
                if (_values := values.get(event)):  # from thread pasres
                    _index_list = [int(el) for el in _values.split(',')]
                elif (not (_values := values["-IN-"]) or _values.lower() == "all"):
                    _index_list = [int(dl.index) for _, dl in self.asyncdl.list_dl.items()]
                else:
                    if any([any([not el.isdecimal(), int(el) == 0, int(el) > len(self.asyncdl.list_dl)]) for el in values["-IN-"].replace(" ", "").split(",")]):
                        sg.cprint("there are characters not decimal or equal to 0 or out of bound of number of dl")
                    else:
                        _index_list = [int(el) for el in values["-IN-"].replace(" ", "").split(",")]

                if _index_list:
                    if event in ["+PasRes", "-PasRes"]:
                        sg.cprint(f"[pause-resume autom] before: {list(self.asyncdl.list_pasres)}")
                    info = []
                    for _index in _index_list:
                        if event == "+PasRes":
                            self.asyncdl.list_pasres.add(_index)
                        elif event == "-PasRes":
                            self.asyncdl.list_pasres.discard(_index)
                        elif event == "Pause":
                            await self.asyncdl.list_dl[_index].pause()
                        elif event == "Resume":
                            await self.asyncdl.list_dl[_index].resume()
                        elif event == "Reset":
                            await self.asyncdl.list_dl[_index].reset_from_console()
                        elif event == "Stop":
                            await self.asyncdl.list_dl[_index].stop()
                        elif event in ["Info", "ToFile"]:
                            _thr = getattr(self.asyncdl.list_dl[_index].info_dl["downloaders"][0], "throttle", None)
                            sg.cprint(f"[{_index}] throttle [{_thr}]")
                            _info = json.dumps(self.asyncdl.list_dl[_index].info_dict)
                            sg.cprint(f"[{_index}] info\n{_info}")
                            info.append(_info)

                        await asyncio.sleep(0)

                    if event in ["+PasRes", "-PasRes"]:
                        sg.cprint(f"[pause-resume autom] after: {list(self.asyncdl.list_pasres)}")

                    if event == "ToFile":
                        with open((_file := Path(Path.home(), "testing", f"{self.asyncdl.launch_time.strftime('%Y%m%d_%H%M')}.json")), "w") as f:
                            f.write(
                                f'{{"entries": [{", ".join(info)}]}}'
                            )
                        sg.cprint(f"saved to file: {_file}")

    async def gui(self):

        try:
            self.stop = asyncio.Event()
            self.window_console = init_gui_console()
            self.window_root = init_gui_root()
            await asyncio.sleep(0)

            while not self.stop.is_set():

                window, event, values = sg.read_all_windows(timeout=0)

                if not window or not event or event == sg.TIMEOUT_KEY:
                    await asyncio.sleep(0)
                    continue

                _res = []
                if window == self.window_console:
                    _res.append(await self.gui_console(event, values))
                elif window == self.window_root:
                    _res.append(await self.gui_root(event, values))

                if "break" in _res:
                    break

                await asyncio.sleep(0)
            await asyncio.sleep(0)

        except BaseException as e:
            if not isinstance(e, asyncio.CancelledError):
                self.logger.exception(
                    f"[gui] {repr(e)}"
                )
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:
            self.logger.debug("[gui] BYE")

    def update_window(self, status, nwmon=None):
        list_upt = {}
        list_res = {}

        trans = {
            "manip": ("init_manipulating", "manipulating"),
            "finish": ("error", "done", "stop"),
            "init": "init",
            "downloading": "downloading"
        }

        if status == "all":
            _status = ("init", "downloading", "manip", "finish")
        else:
            if isinstance(status, str):
                _status = (status,)
            else:
                _status = status

        for st in _status:
            list_upt[st] = {}
            list_res[st] = {}

            for i, dl in self.asyncdl.list_dl.items():

                if dl.info_dl["status"] in trans[st]:
                    list_res[st].update({i: dl.print_hookup()})

            if list_res[st] == self.list_all_old[st]:
                del list_upt[st]
            else:
                list_upt[st] = list_res[st]
        if nwmon:
            list_upt["nwmon"] = nwmon

        if hasattr(self, 'window_root') and self.window_root:
            self.window_root.write_event_value("all", list_upt)

        for st, val in self.list_all_old.items():
            if st not in list_res:
                list_res.update({st: val})

        self.list_all_old = list_res

    @long_operation_in_thread(name='uptwinthr')
    def upt_window_periodic(self, *args, **kwargs):

        stop_upt = kwargs["stop_event"]

        try:

            progress_timer = ProgressTimer()
            short_progress_timer = ProgressTimer()
            self.list_nwmon = []
            init_bytes_recv = psutil.net_io_counters().bytes_recv
            speedometer = SpeedometerMA(initial_bytes=init_bytes_recv)
            ds = None
            while not stop_upt.is_set():

                if self.asyncdl.list_dl:

                    if progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI):
                        _recv = psutil.net_io_counters().bytes_recv
                        ds = speedometer(_recv)

                        msg = f"RECV: {naturalsize(_recv - init_bytes_recv,True)}  DL: {naturalsize(ds,True)}ps"
                        self.update_window("all", nwmon=msg)
                        if short_progress_timer.has_elapsed(seconds=10*CONF_INTERVAL_GUI):
                            self.list_nwmon.append((datetime.now(), ds))
                    else:
                        time.sleep(CONF_INTERVAL_GUI/4)
                else:
                    time.sleep(CONF_INTERVAL_GUI)
                    progress_timer.reset()
                    short_progress_timer.reset()

        except Exception as e:
            self.logger.exception(f"[upt_window_periodic]: error: {repr(e)}")
        finally:
            if self.list_nwmon:
                self.dl_media_str = f"DL MEDIA: {naturalsize(median([el[1] for el in self.list_nwmon]),True)}ps"
                _str_nwmon = ", ".join(
                    [
                        f'{el[0].strftime("%H:%M:")}{(el[0].second + (el[0].microsecond / 1000000)):06.3f}'
                        for el in self.list_nwmon
                    ]
                )
                self.logger.debug(f"[upt_window_periodic] nwmon {len(self.list_nwmon)}]\n{_str_nwmon}")
            self.logger.debug("[upt_window_periodic] BYE")

    @long_operation_in_thread(name='pasresthr')
    def pasres_periodic(self, *args, **kwargs):

        self.logger.debug("[pasres_periodic] START")
        stop_event = kwargs["stop_event"]

        try:
            while not stop_event.is_set():

                if (self.pasres_repeat or self.reset_repeat) and (
                    _list := list(self.asyncdl.list_pasres)
                ):
                    if not self.reset_repeat:
                        self.window_console.write_event_value("Pause", ','.join(list(map(str, _list))))

                        wait_time(self.pasres_time_in_pause, event=stop_event)

                        self.window_console.write_event_value("Resume", ','.join(list(map(str, _list))))
                        wait_time(
                            self.pasres_time_from_resume_to_pause, event=stop_event
                        )
                    else:
                        self.window_console.write_event_value("Reset", ','.join(list(map(str, _list))))
                        wait_time(
                            self.pasres_time_from_resume_to_pause, event=stop_event
                        )

                else:
                    wait_time(CONF_INTERVAL_GUI, event=stop_event)

        except Exception as e:
            logger.exception(f"[pasres_periodic]: error: {repr(e)}")
        finally:
            logger.debug("[pasres_periodic] BYE")

    def close(self):
        self.stop_upt_window.set()
        self.stop_pasres.set()
        if hasattr(self, 'window_console') and self.window_console:
            self.window_console.close()
            del self.window_console
        if hasattr(self, 'window_root') and self.window_root:
            self.window_root.close()
            del self.window_root


class NWSetUp:

    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger("setupnw")
        self.shutdown_proxy = Event()
        self.routing_table = {}
        self.proc_gost = []
        self.proc_aria2c = None
        self.exe = ThreadPoolExecutor(thread_name_prefix="setupnw")
        self.stop_proxy = self.run_proxy_http()
        self._tasks_init = {}
        if self.asyncdl.args.aria2c:
            ainit_aria2c = sync_to_async(init_aria2c, executor=self.exe)
            _tasks_init_aria2c = {
                asyncio.create_task(ainit_aria2c(self.asyncdl.args)): "aria2"
            }
            self._tasks_init.update(_tasks_init_aria2c)
        if self.asyncdl.args.enproxy:
            ainit_proxies = sync_to_async(
                TorGuardProxies.init_proxies, executor=self.exe)
            _task_init_proxies = {
                asyncio.create_task(ainit_proxies()): "proxies"
            }
            self._tasks_init.update(_task_init_proxies)

    async def init(self):

        if self._tasks_init:
            done, _ = await asyncio.wait(self._tasks_init)
            for task in done:
                try:
                    if self._tasks_init[task] == "aria2":
                        self.proc_aria2c = task.result()
                    else:
                        self.proc_gost, self.routing_table = task.result()
                        self.asyncdl.ytdl.params['routing_table'] = self.routing_table
                except Exception as e:
                    logger.exception(f"[async_ex] {repr(e)}")

    @long_operation_in_thread(name='proxythr')
    def run_proxy_http(self, *args, **kwargs):

        stop_event: Event = kwargs["stop_event"]
        log_level = kwargs.get('log_level', 'INFO')
        try:
            with proxy.Proxy(
                [
                    "--log-level",
                    log_level,
                    "--plugins",
                    "proxy.plugin.ProxyPoolByHostPlugin",
                ]
            ) as p:
                logger = logging.getLogger("proxy")
                try:
                    logger.debug(p.flags)
                    stop_event.wait()
                except BaseException:
                    logger.error("context manager proxy")
        finally:
            self.shutdown_proxy.set()

    def close(self):

        self.logger.info("[close] proxy")
        self.stop_proxy.set()
        self.logger.info("[close] waiting for http proxy shutdown")
        self.shutdown_proxy.wait()
        self.logger.info("[close] OK shutdown")

        if self.proc_gost:
            self.logger.info("[close] gost")
            for proc in self.proc_gost:
                try:
                    proc.kill()
                except BaseException as e:
                    self.logger.exception(f"[close] {repr(e)}")

        if self.proc_aria2c:
            self.logger.info("[close] aria2c")
            self.proc_aria2c.kill()


class LocalVideos:
    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger('videoscached')
        self.file_ready: Event = self.get_videos_cached()

    def ready(self):
        self.file_ready.wait()

    @long_operation_in_thread(name='vidcachthr')
    def get_videos_cached(self, *args, **kwargs):

        """
        In local storage, files are saved wihtin the file files.cached.json in 5 groups each in different volumnes.
        If any of the volumes can't be accesed in real time, the local storage info of that volume will be used.
        """

        _finished: Event = kwargs['stop_event']

        local_storage = LocalStorage()

        self.logger.info(f"[videos_cached] start scanning - dlnocaching [{self.asyncdl.args.nodlcaching}]")

        videos_cached = {}
        last_time_sync = {}

        try:

            with local_storage.lock:

                local_storage.load_info()

                list_folders_to_scan = {}
                videos_cached = {}
                last_time_sync = local_storage._last_time_sync

                if self.asyncdl.args.nodlcaching:
                    for _vol, _folder in local_storage.config_folders.items():
                        if _vol != "local":
                            videos_cached.update(local_storage._data_from_file[_vol])
                        else:
                            list_folders_to_scan.update({_folder: _vol})

                else:

                    for _vol, _folder in local_storage.config_folders.items():
                        if not _folder.exists():  # comm failure
                            logger.error(
                                f"Fail to connect to [{_vol}], will use last info"
                            )
                            videos_cached.update(local_storage._data_from_file[_vol])
                        else:
                            list_folders_to_scan.update({_folder: _vol})

                _repeated = []
                _dont_exist = []
                _repeated_by_xattr = []

                for folder in list_folders_to_scan:

                    try:

                        files = [
                            file
                            for file in folder.rglob("*")
                            if file.is_file()
                            and not file.stem.startswith(".")
                            and (file.suffix.lower() in (".mp4", ".mkv", ".zip"))
                        ]

                        for file in files:

                            try:
                                _xattr_desc = xattr.getxattr(file, "user.dublincore.description").decode()
                                if not videos_cached.get(_xattr_desc):
                                    videos_cached.update({_xattr_desc: str(file)})
                                else:
                                    _repeated_by_xattr.append({_xattr_desc: [videos_cached[_xattr_desc], str(file)]})
                            except Exception:
                                pass

                            _res = file.stem.split("_", 1)
                            if len(_res) == 2:
                                _id = _res[0]
                                _title = sanitize_filename(
                                    _res[1], restricted=True
                                ).upper()
                                _name = f"{_id}_{_title}"
                            else:
                                _name = sanitize_filename(
                                    file.stem, restricted=True
                                ).upper()

                            if not (_video_path_str := videos_cached.get(_name)):

                                videos_cached.update({_name: str(file)})

                            else:
                                _video_path = Path(_video_path_str)
                                if _video_path != file:

                                    if (
                                        not file.is_symlink()
                                        and not _video_path.is_symlink()
                                    ):  # only if both are hard files we have to do something, so lets report it in repeated files
                                        _repeated.append(
                                            {
                                                "title": _name,
                                                "indict": _video_path_str,
                                                "file": str(file),
                                            }
                                        )
                                    elif (
                                        not file.is_symlink()
                                        and _video_path.is_symlink()
                                    ):
                                        _links = get_chain_links(_video_path)
                                        if _links[-1] == file:
                                            if len(_links) > 2:
                                                self.logger.debug(
                                                    f'[videos_cached] \nfile not symlink: {str(file)}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links])}'
                                                )
                                                for _link in _links[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(file)
                                                    _link._accessor.utime(
                                                        _link,
                                                        (
                                                            int(
                                                                self.asyncdl.launch_time.timestamp()
                                                            ),
                                                            file.stat().st_mtime,
                                                        ),
                                                        follow_symlinks=False,
                                                    )

                                            videos_cached.update({_name: str(file)})
                                        else:
                                            self.logger.warning(
                                                f'[videos_cached] \n**file not symlink: {str(file)}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links])}'
                                            )

                                    elif (
                                        file.is_symlink()
                                        and not _video_path.is_symlink()
                                    ):
                                        _links = get_chain_links(file)
                                        if _links[-1] == _video_path:
                                            if len(_links) > 2:
                                                self.logger.debug(
                                                    f'[videos_cached] \nfile symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links])}\nvideopath not symlink: {str(_video_path)}'
                                                )
                                                for _link in _links[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_video_path)
                                                    _link._accessor.utime(
                                                        _link,
                                                        (
                                                            int(
                                                                self.asyncdl.launch_time.timestamp()
                                                            ),
                                                            _video_path.stat().st_mtime,
                                                        ),
                                                        follow_symlinks=False,
                                                    )

                                            videos_cached.update(
                                                {_name: str(_video_path)}
                                            )
                                            if not _video_path.exists():
                                                _dont_exist.append(
                                                    {
                                                        "title": _name,
                                                        "file_not_exist": str(
                                                            _video_path
                                                        ),
                                                        "links": [
                                                            str(_l)
                                                            for _l in _links[0:-1]
                                                        ],
                                                    }
                                                )
                                        else:
                                            self.logger.warning(
                                                f'[videos_cached] \n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links])}\nvideopath not symlink: {str(_video_path)}'
                                            )

                                    else:
                                        _links_file = get_chain_links(file)
                                        _links_video_path = get_chain_links(_video_path)
                                        if (
                                            _file := _links_file[-1]
                                        ) == _links_video_path[-1]:
                                            if len(_links_file) > 2:
                                                self.logger.debug(
                                                    f'[videos_cached] \nfile symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}'
                                                )
                                                for _link in _links_file[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(
                                                        _link,
                                                        (
                                                            int(
                                                                self.asyncdl.launch_time.timestamp()
                                                            ),
                                                            _file.stat().st_mtime,
                                                        ),
                                                        follow_symlinks=False,
                                                    )
                                            if len(_links_video_path) > 2:
                                                self.logger.debug(
                                                    f'[videos_cached] \nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}'
                                                )
                                                for _link in _links_video_path[0:-1]:
                                                    _link.unlink()
                                                    _link.symlink_to(_file)
                                                    _link._accessor.utime(
                                                        _link,
                                                        (
                                                            int(
                                                                self.asyncdl.launch_time.timestamp()
                                                            ),
                                                            _file.stat().st_mtime,
                                                        ),
                                                        follow_symlinks=False,
                                                    )

                                            videos_cached.update({_name: str(_file)})
                                            if not _file.exists():
                                                _dont_exist.append(
                                                    {
                                                        "title": _name,
                                                        "file_not_exist": str(_file),
                                                        "links": [
                                                            str(_l)
                                                            for _l in (
                                                                _links_file[0:-1]
                                                                + _links_video_path[
                                                                    0:-1
                                                                ]
                                                            )
                                                        ],
                                                    }
                                                )

                                        else:
                                            self.logger.warning(
                                                f'[videos_cached] \n**file symlink: {str(file)}\n\t\t{" -> ".join([str(_l) for _l in _links_file])}\nvideopath symlink: {str(_video_path)}\n\t\t{" -> ".join([str(_l) for _l in _links_video_path])}'
                                            )

                    except Exception as e:
                        self.logger.error(
                            f"[videos_cached][{list_folders_to_scan[folder]}] {repr(e)}"
                        )

                    else:
                        last_time_sync.update(
                            {list_folders_to_scan[folder]: str(self.asyncdl.launch_time)}
                        )

                try:

                    local_storage.dump_info(videos_cached, last_time_sync)

                    if _repeated:
                        self.logger.warning(
                            "[videos_cached] Please check videos repeated in logs"
                        )
                        self.logger.debug(f"[videos_cached] videos repeated: \n {_repeated}")

                    if _dont_exist:
                        self.logger.warning(
                            "[videos_cached] Please check videos dont exist in logs"
                        )
                        self.logger.debug(
                            f"[videos_cached] videos dont exist: \n {_dont_exist}"
                        )

                except Exception as e:
                    self.logger.exception(f"[videos_cached] {repr(e)}")

            self.logger.info(f"[videos_cached] Total videos cached: [{len(videos_cached)}]")

            self.asyncdl.videos_cached = videos_cached

            _finished.set()

        except Exception as e:
            self.logger.exception(f"[videos_cached] {repr(e)}")

    def get_files_same_id(self):

        config_folders = {
            "local": Path(Path.home(), "testing"),
            "pandaext4": Path("/Volumes/Pandaext4/videos"),
            "datostoni": Path("/Volumes/DatosToni/videos"),
            "wd1b": Path("/Volumes/WD1B/videos"),
            "wd5": Path("/Volumes/WD5/videos"),
            "wd8_1": Path("/Volumes/WD8_1/videos"),
        }

        list_folders = []

        for _vol, _folder in config_folders.items():
            if not _folder.exists():
                self.logger.error(
                    f"failed {_folder}, let get previous info saved in previous files"
                )

            else:
                list_folders.append(_folder)

        files_cached = []
        for folder in list_folders:

            self.logger.info(">>>>>>>>>>>STARTS " + str(folder))

            files = []
            try:

                files = [
                    file
                    for file in folder.rglob("*")
                    if file.is_file()
                    and not file.is_symlink()
                    and "videos/_videos/" not in str(file)
                    and not file.stem.startswith(".")
                    and (file.suffix.lower() in (".mp4", ".mkv", ".ts", ".zip"))
                ]

            except Exception as e:
                self.logger.info(f"[get_files_cached][{folder}] {repr(e)}")

            for file in files:

                _res = file.stem.split("_", 1)
                if len(_res) == 2:
                    _id = _res[0]

                else:
                    _id = sanitize_filename(file.stem, restricted=True).upper()

                files_cached.append((_id, str(file)))

        _res_dict = {}
        for el in files_cached:
            for item in files_cached:
                if (el != item) and (item[0] == el[0]):
                    if not _res_dict.get(el[0]):
                        _res_dict[el[0]] = set([el[1], item[1]])
                    else:
                        _res_dict[el[0]].update([el[1], item[1]])
        _ord_res_dict = sorted(_res_dict.items(), key=lambda x: len(x[1]))
        return _ord_res_dict


class AsyncDL:

    def __init__(self, args):

        # args
        self.args = args
        self.workers = self.args.w
        self.init_nworkers = self.args.winit if self.args.winit > 0 else self.args.w

        # youtube_dl
        self.ytdl = init_ytdl(self.args)

        # listas, dicts con videos
        self.info_videos = {}
        self.videos_cached = {}

        self.list_videos = []
        self.list_initnok = []
        self.list_unsup_urls = []
        self.list_notvalid_urls = []
        self.list_urls_to_check = []

        self.list_dl = {}
        self.videos_to_dl = []

        self.num_videos_to_check = 0
        self.num_videos_pending = 0

        self.wkinit_stop = False

        self.list_pasres = set()

        # contadores sobre número de workers init, workers run y workers manip

        self.hosts_downloading = {}

        self.totalbytes2dl = 0
        self.launch_time = datetime.now()

        self.ex_winit = ThreadPoolExecutor(thread_name_prefix="ex_wkinit")

        self.lock = Lock()

        self.t1 = Timer(
            "execution",
            text="[timers] Time for extracting info by init workers: {:.2f}",
            logger=logger.info,
        )
        self.t2 = Timer(
            "execution",
            text="[timers] Time DL: {:.2f}",
            logger=logger.info
        )
        self.t3 = Timer(
            "execution",
            text="[timers] Time spent by init workers: {:.2f}",
            logger=logger.info,
        )

        # self.routing_table = {}
        # self.proc_gost = []
        # if not self.args.nodl:
        #     if self.args.enproxy:
        #         self.shutdown_proxy = Event()
        #         self.stop_proxy = self.run_proxy_http()

    async def cancel_all_tasks(self):
        self.STOP.set()
        await asyncio.sleep(0)
        try_get(self.ytdl.params["stop"], lambda x: x.set())
        if self.list_dl:
            for i, dl in self.list_dl.items():
                await dl.stop("exit")
                await asyncio.sleep(0)

    async def print_pending_tasks(self):
        try:
            if hasattr(self, 'loop'):
                pending_tasks = asyncio.all_tasks(loop=self.loop)
                logger.debug(f"[pending_all_tasks] {pending_tasks}")
                logger.debug(f"[pending_all_tasks]\n{print_tasks(pending_tasks)}")

        except Exception as e:
            logger.exception(f"[print_pending_tasks]: error: {repr(e)}")

    async def get_list_videos(self):

        logger.info("[get_list_videos] start")

        try:

            url_list = []
            _url_list_caplinks = []
            _url_list_cli = []
            self.url_pl_list = {}
            _url_list = {}

            filecaplinks = Path(PATH_LOGS, "captured_links.txt")
            prevfilecaplinks = Path(PATH_LOGS, "prev_captured_links.txt")
            if self.args.caplinks and filecaplinks.exists():
                if self.STOP.is_set():
                    raise Exception("STOP")
                _temp = set()
                with open(filecaplinks, "r") as file:
                    for _url in file:
                        if _url := _url.strip():
                            _temp.add(_url)

                _url_list_caplinks = list(_temp)
                logger.info(f"[get_videos] video list caplinks:\n{_url_list_caplinks}")
                with open(prevfilecaplinks, "a") as file:
                    _text = '\n'.join(_url_list_caplinks)
                    file.write(f"\n\n[{self.launch_time.strftime('%Y-%m-%d %H:%M')}]\n{_text}")

                with open(filecaplinks, "w") as file:
                    file.write("")

                _url_list["caplinks"] = _url_list_caplinks

            if self.args.collection:
                if self.STOP.is_set():
                    raise Exception("STOP")
                _url_list_cli = list(set(self.args.collection))
                logger.info(f"[get_videos] video list cli:\n{_url_list_cli}")
                _url_list["cli"] = _url_list_cli

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
                    if info_video:
                        if info_video.get("_type", "video") != "playlist":
                            _file_list_videos.append(info_video)
                        elif (_entries := info_video.get("entries")):
                            _file_list_videos.extend(_entries)

                for _vid in _file_list_videos:
                    if not _vid.get("playlist"):

                        _url = _vid.get("webpage_url")
                        if not self.info_videos.get(_url):

                            self.info_videos[_url] = {
                                "source": "file_cli",
                                "video_info": _vid,
                                "status": "init",
                                "aldl": False,
                                "todl": True,
                                "error": [],
                            }

                            _same_video_url = await self.async_check_if_same_video(_url)

                            if _same_video_url:

                                self.info_videos[_url].update(
                                    {"samevideo": _same_video_url}
                                )
                                logger.warning(
                                    f"{_url}: has not been added to video list because it gets same video than {_same_video_url}"
                                )
                                await self.async_prepare_for_dl(_url)

                            else:
                                await self.async_prepare_for_dl(_url)
                                self.list_videos.append(
                                    self.info_videos[_url]["video_info"]
                                )
                    else:
                        await self.async_prepare_entry_pl_for_dl(_vid)

            logger.debug(
                f"[get_list_videos] list videos: \n{_for_print_videos(self.list_videos)}"
            )

            logger.debug(
                f"[get_videos] Initial # urls:\n\tCLI[{len(_url_list_cli )}]\n\tCAP[{len(_url_list_caplinks)}]"
            )

            if _url_list:

                for _source, _ulist in _url_list.items():

                    if self.STOP.is_set():
                        raise Exception("STOP")

                    for _elurl in _ulist:

                        if self.STOP.is_set():
                            raise Exception("STOP")
                        is_pl, ie_key = self.ytdl.is_playlist(_elurl)

                        if not is_pl:

                            _entry = {"_type": "url", "url": _elurl, "ie_key": ie_key}

                            if not self.info_videos.get(_elurl):

                                self.info_videos[_elurl] = {
                                    "source": _source,
                                    "video_info": _entry,
                                    "status": "init",
                                    "aldl": False,
                                    "todl": True,
                                    "ie_key": ie_key,
                                    "error": [],
                                }

                                await self.async_prepare_for_dl(_elurl)
                                self.list_videos.append(_entry)

                        else:
                            if not self.url_pl_list.get(_elurl):
                                self.url_pl_list[_elurl] = {"source": _source}

                url_list = list(self.info_videos.keys())

                logger.debug(
                    f"[url_list] Initial number of urls not pl [{len(url_list)}]"
                )
                logger.debug(f"[url_list] {url_list}")

                if self.url_pl_list:

                    logger.debug(
                        f"[url_playlist_list] Initial number of urls that are pl [{len(self.url_pl_list)}]"
                    )
                    logger.debug(f"[url_playlist_list]\n{self.url_pl_list}")
                    self._url_pl_entries = []
                    self._count_pl = 0

                    self.url_pl_list2 = []

                    if len(self.url_pl_list) == 1 and self.args.use_path_pl:
                        _get_name = True
                    else:
                        _get_name = False

                    async def process_playlist(_get: bool, _kinfo: Union[dict, None] = None):

                        while True:
                            try:
                                await asyncio.sleep(0)
                                _url = await self.url_pl_queue.get()
                                if _url == "KILL":
                                    break
                                if self.STOP.is_set():
                                    raise Exception("STOP")
                                async with self.alock:
                                    self._count_pl += 1
                                    logger.info(
                                        f"[get_videos][url_playlist_list][{self._count_pl}/{len(self.url_pl_list) + len(self.url_pl_list2)}] processing {_url}"
                                    )
                                # _errormsg = None
                                if _kinfo:
                                    _info = _kinfo
                                else:
                                    try:
                                        _info = await self.ytdl.async_extract_info(
                                            _url, download=False, process=False
                                        )
                                        if not _info:
                                            raise Exception("no info")
                                    except Exception as e:
                                        logger.warning(
                                            f"[url_playlist_list] {_url} {repr(e)}"
                                        )
                                        logger.debug(
                                            f"[url_playlist_list] {_url} {repr(e)}"
                                        )

                                        _info = {
                                            "_type": "error",
                                            "url": _url,
                                            "error": repr(e)
                                        }
                                        await self.async_prepare_entry_pl_for_dl(_info)
                                        self._url_pl_entries += [_info]
                                        continue

                                if (
                                    _info.get("_type", "video") != "playlist"
                                ):  # caso generic que es playlist default, pero luego puede ser url, url_trans

                                    _info = self.ytdl.sanitize_info(
                                        await self.ytdl.async_process_ie_result(
                                            _info, download=False
                                        )
                                    )

                                    if not _info.get("original_url"):
                                        _info.update({"original_url": _url})

                                    await self.async_prepare_entry_pl_for_dl(_info)
                                    self._url_pl_entries += [_info]
                                else:
                                    if _get and not self.args.path:
                                        _name = f"{sanitize_filename(_info.get('title'), restricted=True)}{_info.get('extractor_key')}{_info.get('id')}"
                                        self.args.path = str(
                                            Path(Path.home(), "testing", _name)
                                        )
                                        logger.debug(
                                            f"[url_playlist_list] path for playlist {_url}:\n{self.args.path}"
                                        )

                                    if isinstance(_info.get("entries"), list):

                                        _temp_error = []

                                        _entries_ok = []
                                        for _ent in _info["entries"]:
                                            if _ent.get("error"):
                                                _ent["_type"] = "error"
                                                if not _ent.get("original_url"):
                                                    _ent.update(
                                                        {"original_url": _url}
                                                    )
                                                await self.async_prepare_entry_pl_for_dl(
                                                    _ent
                                                )
                                                self._url_pl_entries.append(_ent)
                                                _temp_error.append(_ent)
                                                # del _ent
                                            else:
                                                _entries_ok.append(_ent)

                                        _info["entries"] = _entries_ok
                                        _info = self.ytdl.sanitize_info(
                                            await self.ytdl.async_process_ie_result(
                                                _info, download=False
                                            )
                                        )

                                        if _info.get("extractor_key") in (
                                            "GVDBlogPost",
                                            "GVDBlogPlaylist",
                                        ):
                                            _temp_aldl = []
                                            _temp_nodl = []

                                            for _ent in _info["entries"]:
                                                if not await self.async_check_if_aldl(
                                                    _ent, test=True
                                                ):
                                                    _temp_nodl.append(_ent)
                                                else:
                                                    _temp_aldl.append(_ent)

                                            def get_list_interl(res):
                                                if not res:
                                                    return []
                                                if len(res) < 3:
                                                    return res
                                                _dict = {}
                                                for ent in res:
                                                    _key = get_domain(ent["url"])
                                                    if not _dict.get(_key):
                                                        _dict[_key] = [ent]
                                                    else:
                                                        _dict[_key].append(ent)
                                                logger.info(
                                                    f"[url_playlist_list][{_url}] gvdblogplaylist entries interleave: {len(list(_dict.keys()))} different hosts, longest with {len(max(list(_dict.values()), key=len))} entries"
                                                )
                                                _interl = []
                                                for el in list(
                                                    zip_longest(
                                                        *list(_dict.values())
                                                    )
                                                ):
                                                    _interl.extend(
                                                        [_el for _el in el if _el]
                                                    )
                                                return _interl

                                            _info["entries"] = (
                                                get_list_interl(_temp_nodl)
                                                + _temp_aldl
                                            )

                                    for _ent in _info["entries"]:

                                        if self.STOP.is_set():
                                            raise Exception("STOP")

                                        if _ent.get(
                                            "_type", "video"
                                        ) == "video" and not _ent.get("error"):

                                            _ent = self.ytdl.sanitize_info(
                                                await self.ytdl.async_process_ie_result(
                                                    _ent, download=False
                                                )
                                            )

                                            if not _ent.get("original_url"):
                                                _ent.update({"original_url": _url})
                                            elif _ent["original_url"] != _url:
                                                _ent["initial_url"] = _url
                                            if ((_ent.get("extractor_key", _ent.get("extractor", _ent.get("ie_key", ""))).lower() == "generic")
                                                    and (_ent.get("n_entries", 0) <= 1)):
                                                _ent.pop("playlist", "")
                                                _ent.pop("playlist_index", "")
                                                _ent.pop("n_entries", "")
                                                _ent.pop("playlist", "")
                                                _ent.pop("playlist_id", "")
                                                _ent.pop("playlist_title", "")

                                            if (
                                                _wurl := _ent["webpage_url"]
                                            ) == _ent["original_url"]:
                                                if _ent.get("n_entries", 0) > 1:
                                                    _ent.update(
                                                        {
                                                            "webpage_url": f"{_wurl}?index={_ent['playlist_index']}"
                                                        }
                                                    )
                                                    logger.warning(
                                                        f"[url_playlist_list][{_url}][{_ent['playlist_index']}]: playlist, nentries > 1, webpage_url == original_url: {_wurl}"
                                                    )

                                            await self.async_prepare_entry_pl_for_dl(
                                                _ent
                                            )
                                            self._url_pl_entries += [_ent]
                                        else:
                                            try:
                                                (
                                                    is_pl,
                                                    ie_key,
                                                ) = self.ytdl.is_playlist(
                                                    _ent["url"]
                                                )
                                                _error = _ent.get("error")
                                                if not is_pl or _error:
                                                    if not _ent.get("original_url"):
                                                        _ent.update(
                                                            {"original_url": _url}
                                                        )
                                                    if _error:
                                                        _ent["_type"] = "error"
                                                    await self.async_prepare_entry_pl_for_dl(
                                                        _ent
                                                    )
                                                    self._url_pl_entries.append(
                                                        _ent
                                                    )
                                                else:

                                                    self.url_pl_list2.append(
                                                        _ent["url"]
                                                    )

                                            except Exception as e:
                                                logger.error(
                                                    f"[url_playlist_list][{_url}]:{_ent['url']} no video entries - {repr(e)}"
                                                )

                            except BaseException as e:
                                logger.exception(f"[url_playlist_list] {repr(e)}")
                                if isinstance(e, KeyboardInterrupt):
                                    raise

                    if self.STOP.is_set():
                        raise Exception("STOP")

                    self.url_pl_queue = asyncio.Queue()
                    for url in self.url_pl_list:
                        self.url_pl_queue.put_nowait(url)
                    for _ in range(min(self.init_nworkers, len(self.url_pl_list))):
                        self.url_pl_queue.put_nowait("KILL")
                    tasks_pl_list = [
                        asyncio.create_task(process_playlist(_get_name))
                        for _ in range(min(self.init_nworkers, len(self.url_pl_list)))
                    ]
                    logger.debug(f"[url_playlist_list] initial playlists: {len(self.url_pl_list)}")
                    await asyncio.wait(tasks_pl_list)

                    logger.debug(f"[url_playlist_list] playlists from initial playlists: {len(self.url_pl_list2)}")

                    if self.STOP.is_set():
                        raise Exception("STOP")

                    if self.url_pl_list2:
                        self.url_pl_queue = asyncio.Queue()
                        for url in self.url_pl_list2:
                            self.url_pl_queue.put_nowait(url)
                        for _ in range(min(self.init_nworkers, len(self.url_pl_list2))):
                            self.url_pl_queue.put_nowait("KILL")
                        tasks_pl_list2 = [
                            asyncio.create_task(process_playlist(_get_name))
                            for _ in range(
                                min(self.init_nworkers, len(self.url_pl_list2))
                            )
                        ]
                        await asyncio.wait(tasks_pl_list2)

                    logger.info(
                        f"[get_videos] entries from playlists: {len(self._url_pl_entries)}"
                    )
                    logger.debug(
                        f"[url_playlist_list] {_for_print_videos(self._url_pl_entries)}"
                    )

        except BaseException as e:
            logger.exception(f"[get_videos]: Error {repr(e)}")

        finally:
            self.getlistvid_done.set()
            self.WorkersInit.add_init("KILL")
            if not self.STOP.is_set():
                self.t1.stop()

    def _check_if_aldl(self, info_dict, test=False):

        if not (_id := info_dict.get("id")) or not (_title := info_dict.get("title")):
            return False

        _title = sanitize_filename(_title, restricted=True).upper()
        vid_name = f"{_id}_{_title}"

        if not (vid_path_str := self.videos_cached.get(vid_name)):
            return False

        else:  # video en local

            if test:
                return True

            vid_path = Path(vid_path_str)
            logger.debug(f"[{vid_name}] already DL: {vid_path}")

            if not self.args.nosymlinks:
                if self.args.path:
                    _folderpath = Path(self.args.path)
                else:
                    _folderpath = Path(
                        Path.home(), "testing", self.launch_time.strftime("%Y%m%d")
                    )
                _folderpath.mkdir(parents=True, exist_ok=True)
                file_aldl = Path(_folderpath, vid_path.name)
                if file_aldl not in _folderpath.iterdir():
                    file_aldl.symlink_to(vid_path)
                    try:
                        mtime = int(vid_path.stat().st_mtime)

                        syncos.utime(
                            file_aldl,
                            (int(datetime.timestamp(datetime.now())), mtime),
                            follow_symlinks=False,
                        )
                    except Exception as e:
                        logger.debug(
                            f"[check_if_aldl] [{str(file_aldl)}] -> [{str(vid_path)}] error when copying times {repr(e)}"
                        )

            return vid_path_str

    def _check_if_same_video(self, url_to_check: str) -> Union[str, None]:

        info = self.info_videos[url_to_check]["video_info"]

        if (
            info.get("_type", "video") == "video"
            and (_id := info.get("id"))
            and (_title := info.get("title"))
        ):

            for (urlkey, _vid) in self.info_videos.items():
                if urlkey != url_to_check:
                    if (
                        _vid["video_info"].get("_type", "video") == "video"
                        and (_vid["video_info"].get("id", "") == _id)
                        and (_vid["video_info"].get("title", "")) == _title
                    ):
                        return urlkey

    def _prepare_for_dl(self, url: str, put: bool = True) -> None:
        self.info_videos[url].update({"todl": True})
        if _id := self.info_videos[url]["video_info"].get("id"):
            self.info_videos[url]["video_info"]["id"] = (
                sanitize_filename(_id, restricted=True)
                .replace("_", "")
                .replace("-", "")
            )
        if _title := self.info_videos[url]["video_info"].get("title"):
            self.info_videos[url]["video_info"]["title"] = sanitize_filename(
                _title[:150], restricted=True
            )
        if not self.info_videos[url]["video_info"].get("filesize", None):
            self.info_videos[url]["video_info"]["filesize"] = 0
        if _path := self._check_if_aldl(self.info_videos[url]["video_info"]):
            self.info_videos[url].update({"aldl": _path, "status": "done"})
            logger.debug(
                f"[prepare_for_dl] [{self.info_videos[url]['video_info'].get('id')}][{self.info_videos[url]['video_info'].get('title')}] already DL"
            )

        if (
            self.info_videos[url].get("todl")
            and not self.info_videos[url].get("aldl")
            and not self.info_videos[url].get("samevideo")
            and self.info_videos[url].get("status") != "prenok"
        ):
            with self.lock:
                self.totalbytes2dl += none_to_zero(
                    self.info_videos[url].get("video_info", {}).get("filesize", 0)
                )
                self.videos_to_dl.append(url)
                if put:
                    self.WorkersInit.add_init(url)
                self.num_videos_to_check += 1
                self.num_videos_pending += 1

    def _prepare_entry_pl_for_dl(self, entry: dict) -> None:

        _type = entry.get("_type", "video")
        if _type == "playlist":
            logger.warning(f"[prepare_entry_pl_for_dl] PLAYLIST IN PLAYLIST: {entry}")
            return
        elif _type == "error":
            _errorurl = entry.get("url")
            if _errorurl and not self.info_videos.get(_errorurl):

                self.info_videos[_errorurl] = {
                    "source": self.url_pl_list.get(_errorurl, {}).get("source")
                    or "playlist",
                    "video_info": {},
                    "status": "prenok",
                    "todl": True,
                    "error": [entry.get("error", "no video entry")],
                }

                if any(
                    _ in str(entry.get("error", "no video entry")).lower()
                    for _ in [
                        "not found",
                        "404",
                        "flagged",
                        "403",
                        "410",
                        "suspended",
                        "unavailable",
                        "disabled",
                    ]
                ):
                    self.list_notvalid_urls.append(_errorurl)
                elif (
                    "unsupported url"
                    in str(entry.get("error", "no video entry")).lower()
                ):
                    self.list_unsup_urls.append(_errorurl)
                else:
                    self.list_urls_to_check.append(
                        (_errorurl, entry.get("error", "no video entry"))
                    )
                self.list_initnok.append(
                    (_errorurl, entry.get("error", "no video entry"))
                )
            return

        elif _type == "video":
            _url = entry.get("webpage_url") or entry["url"]

        else:  # url, url_transparent
            _url = entry["url"]

        if not self.info_videos.get(_url):  # es decir, los nuevos videos

            self.info_videos[_url] = {
                "source": "playlist",
                "video_info": entry,
                "status": "init",
                "aldl": False,
                "todl": True,
                "ie_key": entry.get("ie_key") or entry.get("extractor_key"),
                "error": [],
            }

            _same_video_url = self._check_if_same_video(_url)

            if _same_video_url:

                self.info_videos[_url].update({"samevideo": _same_video_url})

                logger.warning(
                    f"[prepare_entry_pl_for_dl] {_url}: has not been added to video list because it gets same video than {_same_video_url}"
                )

                self._prepare_for_dl(_url)
            else:
                self._prepare_for_dl(_url)
                self.list_videos.append(self.info_videos[_url]["video_info"])
        else:
            logger.warning(
                f"[prepare_entry_pl_for_dl] {_url}: has not been added to info_videos because it is already"
            )

    async def init_callback(self, url_key):
        # worker que lanza la creación de los objetos VideoDownloaders, uno por video

        try:
            async with self.alock:
                _pending = self.num_videos_pending
                _to_check = self.num_videos_to_check

            vid = self.info_videos[url_key]["video_info"]
            logger.debug(f"[init_callback]: [{url_key}] extracting info")

            try:
                if self.wkinit_stop:
                    logger.info(f"[init_callback]: [{url_key}]: BLOCKED")

                    while self.wkinit_stop:
                        await async_wait_time(5)
                        logger.debug(f"[init_callback]: [{url_key}]: BLOCKED")

                    logger.info(f"[init_callback]: [{url_key}]: UNBLOCKED")

                if vid.get("_type", "video") != "video":

                    try:

                        _ext_info = (
                            try_get(
                                vid.get("original_url"),
                                lambda x: {"original_url": x},
                            )
                            or {}
                        )
                        logger.debug(
                            f"[init_callback]: [{url_key}]: [{url_key}] extra_info={_ext_info or vid}"
                        )
                        _res = await self.ytdl.async_extract_info(
                            vid["url"], download=False, extra_info=_ext_info
                        )
                        if not _res:
                            raise Exception("no info video")
                        info = self.ytdl.sanitize_info(_res)
                        logger.debug(
                            f"[init_callback]: [{url_key}]: [{url_key}] info extracted\n{_for_print(info)}"
                        )

                    except Exception as e:

                        if "unsupported url" in str(e).lower():
                            self.list_unsup_urls.append(url_key)
                            _error = "unsupported_url"

                        elif any(
                            _ in str(e).lower()
                            for _ in [
                                "not found",
                                "404",
                                "flagged",
                                "403",
                                "410",
                                "suspended",
                                "unavailable",
                                "disabled",
                            ]
                        ):
                            _error = "not_valid_url"
                            self.list_notvalid_urls.append(url_key)

                        else:
                            _error = repr(e)
                            self.list_urls_to_check.append((url_key, _error))

                        self.list_initnok.append((url_key, _error))
                        self.info_videos[url_key]["error"].append(_error)
                        self.info_videos[url_key]["status"] = "initnok"

                        logger.error(f"[init_callback]: [{url_key}]: [{_pending}/{_to_check}] [{url_key}] init nok - {_error}")
                        return

                else:
                    info = vid

                async def go_for_dl(urlkey, infdict, extradict=None):
                    # sanitizamos 'id', y si no lo tiene lo forzamos a un valor basado en la url
                    if _id := infdict.get("id"):

                        infdict["id"] = (
                            sanitize_filename(_id, restricted=True)
                            .replace("_", "")
                            .replace("-", "")
                        )

                    else:
                        infdict["id"] = str(
                            int(
                                hashlib.sha256(
                                    urlkey.encode("utf-8")
                                ).hexdigest(),
                                16,
                            )
                            % 10**8
                        )

                    if _title := infdict.get("title"):
                        _title = sanitize_filename(
                            _title[:150], restricted=True
                        )
                        infdict["title"] = _title

                    if extradict:
                        if not infdict.get("release_timestamp") and (
                            _mtime := extradict.get("release_timestamp")
                        ):

                            infdict["release_timestamp"] = _mtime
                            infdict["release_date"] = extradict.get(
                                "release_date"
                            )

                    logger.debug(
                        f"[init_callback]: [{url_key}]: [{_pending}/{_to_check}] [{infdict.get('id')}][{infdict.get('title')}] info extracted"
                    )
                    logger.debug(
                        f"[init_callback]: [{url_key}]: [{urlkey}] info extracted\n{_for_print(infdict)}"
                    )

                    self.info_videos[urlkey].update({"video_info": infdict})

                    _filesize = (
                        none_to_zero(extradict.get("filesize", 0))
                        if extradict
                        else none_to_zero(infdict.get("filesize", 0))
                    )

                    if _path := await self.async_check_if_aldl(infdict):

                        logger.debug(
                            f"[init_callback]: [{url_key}][{infdict.get('id')}][{infdict.get('title')}] already DL"
                        )

                        if _filesize:
                            async with self.alock:
                                self.totalbytes2dl -= _filesize

                        self.info_videos[urlkey].update(
                            {"status": "done", "aldl": _path}
                        )
                        return False

                    if _same_video_url := await self.async_check_if_same_video(
                        urlkey
                    ):
                        if _filesize:
                            async with self.alock:
                                self.totalbytes2dl -= _filesize

                        self.info_videos[urlkey].update(
                            {"samevideo": _same_video_url}
                        )
                        logger.warning(
                            f"[{urlkey}]: has not been added to video list because it gets same video than {_same_video_url}"
                        )
                        return False

                    return True

                async def get_dl(urlkey, infdict, extradict=None):

                    if (
                        await go_for_dl(urlkey, infdict, extradict)
                    ) and not self.args.nodl:

                        async def async_videodl_init(*args, **kwargs) -> VideoDownloader:
                            if not self.is_ready_to_dl.is_set():
                                await self.is_ready_to_dl.wait()
                            return await sync_to_async(VideoDownloader, executor=self.ex_winit)(*args, **kwargs)  # type: ignore

                        dl = await async_videodl_init(
                            self.info_videos[urlkey]["video_info"],
                            self.ytdl, self.args,
                            self.hosts_downloading,
                            self.alock,
                            self.hosts_alock
                        )

                        logger.debug(
                            f"[init_callback]: [{url_key}]: [{dl.info_dict['id']}][{dl.info_dict['title']}]: {dl.info_dl}"
                        )

                        _filesize = (
                            none_to_zero(extradict.get("filesize", 0))
                            if extradict
                            else none_to_zero(infdict.get("filesize", 0))
                        )

                        if not dl.info_dl.get("status", "") == "error":

                            if dl.info_dl.get("filesize"):
                                self.info_videos[urlkey]["video_info"][
                                    "filesize"
                                ] = dl.info_dl.get("filesize")
                                async with self.alock:
                                    self.totalbytes2dl = (
                                        self.totalbytes2dl
                                        - _filesize
                                        + dl.info_dl.get("filesize", 0)
                                    )

                            self.info_videos[urlkey].update(
                                {
                                    "status": "initok",
                                    "filename": str(dl.info_dl.get("filename")),
                                    "dl": str(dl),
                                }
                            )

                            async with self.alock:
                                self.getlistvid_first.set()
                                dl.index = len(self.list_dl) + 1
                                self.list_dl.update({dl.index: dl})

                            if dl.info_dl["status"] in (
                                "init_manipulating",
                                "done",
                            ):

                                await self.WorkersRun.add_dl(dl, urlkey)

                                logger.info(
                                    f"[init_callback]: [{url_key}][{dl.info_dict['id']}][{dl.info_dict['title']}] init OK, video parts DL"
                                )

                            else:

                                _msg = ""

                                if not self.args.nodl:
                                    await self.WorkersRun.add_dl(dl, urlkey)
                                    if dl.info_dl.get("auto_pasres"):
                                        self.list_pasres.add(dl.index)
                                        _msg = f", add this dl[{dl.index}] to auto_pasres{list(self.list_pasres)}"
                                        logger.debug(
                                            f"[init_callback]: [{url_key}][{dl.info_dict['id']}][{dl.info_dict['title']}] pause-resume update{_msg}"
                                        )

                                logger.debug(
                                    f"[init_callback]: [{url_key}][{dl.info_dict['id']}][{dl.info_dict['title']}] init OK, ready to DL{_msg}"
                                )

                        else:
                            async with self.alock:
                                self.totalbytes2dl -= _filesize

                            raise Exception("no DL init")

                if (_type := info.get("_type", "video")) == "video":

                    if self.wkinit_stop:
                        logger.info(f"[init_callback]: [{url_key}]: BLOCKED")

                        while self.wkinit_stop:
                            await async_wait_time(5)
                            logger.debug(f"[init_callback]: [{url_key}]: BLOCKED")

                        logger.info(f"[init_callback]: [{url_key}]: UNBLOCKED")

                    if not self.STOP.is_set():
                        await get_dl(url_key, infdict=info, extradict=vid)

                elif _type == "playlist":

                    logger.warning(f"[init_callback]: [{url_key}]: [{url_key}] playlist en worker_init")
                    self.info_videos[url_key]["todl"] = False

                    for _entry in info["entries"]:

                        if (_type := _entry.get("_type", "video")) != "video":
                            logger.warning(f"[init_callback]: [{url_key}]: [{url_key}] playlist of entries that are not videos")
                            continue
                        else:
                            _url = _entry.get("original_url") or _entry.get("url")
                            try:

                                if not self.info_videos.get(_url):  # es decir, los nuevos videos

                                    self.info_videos[_url] = {
                                        "source": "playlist",
                                        "video_info": _entry,
                                        "status": "init",
                                        "aldl": False,
                                        "todl": True,
                                        "ie_key": _entry.get("ie_key")
                                        or _entry.get("extractor_key"),
                                        "error": [],
                                    }

                                    _same_video_url = (
                                        await self.async_check_if_same_video(
                                            _url
                                        )
                                    )

                                    if _same_video_url:

                                        self.info_videos[_url].update(
                                            {"samevideo": _same_video_url}
                                        )
                                        logger.warning(
                                            f"{_url}: has not been added to video list because it gets same video than {_same_video_url}"
                                        )
                                        await self.async_prepare_for_dl(
                                            _url, put=False
                                        )

                                    else:

                                        try:
                                            await self.async_prepare_for_dl(_url, put=False)
                                            if self.wkinit_stop:
                                                logger.info(f"[init_callback]: [{url_key}]: BLOCKED")

                                                while self.wkinit_stop:
                                                    await async_wait_time(5)
                                                    logger.debug(
                                                        f"[init_callback]: [{url_key}]: BLOCKED"
                                                    )

                                                logger.info(
                                                    f"[init_callback]: [{url_key}]: UNBLOCKED"
                                                )
                                            if not self.STOP.is_set():
                                                await get_dl(
                                                    _url, infdict=_entry
                                                )

                                        except Exception:
                                            raise
                                        finally:
                                            async with self.alock:
                                                self.num_videos_pending -= 1

                            except Exception as e:

                                self.list_initnok.append(
                                    (_entry, f"Error:{repr(e)}")
                                )
                                logger.error(
                                    f"[init_callback]: [{url_key}]: [{_url}] init nok - Error:{repr(e)}"
                                )

                                self.list_urls_to_check.append((_url, repr(e)))
                                self.info_videos[_url]["error"].append(
                                    f"DL constructor error:{repr(e)}"
                                )
                                self.info_videos[_url]["status"] = "initnok"

            except Exception as e:
                self.list_initnok.append((vid, f"Error:{repr(e)}"))
                logger.error(f"[init_callback]: [{url_key}]: [{_pending}/{_to_check}] [{url_key}] init nok - Error:{repr(e)}")
                self.list_urls_to_check.append((url_key, repr(e)))
                self.info_videos[url_key]["error"].append(
                    f"DL constructor error:{repr(e)}"
                )
                self.info_videos[url_key]["status"] = "initnok"
            finally:
                async with self.alock:
                    self.num_videos_pending -= 1

        except BaseException as e:
            logger.error(f"[init_callback]: [{url_key}]: Error:{repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise

    async def run_callback(self, dl, url_key):

        try:

            self.list_pasres.discard(dl.index)
            if dl.info_dl["status"] == "init_manipulating":

                logger.info(f"[run_callback] start to manip {dl.info_dl['title']}")
                task_run_manip = asyncio.create_task(dl.run_manip())
                done, _ = await asyncio.wait([task_run_manip])

                for d in done:
                    try:
                        d.result()
                    except Exception as e:
                        logger.exception(
                            f"[run_callback] [{dl.info_dict['title']}]: Error with video manipulation - {repr(e)}"
                        )

                        self.info_videos[url_key]["error"].append(
                            f"\n error with video manipulation {str(e)}"
                        )

                if dl.info_dl["status"] == "done":
                    self.info_videos[url_key].update({"status": "done"})
                else:
                    self.info_videos[url_key].update({"status": "nok"})

            elif dl.info_dl["status"] == "stop":
                logger.debug(f"[run_callback][{url_key}]: STOPPED")
                self.info_videos[url_key]["error"].append("dl stopped")
                self.info_videos[url_key]["status"] = "nok"

            elif dl.info_dl["status"] == "error":

                logger.error(
                    f"[run_callback][{url_key}]: error when dl video, can't go por manipulation - {dl.info_dl.get('error_message')}"
                )
                self.info_videos[url_key]["error"].append(
                    f"error when dl video: {dl.info_dl.get('error_message')}"
                )
                self.info_videos[url_key]["status"] = "nok"

            else:

                logger.error(
                    f"[run_callback][{url_key}]: STATUS NOT EXPECTED: {dl.info_dl['status']}"
                )
                self.info_videos[url_key]["error"].append(
                    f"error when dl video: {dl.info_dl.get('error_message')}"
                )
                self.info_videos[url_key]["status"] = "nok"

        except Exception as e:
            logger.exception(f"[run_callback][{url_key}]: error {repr(e)}")

    async def async_ex(self):

        self.STOP = asyncio.Event()

        self.getlistvid_done = asyncio.Event()
        self.getlistvid_done.name = "done"  # type: ignore
        self.getlistvid_first = asyncio.Event()
        self.getlistvid_first.name = "first"  # type: ignore
        self.is_ready_to_dl = asyncio.Event()

        self.alock = asyncio.Lock()
        self.hosts_alock = asyncio.Lock()

        self.async_prepare_for_dl = sync_to_async(self._prepare_for_dl, executor=self.ex_winit)
        self.async_prepare_entry_pl_for_dl = sync_to_async(self._prepare_entry_pl_for_dl, executor=self.ex_winit)

        self.async_check_if_aldl = sync_to_async(self._check_if_aldl, executor=self.ex_winit)
        self.async_check_if_same_video = sync_to_async(self._check_if_same_video, executor=self.ex_winit)

        tasks_gui = []
        tasks_to_wait = {}

        self.t1.start()
        self.t2.start()
        self.t3.start()

        try:

            self.localstorage = LocalVideos(self)
            self.loop = asyncio.get_running_loop()

            if not self.args.nodl:
                self.nwsetup = NWSetUp(self)

            self.localstorage.ready()  # bloquea pero de todas formas necesitamos el resultado para progresar

            self.WorkersInit = WorkersInit(self)
            self.WorkersRun = WorkersRun(self)
            tasks_to_wait.update({asyncio.create_task(self.get_list_videos()): "task_get_videos"})

            if not self.args.nodl:
                await self.nwsetup.init()
                self.is_ready_to_dl.set()
                _res = await async_waitfortasks(events=(self.getlistvid_first, self.getlistvid_done))
                if _res.get("event") == "first" or len(self.videos_to_dl) > 0:
                    self.FEgui = FrontEndGUI(self)
                    tasks_gui = [asyncio.create_task(self.FEgui.gui())]
                    tasks_to_wait.update({asyncio.create_task(self.WorkersRun.exit.wait()): "task_workers_run"})

            done, _ = await asyncio.wait(tasks_to_wait)
            for d in done:
                try:
                    d.result()
                except BaseException as e:
                    logger.error(f"[async_ex][{tasks_to_wait[d]}] {repr(e)}")
                    if isinstance(e, KeyboardInterrupt):
                        raise

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                print("")
            logger.exception(f"[async_ex] {repr(e)}")
            self.STOP.set()
            await asyncio.sleep(0)
            try_get(self.ytdl.params["stop"], lambda x: x.set())
            if self.list_dl:
                for _, dl in self.list_dl.items():
                    await dl.stop()
            await asyncio.sleep(0)
            raise
        finally:
            if hasattr(self, 'FEgui'):
                self.FEgui.stop.set()
                await asyncio.sleep(0)
                if tasks_gui:
                    await async_waitfortasks(tasks_gui)
                self.FEgui.close()

            logger.info("[async_ex] BYE")

    def get_results_info(self):
        def _getter(url: str, vid: dict) -> str:
            webpageurl = try_get(traverse_obj(vid, ("video_info", "webpage_url")), lambda x: str(x) if x else None)
            originalurl = try_get(traverse_obj(vid, ("video_info", "original_url")), lambda x: str(x) if x else None)
            playlist = traverse_obj(vid, ("video_info", "playlist"))
            if not webpageurl and not originalurl and not playlist:
                return url
            nentries = try_get(traverse_obj(vid, ("video_info", "n_entries")), lambda x: int(x) if x else None)
            if playlist and nentries and nentries > 1:
                return f"[{traverse_obj(vid, ('video_info','playlist_index'))}]:{originalurl or webpageurl}:{url}"
            else:
                _url = originalurl or webpageurl or ""
                return _url

        def _print_list_videos():
            try:

                col = shutil.get_terminal_size().columns

                list_videos = [
                    _getter(url, vid)
                    for url, vid in self.info_videos.items()
                    if vid.get("todl")
                ]

                if list_videos:
                    list_videos_str = [[fill(text=url, width=col // 2)] for url in list_videos]
                else:
                    list_videos_str = []

                list_videos2dl = [
                    _getter(url, vid)
                    for url, vid in self.info_videos.items()
                    if not vid.get("aldl")
                    and not vid.get("samevideo")
                    and vid.get("todl")
                    and vid.get("status") != "prenok"
                ]

                list_videos2dl_str = (
                    [
                        [
                            fill(vid["video_info"].get("id", ""), col // 6),
                            fill(vid["video_info"].get("title", ""), col // 6),
                            naturalsize(
                                none_to_zero(vid["video_info"].get("filesize", 0))
                            ),
                            fill(_getter(url, vid), col // 2),
                        ]
                        for url, vid in self.info_videos.items()
                        if not vid.get("aldl")
                        and not vid.get("samevideo")
                        and vid.get("todl")
                        and vid.get("status") != "prenok"
                    ]
                    if list_videos2dl
                    else []
                )

                list_videosaldl = [
                    _getter(url, vid)
                    for url, vid in self.info_videos.items()
                    if vid.get("aldl") and vid.get("todl")
                ]
                list_videosaldl_str = (
                    [
                        [
                            fill(vid["video_info"].get("id", ""), col // 6),
                            fill(vid["video_info"].get("title", ""), col // 6),
                            fill(_getter(url, vid), col // 3),
                            fill(vid["aldl"], col // 3),
                        ]
                        for url, vid in self.info_videos.items()
                        if vid.get("aldl") and vid.get("todl")
                    ]
                    if list_videosaldl
                    else []
                )

                list_videossamevideo = [
                    _getter(url, vid)
                    for url, vid in self.info_videos.items()
                    if vid.get("samevideo")
                ]
                list_videossamevideo_str = (
                    [
                        [
                            fill(vid["video_info"].get("id", ""), col // 6),
                            fill(vid["video_info"].get("title", ""), col // 6),
                            fill(_getter(url, vid), col // 3),
                            fill(vid["samevideo"], col // 3),
                        ]
                        for url, vid in self.info_videos.items()
                        if vid.get("samevideo")
                    ]
                    if list_videossamevideo
                    else []
                )

                logger.info(
                    f"Total videos [{len(list_videos)}]\nTo DL [{(_tv2dl := len(list_videos2dl))}]\nAlready DL [{len(list_videosaldl)}]\nSame requests [{len(list_videossamevideo)}]"
                )
                logger.info(f"Total bytes to DL: [{naturalsize(self.totalbytes2dl)}]")

                _columns = ["URL"]
                tab_tv = (
                    tabulate(
                        list_videos_str,
                        showindex=True,
                        headers=_columns,
                        tablefmt="simple",
                    )
                    if list_videos_str
                    else None
                )

                _columns = ["ID", "Title", "Size", "URL"]
                tab_v2dl = (
                    tabulate(
                        list_videos2dl_str,
                        showindex=True,
                        headers=_columns,
                        tablefmt="simple",
                    )
                    if list_videos2dl_str
                    else None
                )

                logger.debug(f"%no%\n\n{tab_tv}\n\n")
                try:
                    if tab_v2dl:
                        logger.info(f"Videos to DL: [{_tv2dl}]")
                        logger.info(f"%no%\n\n\n{tab_v2dl}\n\n\n")
                    else:
                        logger.info("Videos to DL: []")

                except Exception as e:
                    logger.exception(f"[print_videos] {repr(e)}")

                return {
                    "videos": {"urls": list_videos, "str": list_videos_str},
                    "videos2dl": {"urls": list_videos2dl, "str": list_videos2dl_str},
                    "videosaldl": {"urls": list_videosaldl, "str": list_videosaldl_str},
                    "videossamevideo": {
                        "urls": list_videossamevideo,
                        "str": list_videossamevideo_str,
                    },
                }
            except Exception as e:
                logger.exception(repr(e))
                return {}

        _videos_url_notsupported = self.list_unsup_urls
        _videos_url_notvalid = self.list_notvalid_urls
        _videos_url_tocheck = (
            [_url for _url, _ in self.list_urls_to_check]
            if self.list_urls_to_check
            else []
        )
        _videos_url_tocheck_str = (
            [f"{_url}:{_error}" for _url, _error in self.list_urls_to_check]
            if self.list_urls_to_check
            else []
        )

        videos_okdl = []
        videos_kodl = []
        videos_koinit = []

        local_storage = Path(PATH_LOGS, "files_cached.json")

        with open(local_storage, "r") as f:
            _temp = json.load(f)

        for url, video in self.info_videos.items():
            if (
                not video.get("aldl")
                and not video.get("samevideo")
                and video.get("todl")
            ):
                if video["status"] == "done":
                    videos_okdl.append(_getter(url, video))

                    _temp["local"].update(
                        {
                            f"{traverse_obj(video, ('video_info', 'id'))}_{str(traverse_obj(video, ('video_info', 'title'), default='')).upper()}": str(
                                traverse_obj(video, "filename")
                            )
                        }
                    )
                    try:
                        _xattr_desc = xattr.getxattr(str(traverse_obj(video, "filename")), "user.dublincore.description").decode()
                        _temp["local"].update(
                            {
                                _xattr_desc: str(traverse_obj(video, "filename"))
                            }
                        )

                    except Exception:
                        pass

                else:
                    if (video["status"] == "initnok") or (video["status"] == "prenok"):
                        videos_kodl.append(_getter(url, video))
                        videos_koinit.append(_getter(url, video))
                    elif video["status"] == "initok":
                        if self.args.nodl:
                            videos_okdl.append(_getter(url, video))
                    else:
                        videos_kodl.append(_getter(url, video))

        with open(local_storage, "w") as f:
            json.dump(_temp, f)

        info_dict = _print_list_videos()

        info_dict.update(
            {
                "videosokdl": {"urls": videos_okdl},
                "videoskodl": {"urls": videos_kodl},
                "videoskoinit": {"urls": videos_koinit},
                "videosnotsupported": {"urls": _videos_url_notsupported},
                "videosnotvalid": {"urls": _videos_url_notvalid},
                "videos2check": {
                    "urls": _videos_url_tocheck,
                    "str": _videos_url_tocheck_str,
                },
            }
        )

        _columnsaldl = ["ID", "Title", "URL", "Path"]
        tab_valdl = (
            tabulate(
                info_dict["videosaldl"]["str"],
                showindex=True,
                headers=_columnsaldl,
                tablefmt="simple",
            )
            if info_dict["videosaldl"]["str"]
            else None
        )
        _columnssamevideo = ["ID", "Title", "URL", "Same URL"]
        tab_vsamevideo = (
            tabulate(
                info_dict["videossamevideo"]["str"],
                showindex=True,
                headers=_columnssamevideo,
                tablefmt="simple",
            )
            if info_dict["videossamevideo"]["str"]
            else None
        )

        if self.args.path:
            _path_str = f"--path {self.args.path} "
        else:
            _path_str = ""

        try:

            logger.info("******************************************************")
            logger.info("******************************************************")
            logger.info("*********** FINAL SUMMARY ****************************")
            logger.info("******************************************************")
            logger.info("******************************************************\n\n")
            logger.info(f"Request to DL: [{len(info_dict['videos']['urls'])}]\n\n")
            logger.info(f"         Already DL: [{len(info_dict['videosaldl']['urls'])}]")
            logger.info(f"         Same requests: [{len(info_dict['videossamevideo']['urls'])}]")
            logger.info(f"         Videos to DL: [{len(info_dict['videos2dl']['urls'])}]\n\n")
            logger.info(f"                 OK DL: [{len(videos_okdl)}]")
            logger.info(f"                 ERROR DL: [{len(videos_kodl)}]")
            logger.info(f"                     ERROR init DL: [{len(videos_koinit)}]")
            logger.info(f"                         UNSUP URLS: [{len(_videos_url_notsupported)}]")
            logger.info(f"                         NOTVALID URLS: [{len(_videos_url_notvalid)}]")
            logger.info(f"                         TO CHECK URLS: [{len(_videos_url_tocheck)}]\n\n")
            logger.info("*********** VIDEO RESULT LISTS **************************\n\n")
            if tab_valdl:
                logger.info("Videos ALREADY DL:\n")
                logger.info(f"%no%\n\n{tab_valdl}\n\n")
            else:
                logger.info("Videos ALREADY DL: []")
            if tab_vsamevideo:
                logger.info("SAME requests:\n")
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
                logger.info("Videos TOTAL ERROR DL: []")
            if videos_koinit:
                logger.info("Videos ERROR INIT DL:")
                logger.info(f"%no%\n\n{videos_koinit} \n[{_path_str}-u {' -u '.join(videos_koinit)}]")
            if _videos_url_notsupported:
                logger.info("Unsupported URLS:")
                logger.info(f"%no%\n\n{_videos_url_notsupported}")
            if _videos_url_notvalid:
                logger.info("Not Valid URLS:")
                logger.info(f"%no%\n\n{_videos_url_notvalid}")
            if _videos_url_tocheck:
                logger.info("To check URLS:")
                logger.info(f"%no%\n\n{_videos_url_tocheck}")
            logger.info("*****************************************************")
            logger.info("*****************************************************")
            logger.info("*****************************************************")
            logger.info("*****************************************************")
        except Exception as e:
            logger.exception(f"[get_results] {repr(e)}")

        logger.debug(f"\n{_for_print_videos(self.info_videos)}")

        videos_ko = list(set(info_dict["videoskodl"]["urls"] + info_dict["videoskoinit"]["urls"]))

        if videos_ko:
            videos_ko_str = "\n".join(videos_ko)
        else:
            videos_ko_str = ""

        with open(Path(PATH_LOGS, "error_links.txt"), "w") as file:
            file.write(videos_ko_str)

        return info_dict

    def close(self):

        try:

            logger.info("[close] start to close")

            try:
                if not self.STOP.is_set():
                    self.t2.stop()
                    if hasattr(self, 'FEgui') and self.FEgui.dl_media_str:
                        logger.info(f"[close] {self.FEgui.dl_media_str}")
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")

            try:
                if hasattr(self, 'nwsetup'):
                    self.nwsetup.close()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")

            try:
                self.ytdl.shutdown()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")

            if self.list_dl:
                for _, vdl in self.list_dl.items():
                    try:
                        vdl.shutdown()
                    except Exception as e:
                        logger.exception(f"[close] {repr(e)}")

            try:
                logger.info("[close] kill processes")
                kill_processes(logger=logger, rpcport=self.args.rpcport)
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")

        except BaseException as e:
            logger.exception(f"[close] {repr(e)}")
            raise
