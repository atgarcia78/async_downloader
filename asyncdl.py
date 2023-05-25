import asyncio
import json
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import zip_longest
from collections import deque, defaultdict
import signal
import re

from pathlib import Path

import os as syncos

from textwrap import fill
from threading import Lock

from codetiming import Timer
from tabulate import tabulate

from utils import (
    PATH_LOGS,
    CONF_PLAYLIST_INTERL_URLS,
    MAXLEN_TITLE,
    FrontEndGUI,
    MySyncAsyncEvent,
    NWSetUp,
    LocalVideos,
    _for_print,
    _for_print_videos,
    sync_to_async,
    async_waitfortasks,
    get_domain,
    init_ytdl,
    js_to_json,
    kill_processes,
    naturalsize,
    none_to_zero,
    print_tasks,
    sanitize_filename,
    traverse_obj,
    try_get,
    Union,
    cast,
    add_task
)

from videodownloader import VideoDownloader

logger = logging.getLogger('asyncDL')


def get_dif_interl(_dict, _interl, workers):
    '''
    get dif in the interl list if distance of elements
    with same host is less than num runners dl workers of asyncdl
    '''
    dif = defaultdict(lambda: [])
    for host, group in _dict.items():
        index_old = None
        _group = sorted(group, key=lambda x: _interl.index(x))
        for el in _group:
            index = _interl.index(el)
            if index_old:
                if index - index_old < workers:
                    dif[host].append(el)
            index_old = index
    return dif


def get_list_interl(res, asyncdl, _pre):
    if not res:
        return []
    if len(res) < 3:
        return res
    _dict = defaultdict(lambda: [])
    for ent in res:
        _dict[get_domain(ent["url"])].append(ent['id'])

    logger.info(
        f"{_pre}[get_list_interl]  entries" +
        f"interleave: {len(list(_dict.keys()))} different hosts" +
        f"longest with {len(max(list(_dict.values()), key=len))} entries")

    _workers = asyncdl.workers
    while _workers > asyncdl.workers // 2:

        _interl = []
        for el in list(zip_longest(*list(_dict.values()))):
            _interl.extend([_el for _el in el if _el])

        for tunein in range(3):

            dif = get_dif_interl(_dict, _interl, _workers)

            if dif:
                if tunein < 2:
                    for i, host in enumerate(list(dif.keys())):
                        group = [el for el in _dict[host]]

                        for j, el in enumerate(group):
                            _interl.pop(_interl.index(el))
                            _interl.insert(_workers*(j+1) + i, el)
                    continue
                else:
                    logger.info(f"{_pre}[get_list_interl] tune in NOK, try with less num of workers")
                    _workers -= 1
                    break

            else:
                logger.info(f"{_pre}[get_list_interl] tune in OK, no dif with workers[{_workers}]")
                asyncdl.workers = _workers
                return sorted(res, key=lambda x: _interl.index(x['id']))

    return sorted(res, key=lambda x: _interl.index(x['id']))


class WorkersRun:
    _running = set()
    _waiting = deque()
    _tasks = {}
    _info_dl = {}
    _alock = asyncio.Lock()
    _exit = MySyncAsyncEvent("workersrunexit")
    _max = 0

    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger('WorkersRun')
        self.max = asyncdl.workers

    @property
    def exit(self):
        return WorkersRun._exit

    @property
    def max(self):
        return WorkersRun._max

    @max.setter
    def max(self, value):
        WorkersRun._max = value

    @property
    def running_count(self):
        return len(WorkersRun._running)

    @property
    def waiting_count(self):
        return len(WorkersRun._waiting)

    @property
    def running(self):
        return WorkersRun._running

    @property
    def waiting(self):
        return WorkersRun._waiting

    async def add_worker(self):

        async with WorkersRun._alock:
            WorkersRun._max += 1
            if WorkersRun._waiting:
                if self.running_count < WorkersRun._max:
                    dl_index = WorkersRun._waiting.popleft()
                    self._start_task(dl_index)

    async def del_worker(self):
        async with WorkersRun._alock:
            if WorkersRun._max > 0:
                WorkersRun._max -= 1

    async def check_to_stop(self, force=False):
        self.logger.info(f'[check_to_stop] force[{force}] running[{self.running_count}] waiting[{self.waiting_count}]')
        async with WorkersRun._alock:
            if (not force and not WorkersRun._waiting and not WorkersRun._running) or force:
                if force:
                    WorkersRun._waiting.clear()
                self.logger.info('[check_to_stop] set exit')
                WorkersRun._exit.set()
                self.asyncdl.end_dl.set()
                self.logger.debug("end_dl set")

    async def move_to_waiting_top(self, dl_index):
        async with WorkersRun._alock:
            if dl_index in WorkersRun._waiting:
                if WorkersRun._waiting.index(dl_index) > 0:
                    WorkersRun._waiting.remove(dl_index)
                    WorkersRun._waiting.appendleft(dl_index)
                    self.logger.debug(f'[move_to_waiting_top] {list(self.waiting)}')
            elif dl_index not in WorkersRun._running and WorkersRun._info_dl[dl_index]['dl'].info_dl['status'] == "stop":
                await WorkersRun._info_dl[dl_index]['dl'].reinit()
                if self.running_count >= WorkersRun._max:
                    WorkersRun._waiting.append(dl_index)
                else:
                    self._start_task(dl_index)

    async def add_dl(self, dl, url_key):
        _pre = f"[add_dl]:[{dl.info_dict['id']}][{dl.info_dict['title']}][{url_key}]"

        WorkersRun._info_dl.update({dl.index: {'url': url_key, 'dl': dl}})

        self.logger.debug(
            f'{_pre} add dl. Running [{self.running_count}]' +
            f'Waiting[{self.waiting_count}]')

        async with WorkersRun._alock:
            if self.running_count >= WorkersRun._max:
                WorkersRun._waiting.append(dl.index)
            else:
                self._start_task(dl.index)

    def _start_task(self, dl_index):
        WorkersRun._running.add(dl_index)
        url_key = WorkersRun._info_dl[dl_index]['url']
        WorkersRun._tasks.update({add_task(self._task(dl_index), self.asyncdl.background_tasks): url_key})
        self.logger.debug(f'[{url_key}] task ok {print_tasks(WorkersRun._tasks)}')

    async def _task(self, dl_index):
        url_key, dl = WorkersRun._info_dl[dl_index]['url'], WorkersRun._info_dl[dl_index]['dl']
        _pre = f"[_task]:[{dl.info_dict['id']}][{dl.info_dict['title']}][{url_key}]"

        try:
            if dl.info_dl['status'] not in ('init_manipulating', 'done'):
                if dl.info_dl.get("auto_pasres"):
                    self.asyncdl.list_pasres.add(dl.index)
                    _msg = f", added this dl[{dl.index}] to auto_pasres{list(self.asyncdl.list_pasres)}"
                    self.logger.debug(f"{_pre} pause-resume update{_msg}")

                await async_waitfortasks(
                    dl.run_dl(), background_tasks=self.asyncdl.background_tasks)

            await async_waitfortasks(
                self.asyncdl.run_callback(dl, url_key), background_tasks=self.asyncdl.background_tasks)

            async with WorkersRun._alock:
                WorkersRun._running.remove(dl_index)
                if WorkersRun._waiting:
                    if self.running_count < WorkersRun._max:
                        dl_index2 = WorkersRun._waiting.popleft()
                        self._start_task(dl_index2)

        except Exception as e:
            self.logger.exception(f'{_pre} error {repr(e)}')
        finally:
            self.logger.debug(f'{_pre} end task worker run')
            if self.asyncdl.WorkersInit.exit.is_set():
                self.logger.debug(f'{_pre} WorkersInit.exit is set')
                if not WorkersRun._waiting and not WorkersRun._running:
                    self.logger.debug(f'{_pre} no running no waiting, lets set exit ')
                    WorkersRun._exit.set()
                    self.asyncdl.end_dl.set()
                    self.logger.debug("end_dl set")
                else:
                    self.logger.debug(f'{_pre} there are videos running or waiting, so lets exit')
            else:
                self.logger.debug(f'{_pre} WorkersInit.exit not set')
                if not WorkersRun._waiting and not WorkersRun._running:
                    self.logger.debug(f'{_pre} there are no videos running or waiting, so lets wait for WorkersInit.exit')
                    await self.asyncdl.WorkersInit.exit.async_wait()
                    if not WorkersRun._waiting and not WorkersRun._running:
                        self.logger.debug(f'{_pre} WorkersInit.exit is set after waiting, no running no waiting, lets set exit')
                        WorkersRun._exit.set()
                        self.asyncdl.end_dl.set()
                        self.logger.info("end_dl set")
                    else:
                        self.logger.debug(
                            f'{_pre} WorkersInit.exit is set after waiting, there are videos running or waiting, so lets exit')
                else:
                    self.logger.debug(f'{_pre} there are videos running or waiting, so lets exit')


class WorkersInit:
    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.max = self.asyncdl.init_nworkers
        self.running = set()
        self.waiting = deque()
        self.tasks = {}
        self.alock = asyncio.Lock()
        self.exit = MySyncAsyncEvent("workersinitexit")
        self.logger = logging.getLogger('WorkersInit')

    @property
    def running_count(self):
        return len(self.running)

    @property
    def waiting_count(self):
        return len(self.waiting)

    async def add_init(self, url_key):

        _pre = f"[add_init]:[{url_key}]"

        async with self.alock:
            self.logger.debug(
                f'{_pre} init. Running [{self.running_count}] Waiting[{len(self.waiting)}]')
            if self.running_count >= self.max:
                self.waiting.append(url_key)
            else:
                self._start_task(url_key)

    def _start_task(self, url_key):

        self.running.add(url_key)

        self.tasks.update({add_task(self._task(url_key), self.asyncdl.background_tasks): url_key})
        self.logger.debug(f'[{url_key}] task ok {print_tasks(self.tasks)}')

    async def _task(self, url_key):

        _pre = f"[_task]:[{url_key}]"

        try:
            if url_key == 'KILL':
                async with self.alock:
                    self.running.remove(url_key)

                if self.waiting:
                    while self.waiting_count:
                        await asyncio.sleep(0)

                if self.running:
                    while self.running_count:
                        await asyncio.sleep(0)

                self.logger.debug(f'{_pre} end tasks worker init: exit')
                self.asyncdl.t3.stop()
                self.exit.set()
                await self.asyncdl.WorkersRun.check_to_stop()
            else:
                await async_waitfortasks(
                    self.asyncdl.init_callback(url_key), background_tasks=self.asyncdl.background_tasks)

                async with self.alock:
                    self.running.remove(url_key)
                    if self.waiting:
                        if self.running_count < self.max:
                            url = self.waiting.popleft()
                            self._start_task(url)

        except Exception as e:
            self.logger.exception(f'{_pre} error {repr(e)}')


class AsyncDL:

    def __init__(self, args):

        self.background_tasks = set()
        # args
        self.args = args
        self.workers = self.args.w
        self.init_nworkers = self.args.winit or self.args.w

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
            logger=logger.info)
        self.t2 = Timer(
            "execution",
            text="[timers] Time DL: {:.2f}",
            logger=logger.info)
        self.t3 = Timer(
            "execution",
            text="[timers] Time spent by init workers: {:.2f}",
            logger=logger.info)

        self.localstorage = LocalVideos(self)

    async def cancel_all_dl(self):
        self.WorkersRun.max = 0
        self.WorkersRun.waiting.clear()
        self.STOP.set()
        await asyncio.sleep(0)
        await self.ytdl.stop()
        if self.list_dl:
            for _, dl in self.list_dl.items():
                await dl.stop("exit")
                await asyncio.sleep(0)
        await self.WorkersRun.check_to_stop()

    def print_pending_tasks(self):
        try:
            pending_tasks = asyncio.all_tasks()
            # logger.debug(f"[pending_all_tasks] {pending_tasks}")
            logger.debug(f"[pending_all_tasks]\n{print_tasks(pending_tasks)}")
        except Exception as e:
            logger.exception(f"[print_pending_tasks]: error: {repr(e)}")

    async def get_list_videos(self):

        logger.debug("[get_list_videos] start")

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
                            _temp.add(re.sub(r'#\d+$', '', _url))

                _url_list_caplinks = list(_temp)
                logger.info(f"[get_list_videos] video list caplinks:\n{_url_list_caplinks}")
                with open(prevfilecaplinks, "a") as file:
                    _text = '\n'.join(_url_list_caplinks)
                    file.write(f"\n\n[{self.launch_time.strftime('%Y-%m-%d %H:%M')}]\n{_text}")

                with open(filecaplinks, "w") as file:
                    file.write("")

                _url_list["caplinks"] = _url_list_caplinks

            if self.args.collection:
                if self.STOP.is_set():
                    raise Exception("STOP")
                _url_list_cli = list(dict.fromkeys(list(map(lambda x: re.sub(r'#\d+$', '', x), self.args.collection))))
                logger.info(f"[get_list_videos] video list cli:\n{_url_list_cli}")
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

                                self.info_videos[_url].update({"samevideo": _same_video_url})
                                logger.warning(
                                    f"{_url}: has not been added to video list" +
                                    f"because it gets same video than {_same_video_url}")
                                await self._prepare_for_dl(_url)

                            else:
                                await self._prepare_for_dl(_url)
                                self.list_videos.append(self.info_videos[_url]["video_info"])
                    else:
                        await self._prepare_entry_pl_for_dl(_vid)

            logger.debug(
                f"[get_list_videos] list videos: \n{_for_print_videos(self.list_videos)}")

            logger.debug(
                f"[get_list_videos] Initial # urls:\n\tCLI[{len(_url_list_cli )}]\n\t" +
                f"CAP[{len(_url_list_caplinks)}]")

            if _url_list:

                for _source, _ulist in _url_list.items():

                    if self.STOP.is_set():
                        raise Exception("STOP")

                    for _elurl in _ulist:

                        if self.STOP.is_set():
                            raise Exception("STOP")

                        is_pl, ie_key = self.ytdl.is_playlist(_elurl)

                        if not is_pl:

                            _entry = {"_type": "url",
                                      "url": _elurl,
                                      "ie_key": ie_key}

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

                                await self._prepare_for_dl(_elurl)
                                self.list_videos.append(_entry)

                        else:
                            if not self.url_pl_list.get(_elurl):
                                self.url_pl_list[_elurl] = {"source": _source}

                url_list = list(self.info_videos.keys())

                logger.debug(
                    f"[get_list_videos][url_list] Initial number of urls not pl [{len(url_list)}]")
                logger.debug(f"[get_list_videos][url_list] {url_list}")

                if self.url_pl_list:

                    logger.debug(
                        "[get_list_videos][url_playlist_list] Initial number of urls that " +
                        f"are pl [{len(self.url_pl_list)}]")
                    logger.debug(f"[get_list_videos][url_playlist_list]\n{self.url_pl_list}")
                    self._url_pl_entries = []
                    self._count_pl = 0
                    self.url_pl_list2 = []

                    if (len(self.url_pl_list) == 1 and self.args.use_path_pl
                            and not self.args.path):
                        _get_path_name = True
                    else:
                        _get_path_name = False

                    if self.STOP.is_set():
                        raise Exception("STOP")

                    self.url_pl_queue = asyncio.Queue()
                    for url in self.url_pl_list:
                        self.url_pl_queue.put_nowait(url)
                    for _ in range(min(self.init_nworkers,
                                       len(self.url_pl_list))):
                        self.url_pl_queue.put_nowait("KILL")
                    tasks_pl_list = [
                        add_task(self.process_playlist(_get_path_name), self.background_tasks)
                        for _ in range(min(self.init_nworkers, len(self.url_pl_list)))
                    ]

                    logger.debug(f"[get_list_videos][url_playlist_list] initial playlists: {len(self.url_pl_list)}")

                    await asyncio.wait(tasks_pl_list)

                    logger.debug(f"[get_list_videos][url_playlist_list] from initial playlists: {len(self.url_pl_list2)}")

                    if self.STOP.is_set():
                        raise Exception("STOP")

                    if self.url_pl_list2:
                        self.url_pl_queue = asyncio.Queue()
                        for url in self.url_pl_list2:
                            self.url_pl_queue.put_nowait(url)
                        for _ in range(min(self.init_nworkers,
                                           len(self.url_pl_list2))):
                            self.url_pl_queue.put_nowait("KILL")
                        tasks_pl_list2 = [
                            add_task(self.process_playlist(_get_path_name), self.background_tasks)
                            for _ in range(min(self.init_nworkers, len(self.url_pl_list2)))
                        ]

                        await asyncio.wait(tasks_pl_list2)

                    logger.info(
                        f"[get_list_videos] entries from playlists: {len(self._url_pl_entries)}")
                    logger.debug(
                        f"[get_list_videos]\n{_for_print_videos(self._url_pl_entries)}")

        except BaseException as e:
            logger.exception(f"[get_list_videos]: Error {repr(e)}")

        finally:
            await self.WorkersInit.add_init("KILL")
            if not self.STOP.is_set():
                self.t1.stop()

    async def process_playlist(self, _get: bool = False):

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

                _pre = f"[get_list_videos][process_playlist][{_url}]"

                logger.info(f"{_pre}[{self._count_pl}/{len(self.url_pl_list) + len(self.url_pl_list2)}] processing")

                try:
                    # _info = await self.ytdl.async_extract_info(_url, download=False, process=False)
                    _info = self.ytdl.sanitize_info(await self.ytdl.async_extract_info(_url, download=False))
                    if not _info:
                        raise Exception("no info")
                except Exception as e:
                    logger.warning(f"{_pre} {repr(e)}")

                    _info = {
                        "_type": "error",
                        "url": _url,
                        "error": repr(e)
                    }
                    await self._prepare_entry_pl_for_dl(_info)
                    self._url_pl_entries += [_info]
                    continue

                if (_info.get("_type", "video") != "playlist"):

                    if not _info.get("original_url"):
                        _info.update({"original_url": _url})

                    await self._prepare_entry_pl_for_dl(_info)
                    self._url_pl_entries += [_info]

                else:
                    if _get:
                        _title = sanitize_filename(_info.get('title'), restricted=True)
                        _name = f"{_title}{_info.get('extractor_key')}{_info.get('id')}"
                        self.args.path = str(Path(Path.home(), "testing", _name))
                        logger.debug(f"[get_list_videos][process_playlist] path for playlist {_url}:\n{self.args.path}")

                    if isinstance(_info.get("entries"), list):

                        _temp_error = []

                        _entries_ok = []
                        for _ent in _info["entries"]:
                            if _ent.get("error"):
                                _ent["_type"] = "error"
                                if not _ent.get("original_url"):
                                    _ent.update({"original_url": _url})
                                await self._prepare_entry_pl_for_dl(_ent)
                                self._url_pl_entries.append(_ent)
                                _temp_error.append(_ent)
                            else:
                                _entries_ok.append(_ent)

                        _info["entries"] = _entries_ok
                        # _info = self.ytdl.sanitize_info(
                        #     await self.ytdl.async_process_ie_result(_info, download=False))

                        # assert _info and isinstance(_info, dict)

                        if _info.get("extractor_key") in CONF_PLAYLIST_INTERL_URLS:
                            _temp_aldl = []
                            _temp_nodl = []

                            for _ent in _info["entries"]:
                                if (not await self.async_check_if_aldl(_ent, test=True)):
                                    _temp_nodl.append(_ent)
                                else:
                                    _temp_aldl.append(_ent)

                            _info["entries"] = get_list_interl(_temp_nodl, self, _pre) + _temp_aldl

                    for _ent in _info["entries"]:

                        if self.STOP.is_set():
                            raise Exception("STOP")

                        if _ent.get("_type", "video") == "video" and not _ent.get("error"):

                            # _ent = self.ytdl.sanitize_info(
                            #     await self.ytdl.async_process_ie_result(_ent, download=False))

                            # assert isinstance(_ent, dict)

                            if not _ent.get("original_url"):
                                _ent.update({"original_url": _url})
                            # elif _ent["original_url"] != _url:
                            #     _ent["playlist_url"] = _url
                            if ((_ent.get("extractor_key", _ent.get("ie_key", ""))).lower() == "generic"
                                    and (_ent.get("n_entries", 0) <= 1)):

                                _ent.pop("playlist", "")
                                _ent.pop("playlist_index", "")
                                _ent.pop("n_entries", "")
                                _ent.pop("playlist", "")
                                _ent.pop("playlist_id", "")
                                _ent.pop("playlist_title", "")

                            if (_wurl := _ent["webpage_url"]) == _ent["original_url"]:
                                if _ent.get("n_entries", 0) > 1:
                                    _ent.update({"webpage_url": f"{_wurl}?index={_ent['playlist_index']}"})
                                    logger.warning(
                                        f"{_pre}[{_ent['playlist_index']}]: nentries > 1, webpage_url == original_url: {_wurl}")

                            await self._prepare_entry_pl_for_dl(_ent)
                            self._url_pl_entries += [_ent]

                        else:
                            try:
                                is_pl, _ = self.ytdl.is_playlist(_ent["url"])
                                _error = _ent.get("error")
                                if not is_pl or _error:
                                    if not _ent.get("original_url"):
                                        _ent.update({"original_url": _url})
                                    if _error:
                                        _ent["_type"] = "error"
                                    await self._prepare_entry_pl_for_dl(_ent)
                                    self._url_pl_entries.append(_ent)
                                else:
                                    self.url_pl_list2.append(_ent["url"])

                            except Exception as e:
                                logger.error(f"{_pre} {_ent['url']} no video entries - {repr(e)}")

            except BaseException as e:
                logger.exception(f"[get_list_videos][process_playlist] {repr(e)}")
                if isinstance(e, KeyboardInterrupt):
                    raise

    def _check_if_aldl(self, info_dict, test=False):

        if (not (_id := info_dict.get("id")) or
                not (_title := info_dict.get("title"))):
            return False

        _pre = f"[check_if_aldl][{_id}][{_title}]"

        try:

            _title = sanitize_filename(_title[:MAXLEN_TITLE], restricted=True).upper()
            _id = sanitize_filename(_id, restricted=True).replace("_", "").replace("-", "")
            vid_name = f"{_id}_{_title}"

            if not (vid_path_str := self.videos_cached.get(vid_name)):
                return False
            else:  # video en local
                if test:
                    return True
                vid_path = Path(vid_path_str)
                logger.debug(f"{_pre} already DL")  #: {vid_path}")

                if not self.args.nosymlinks:
                    if self.args.path:
                        _folderpath = Path(self.args.path)
                    else:
                        _folderpath = Path(Path.home(), "testing", self.launch_time.strftime("%Y%m%d"))
                    _folderpath.mkdir(parents=True, exist_ok=True)
                    file_aldl = Path(_folderpath, vid_path.name)
                    if file_aldl not in _folderpath.iterdir():
                        file_aldl.symlink_to(vid_path)
                        try:
                            mtime = int(vid_path.stat().st_mtime)
                            syncos.utime(file_aldl, (int(time.time()), mtime), follow_symlinks=False)
                        except Exception as e:
                            logger.debug(
                                f"[check_if_aldl] [{str(file_aldl)}] -> " +
                                f"[{str(vid_path)}] error when copying times {repr(e)}")

                return vid_path_str

        except Exception as e:
            logger.warning(f'{_pre} error {repr(e)}')

    async def async_check_if_aldl(self, info_dict, test=False):
        return await sync_to_async(self._check_if_aldl, thread_sensitive=False, executor=self.ex_winit)(info_dict, test=test)

    def _check_if_same_video(self, url_to_check: str) -> Union[str, None]:

        info = self.info_videos[url_to_check]["video_info"]
        if (
            not info.get("_type", "video") == "video"
            or not (_id := info.get("id"))
            or not (_title := info.get("title"))
        ):
            return

        for (urlkey, _vid) in self.info_videos.items():
            if urlkey != url_to_check:
                if (
                    _vid["video_info"].get("_type", "video") == "video"
                    and (_vid["video_info"].get("id", "") == _id)
                    and (_vid["video_info"].get("title", "")) == _title
                ):
                    return urlkey

    async def async_check_if_same_video(self, url_to_check):
        return await sync_to_async(self._check_if_same_video, thread_sensitive=False, executor=self.ex_winit)(url_to_check)

    async def _prepare_for_dl(self, url: str, put: bool = True) -> None:
        self.info_videos[url].update({"todl": True})
        if _id := self.info_videos[url]["video_info"].get("id"):
            self.info_videos[url]["video_info"]["id"] = (
                sanitize_filename(_id, restricted=True)
                .replace("_", "")
                .replace("-", "")
            )

        if _title := self.info_videos[url]["video_info"].get("title"):
            self.info_videos[url]["video_info"]["title"] = sanitize_filename(
                _title[:MAXLEN_TITLE], restricted=True)
        if not self.info_videos[url]["video_info"].get("filesize", None):
            self.info_videos[url]["video_info"]["filesize"] = 0
        if (_path := await self.async_check_if_aldl(self.info_videos[url]["video_info"])):
            self.info_videos[url].update({"aldl": _path, "status": "done"})
            logger.debug(
                "[prepare_for_dl] " +
                f"[{self.info_videos[url]['video_info'].get('id')}]" +
                f"[{self.info_videos[url]['video_info'].get('title')}] already DL")

        if (
            self.info_videos[url].get("todl")
            and not self.info_videos[url].get("aldl")
            and not self.info_videos[url].get("samevideo")
            and self.info_videos[url].get("status") != "prenok"
        ):
            async with self.alock:
                self.videos_to_dl.append(url)
                if put:
                    await self.WorkersInit.add_init(url)
                self.num_videos_to_check += 1
                self.num_videos_pending += 1

    async def _prepare_entry_pl_for_dl(self, entry: dict) -> None:

        _pre = "[prepare_entry_pl_for_dl]"

        try:
            _type = entry.get("_type", "video")
            if _type == "playlist":
                logger.warning(f"{_pre} PLAYLIST IN PLAYLIST:{entry}")
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

                _same_video_url = await self.async_check_if_same_video(_url)

                if _same_video_url:

                    self.info_videos[_url].update({"samevideo": _same_video_url})

                    logger.warning(
                        f"[prepare_entry_pl_for_dl] {_url}: has not been added" +
                        f" to video list because it gets same video than {_same_video_url}")

                    await self._prepare_for_dl(_url)
                else:
                    await self._prepare_for_dl(_url)
                    self.list_videos.append(self.info_videos[_url]["video_info"])
            else:
                logger.debug(
                    f"{_pre} {_url}: has not been added to info_videos because it is already")

        except Exception as e:
            logger.exception(f'{_pre} error {repr(e)} with entry\n{entry}')

    async def get_dl(self, url_key):

        if not self.args.nodl:

            async def async_videodl_init(*args, **kwargs) -> VideoDownloader:
                if not self.is_ready_to_dl.is_set():
                    await self.is_ready_to_dl.async_wait()
                return await sync_to_async(
                    VideoDownloader, thread_sensitive=False, executor=self.ex_winit)(*args, **kwargs)  # type: ignore

            dl = await async_videodl_init(
                self.info_videos[url_key]["video_info"],
                self.ytdl, self.nwsetup, self.args,
                self.hosts_downloading,
                self.alock,
                self.hosts_alock)

            _pre = f"[init_callback][get_dl]:[{dl.info_dict.get('id')}][{dl.info_dict.get('title')}][{url_key}]:"

            logger.debug(f'{_pre} {dl.info_dl}')

            if dl.info_dl.get("status", "") == "error":
                raise Exception("no DL init")

            if (_filesize := dl.info_dl.get("filesize")):
                self.info_videos[url_key]["video_info"]["filesize"] = dl.info_dl.get("filesize")
                async with self.alock:
                    self.totalbytes2dl += _filesize

            self.info_videos[url_key].update(
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

            if dl.info_dl["status"] in ("init_manipulating", "done"):

                await self.WorkersRun.add_dl(dl, url_key)

                logger.info(f"{_pre} init OK video parts DL")

            else:

                await self.WorkersRun.add_dl(dl, url_key)

                logger.debug(f"{_pre} init OK, ready to DL")

    async def init_callback(self, url_key):
        # worker que lanza la creación de los objetos VideoDownloaders,
        # uno por video
        async with self.alock:
            _pending = self.num_videos_pending
            _to_check = self.num_videos_to_check

        vid = self.info_videos[url_key]["video_info"]
        _pre = f"[init_callback]:[{url_key}][{_pending}/{_to_check}]:"

        logger.debug(f"{_pre} extracting info")

        try:
            if vid.get("_type", "video") != "video":

                try:
                    _ext_info = try_get(vid.get("original_url"), lambda x: {"original_url": x}) or {}
                    logger.debug(f"{_pre} extra_info={_ext_info or vid}")
                    _res = await self.ytdl.async_extract_info(
                        vid["url"], download=False, extra_info=_ext_info)
                    if not _res:
                        raise Exception("no info video")
                    info = self.ytdl.sanitize_info(_res)
                    if not info.get("release_timestamp") and (_mtime := vid.get("release_timestamp")):

                        info["release_timestamp"] = _mtime
                        info["release_date"] = vid.get("release_date")

                    self.info_videos[url_key]["video_info"] = info

                    logger.debug(f"{_pre} info extracted\n{_for_print(info)}")

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

                    logger.error(f"{_pre} init nok - {_error}")

                    return

            else:
                info = vid

            if (_type := info.get("_type", "video")) == "video":

                if not self.STOP.is_set():
                    await self.get_dl(url_key)

            elif _type == "playlist":

                logger.warning(f"{_pre} playlist en worker_init")

                self.info_videos[url_key]["todl"] = False

                for _entry in info["entries"]:

                    if (_type := _entry.get("_type", "video")) != "video":
                        logger.warning(f"{_pre} playlist of entries that are not videos")
                        continue
                    else:
                        _url = _entry.get("original_url") or _entry.get("url")
                        try:
                            if not self.info_videos.get(_url):
                                # es decir, los nuevos videos
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

                                if (_same_video_url := await self.async_check_if_same_video(_url)):

                                    self.info_videos[_url].update({"samevideo": _same_video_url})
                                    logger.warning(
                                        f"{_pre}[{_url}]: has not been added to" +
                                        f"video list because it gets same video than {_same_video_url}")

                                    await self._prepare_for_dl(_url, put=False)

                                else:
                                    try:
                                        await self._prepare_for_dl(_url, put=False)
                                        if not self.STOP.is_set():
                                            await self.get_dl(_url)
                                    except Exception:
                                        raise

                        except Exception as e:
                            self.list_initnok.append((_entry, f"Error:{repr(e)}"))
                            logger.error(f"{_pre}[{_url}] init nok - Error:{repr(e)}")
                            self.list_urls_to_check.append((_url, repr(e)))
                            self.info_videos[_url]["error"].append(f"DL constructor error:{repr(e)}")
                            self.info_videos[_url]["status"] = "initnok"

        except Exception as e:
            self.list_initnok.append((vid, f"Error:{repr(e)}"))
            logger.error(f"{_pre} init nok - Error:{repr(e)}")
            self.list_urls_to_check.append((url_key, repr(e)))
            self.info_videos[url_key]["error"].append(f"DL constructor error:{repr(e)}")
            self.info_videos[url_key]["status"] = "initnok"
        finally:
            async with self.alock:
                self.num_videos_pending -= 1

    async def run_callback(self, dl, url_key):

        try:

            self.list_pasres.discard(dl.index)
            if dl.info_dl["status"] == "init_manipulating":

                logger.debug(f"[run_callback] start to manip {dl.info_dl['title']}")
                task_run_manip = add_task(dl.run_manip(), self.background_tasks)

                done, _ = await asyncio.wait([task_run_manip])

                for d in done:
                    try:
                        d.result()
                    except Exception as e:
                        logger.exception(
                            f"[run_callback] [{dl.info_dict['title']}]: " +
                            f"Error with video manipulation - {repr(e)}")

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
                    f"[run_callback][{url_key}]: error when dl video, can't go" +
                    f"por manipulation - {dl.info_dl.get('error_message')}")

                self.info_videos[url_key]["error"].append(
                    f"error when dl video: {dl.info_dl.get('error_message')}")
                self.info_videos[url_key]["status"] = "nok"

            else:

                logger.error(
                    f"[run_callback][{url_key}]: STATUS NOT EXPECTED: " +
                    f"{dl.info_dl['status']}")

                self.info_videos[url_key]["error"].append(
                    f"error when dl video: {dl.info_dl.get('error_message')}")
                self.info_videos[url_key]["status"] = "nok"

        except Exception as e:
            logger.exception(f"[run_callback][{url_key}]: error {repr(e)}")

    async def async_ex(self):

        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            asyncio.get_running_loop().add_signal_handler(
                s, lambda s=s: asyncio.create_task(self.shutdown(s)))

        try:

            self.STOP = MySyncAsyncEvent("MAINSTOP")
            self.getlistvid_first = MySyncAsyncEvent("first")
            self.end_dl = MySyncAsyncEvent("enddl")
            self.alock = asyncio.Lock()
            self.hosts_alock = asyncio.Lock()

            self.t1.start()
            self.t2.start()
            self.t3.start()

            self.WorkersInit = WorkersInit(self)
            self.WorkersRun = WorkersRun(self)

            await self.localstorage.aready()

            if not self.args.nodl:
                self.nwsetup = NWSetUp(self)
                self.is_ready_to_dl = self.nwsetup.init_ready

            tasks_to_wait = {}

            tasks_to_wait.update(
                {add_task(self.get_list_videos(), self.background_tasks):
                 "task_get_videos"})

            if not self.args.nodl:

                _res = await async_waitfortasks(
                    events=(self.getlistvid_first, self.end_dl, self.STOP),
                    background_tasks=self.background_tasks)

                if _res.get("event") == "first":
                    self.FEgui = FrontEndGUI(self)

                    tasks_to_wait.update({add_task(self.end_dl.async_wait(), self.background_tasks): "task_workers_run"})

            if tasks_to_wait:
                await asyncio.wait(tasks_to_wait)

        except BaseException as e:
            logger.error(f"[async_ex] {repr(e)}")
            raise
        finally:
            _task = [asyncio.create_task(self.shutdown())]
            await asyncio.wait(_task)
            self.get_results_info()
            logger.info("[async_ex] BYE")

    async def shutdown(self, signal=None):

        try:

            logger.info(f'[shutdown] signal {signal}')

            self.print_pending_tasks()

            if not self.STOP.is_set():
                self.t2.stop()
                if hasattr(self, 'FEgui'):
                    logger.info(f"[shutdown] {self.FEgui.get_dl_media()}")
                self.STOP.set()
                await self.ytdl.stop()
                await asyncio.sleep(0)
                if self.list_dl:
                    for _, dl in self.list_dl.items():
                        await dl.stop('exit')
                        await asyncio.sleep(0)

            await self.close()
            await asyncio.sleep(0)
            self.print_pending_tasks()
            _pending_tasks = [
                task for task in asyncio.all_tasks() if
                task is not asyncio.current_task() and 'async_ex' not in repr(task.get_coro())]
            if _pending_tasks:
                list(map(lambda task: task.cancel(), _pending_tasks))
                await asyncio.wait(_pending_tasks)

        except Exception as e:
            logger.exception(f'[shutdown] {repr(e)}')

    async def close(self):

        try:
            logger.debug("[close] start to close")

            try:
                logger.debug("[close] start to close countdowns")
                from asynchlsdownloader import AsyncHLSDownloader
                if AsyncHLSDownloader._COUNTDOWNS:
                    AsyncHLSDownloader._COUNTDOWNS.clean()
            except Exception as e:
                logger.exception(f"[close] asyncdlhls countdown {repr(e)}")

            try:
                if hasattr(self, 'FEgui'):
                    logger.debug("[close] start to close fegui")
                    await self.FEgui.close()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")

            try:
                if hasattr(self, 'nwsetup'):
                    logger.debug("[close] start to close nw")
                    if not self.nwsetup.init_ready.is_set():
                        await self.nwsetup.init_ready.async_wait()
                    await self.nwsetup.close()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")

            try:
                self.ytdl.close()
            except BaseException as e:
                logger.exception(f"[close] {repr(e)}")

            if self.list_dl:
                for _, vdl in self.list_dl.items():
                    try:
                        vdl.close()
                    except Exception as e:
                        logger.exception(f"[close] {repr(e)}")

            # waits for upt local
            await self.localstorage.aready()

        except BaseException as e:
            logger.error(f"[close] error {str(e)}. Lets kill processes")
            kill_processes(logger=logger, rpcport=self.args.rpcport)

    def get_results_info(self):
        _DOMAINS_CONF_PRINT = ['nakedsword.com', 'onlyfans.com']

        def _getter(url: str, vid: dict) -> str:
            webpageurl = traverse_obj(
                vid, ("video_info", "webpage_url"))
            originalurl = traverse_obj(
                vid, ("video_info", "original_url"))
            playlisturl = traverse_obj(vid, ("video_info", "playlist_url"))

            assert (isinstance(originalurl, (str, type(None))) and isinstance(webpageurl, (str, type(None)))
                    and isinstance(playlisturl, (str, type(None))))

            if webpageurl and any([_ in webpageurl for _ in _DOMAINS_CONF_PRINT]):
                return (webpageurl or originalurl or url)
            else:
                return (originalurl or webpageurl or url)

        def _print_list_videos():
            try:

                col = shutil.get_terminal_size().columns

                list_videos = [
                    _getter(url, vid)
                    for url, vid in self.info_videos.items()
                    if vid.get("todl")
                ]

                if list_videos:
                    list_videos_str = [[fill(text=url, width=col // 2)]
                                       for url in list_videos]
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
                                none_to_zero(vid["video_info"].get(
                                    "filesize", 0))
                            ),
                            fill(_getter(url, vid), col // 2),
                            vid.get("status")
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
                            # fill(vid["aldl"], col // 3),
                            vid["aldl"]
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
                    f"Total videos [{len(list_videos)}]\nTo DL " +
                    f"[{(_tv2dl := len(list_videos2dl))}]\nAlready DL [{len(list_videosaldl)}]\n" +
                    f"Same requests [{len(list_videossamevideo)}]")

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

                _columns = ["ID", "Title", "Size", "URL", "Status"]
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
                    "videos": {"urls": list_videos,
                               "str": list_videos_str},
                    "videos2dl": {"urls": list_videos2dl,
                                  "str": list_videos2dl_str},
                    "videosaldl": {"urls": list_videosaldl,
                                   "str": list_videosaldl_str},
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
            else [])

        _videos_url_tocheck_str = (
            [f"{_url}:{_error}" for _url, _error in self.list_urls_to_check]
            if self.list_urls_to_check
            else [])

        videos_okdl = []
        videos_kodl = []
        videos_koinit = []

        for url, video in self.info_videos.items():
            if (
                not video.get("aldl")
                and not video.get("samevideo")
                and video.get("todl")
            ):
                if video["status"] == "done":
                    videos_okdl.append(_getter(url, video))

                else:
                    if ((video["status"] == "initnok") or
                            (video["status"] == "prenok")):
                        videos_kodl.append(_getter(url, video))
                        videos_koinit.append(_getter(url, video))
                    elif video["status"] == "initok":
                        if self.args.nodl:
                            videos_okdl.append(_getter(url, video))
                    else:
                        videos_kodl.append(_getter(url, video))

        if videos_okdl:
            self.localstorage.upt_local()

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

            logger.info("****************************************************")
            logger.info("****************************************************")
            logger.info("*********** FINAL SUMMARY **************************")
            logger.info("****************************************************")
            logger.info("****************************************************\n\n")
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
            logger.info("*********** VIDEO RESULT LISTS **********************\n\n")
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

            if videos_kodl:
                logger.info("Videos TOTAL ERROR DL:")
                _videos_kodl_uniq_url = cast(
                    list, list(dict.fromkeys(list(map(lambda x: re.sub(r'#\d+$', '', x), videos_kodl)))))
                logger.info(
                    f"%no%\n\n{videos_kodl} \n[{_path_str}-u {' -u '.join(_videos_kodl_uniq_url)}")
            else:
                logger.info("Videos TOTAL ERROR DL: []")
            if videos_koinit:
                _videos_koinit_uniq_url = cast(
                    list, list(dict.fromkeys(list(map(lambda x: re.sub(r'#\d+$', '', x), videos_koinit)))))
                logger.info("Videos ERROR INIT DL:")
                logger.info(
                    f"%no%\n\n{videos_koinit} \n[{_path_str}-u {' -u '.join(_videos_koinit_uniq_url)}]")
            if _videos_url_notsupported:
                logger.info("Unsupported URLS:")
                logger.info(f"%no%\n\n{_videos_url_notsupported}")
            if _videos_url_notvalid:
                logger.info("Not Valid URLS:")
                logger.info(f"%no%\n\n{_videos_url_notvalid}")
            if _videos_url_tocheck:
                logger.info("To check URLS:")
                logger.info(f"%no%\n\n{_videos_url_tocheck}")
            logger.info("****************************************************")
            logger.info("****************************************************")
            logger.info("****************************************************")
            logger.info("****************************************************")
        except Exception as e:
            logger.exception(f"[get_results] {repr(e)}")

        logger.debug(f"[info_videos]\n{_for_print_videos(self.info_videos)}")

        return info_dict
