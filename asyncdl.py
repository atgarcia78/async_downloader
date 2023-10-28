import asyncio
import json
import logging
import os as syncos
import re
import shutil
import signal
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from threading import Lock

from codetiming import Timer

from utils import (
    CONF_PLAYLIST_INTERL_URLS,
    MAXLEN_TITLE,
    PATH_LOGS,
    Coroutine,
    FrontEndGUI,
    LocalVideos,
    MySyncAsyncEvent,
    NWSetUp,
    Union,
    _for_print,
    _for_print_videos,
    async_waitfortasks,
    cast,
    get_list_interl,
    init_ytdl,
    js_to_json,
    kill_processes,
    mylogger,
    naturalsize,
    none_to_zero,
    print_tasks,
    render_res_table,
    sanitize_filename,
    sync_to_async,
    traverse_obj,
    try_get,
)
from videodownloader import VideoDownloader

logger = mylogger(logging.getLogger("asyncDL"))


class WorkersRun:
    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger("WorkersRun")
        self._max = asyncdl.workers
        self.running = deque()
        self.waiting = deque()
        self.tasks = {}
        self.info_dl = {}
        self.alock = asyncio.Lock()
        self.exit = MySyncAsyncEvent("workersrunexit")

    @property
    def max(self):
        return self._max

    @max.setter
    def max(self, value):
        self._max = value

    async def add_worker(self):
        async with self.alock:
            self.max += 1
            if self.waiting:
                if len(self.running) < self.max:
                    dl_index = self.waiting.popleft()
                    self.running.append(dl_index)
                    self.tasks.update(
                        {self.asyncdl.add_task(self._task(dl_index)): dl_index})

    async def del_worker(self):
        async with self.alock:
            if self.max > 0:
                self.max -= 1

    async def check_to_stop(self):
        async with self.alock:
            self.logger.debug(
                f"[check_to_stop] running[{len(self.running)}] " +
                f"waiting[{len(self.waiting)}]")
            if not self.waiting and not self.running:
                self.logger.debug("[check_to_stop] set exit")
                self.exit.set()
                self.asyncdl.end_dl.set()

    async def move_to_waiting_top(self, dl_index):
        async with self.alock:
            dl = self.info_dl[dl_index]["dl"]
            dl_status = dl.info_dl["status"]
            if dl_index in self.waiting:
                if self.waiting.index(dl_index) > 0:
                    self.waiting.remove(dl_index)
                    self.waiting.appendleft(dl_index)
                    self.logger.debug(f"[move_to_waiting_top] {list(self.waiting)}")
            elif dl_index not in self.running and dl_status in ("stop", "error"):
                await dl.reinit()
                if len(self.running) >= self.max:
                    self.waiting.append(dl_index)
                else:
                    self.running.append(dl_index)
                    self.tasks.update(
                        {self.asyncdl.add_task(self._task(dl_index)): dl_index})

    async def add_dl(self, dl, url_key):
        _pre = f"[add_dl]:[{dl.info_dict['id']}][{dl.info_dict['title']}][{url_key}]"

        if dl.index in self.info_dl:
            self.logger.warning(f"{_pre} dl with index[{dl.index}] already processed")
            return

        self.info_dl.update({dl.index: {"url": url_key, "dl": dl}})

        async with self.alock:
            self.logger.debug(f"{_pre} running[{len(self.running)}] waiting[{len(self.waiting)}]")
            if len(self.running) >= self.max:
                self.waiting.append(dl.index)
                self.waiting = deque(sorted(self.waiting))
            else:
                self.running.append(dl.index)
                self.tasks.update({self.asyncdl.add_task(self._task(dl.index)): dl.index})
                self.logger.debug(f"[{url_key}] task ok {print_tasks(self.tasks)}")

    async def _task(self, dl_index):
        url_key, dl = self.info_dl[dl_index]["url"], self.info_dl[dl_index]["dl"]
        _pre = f"[_task]:[{dl.info_dict['id']}][{dl.info_dict['title']}][{url_key}]"

        try:
            if dl.info_dl["status"] not in ("init_manipulating", "done"):
                self.logger.debug(f"{_pre} DL init OK, ready to DL")
                if dl.info_dl.get("auto_pasres"):
                    self.asyncdl.list_pasres.add(dl.index)
                    _msg = f", added this dl[{dl.index}] to auto_pasres{list(self.asyncdl.list_pasres)}"
                    self.logger.debug(f"{_pre} pause-resume update{_msg}")

                await dl.run_dl()

            else:
                self.logger.debug(f"{_pre} DL init OK, video parts DL OK")

            await self.asyncdl.run_callback(dl, url_key)

            async with self.alock:
                self.running.remove(dl_index)
                if self.waiting:
                    if len(self.running) < self.max:
                        dl_index2 = self.waiting.popleft()
                        self.running.append(dl_index2)
                        self.tasks.update(
                            {self.asyncdl.add_task(self._task(dl_index2)): dl_index2})

        except Exception as e:
            self.logger.exception(f"{_pre} error {str(e)}")
        finally:
            self.logger.debug(f"{_pre} end task worker run")
            if self.asyncdl.WorkersInit.exit.is_set():
                self.logger.debug(f"{_pre} WorkersInit.exit is set")
                async with self.alock:
                    if not self.waiting and not self.running:
                        self.logger.debug(f"{_pre} no running no waiting, lets set exit")
                        self.exit.set()
                        self.asyncdl.end_dl.set()
                        self.logger.debug("end_dl set")
                    else:
                        self.logger.debug(
                            f"{_pre} there are videos running or waiting, so lets exit")
            else:
                self.logger.debug(f"{_pre} WorkersInit.exit not set")
                async with self.alock:
                    if self.waiting or self.running:
                        self.logger.debug(
                            f"{_pre} there are videos running or waiting, so lets exit")
                        return

                self.logger.debug(
                    f"{_pre} there are no videos running or waiting, " +
                    "so lets wait for WorkersInit.exit")
                await self.asyncdl.WorkersInit.exit.async_wait()
                async with self.alock:
                    if not self.waiting and not self.running:
                        self.logger.debug(
                            f"{_pre} WorkersInit.exit is set after waiting, " +
                            "no running no waiting, lets set exit")
                        self.exit.set()
                        self.asyncdl.end_dl.set()
                        self.logger.info("end_dl set")
                    else:
                        self.logger.debug(
                            f"{_pre} WorkersInit.exit is set after waiting, " +
                            "there are videos running or waiting, so lets exit")


class WorkersInit:
    def __init__(self, asyncdl):
        self.asyncdl = asyncdl
        self.max = self.asyncdl.init_nworkers
        self.running = set()
        self.waiting = deque()
        self.tasks = {}
        self.alock = asyncio.Lock()
        self.exit = MySyncAsyncEvent("workersinitexit")
        self.logger = logging.getLogger("WorkersInit")

    async def add_init(self, url_key):
        _pre = f"[add_init]:[{url_key}]"
        async with self.alock:
            self.logger.debug(f"{_pre} running[{len(self.running)}] waiting[{len(self.waiting)}]")
            if url_key in self.waiting or url_key in list(self.tasks.values()):
                self.logger.warning(f"{_pre} already processed")
                return

            if len(self.running) > self.max:
                self.waiting.append(url_key)
            else:
                self.running.add(url_key)
                self.tasks.update({self.asyncdl.add_task(self._task(url_key)): url_key})
                self.logger.debug(f"[{url_key}] task ok {print_tasks(self.tasks)}")

    async def _task(self, url_key):
        _pre = f"[_task]:[{url_key}]"

        try:
            if url_key == "KILL":
                async with self.alock:
                    self.running.remove(url_key)

                while True:
                    await asyncio.sleep(0)
                    async with self.alock:
                        if not self.waiting and not self.running:
                            break

                self.logger.debug(f"{_pre} end tasks worker init: exit")
                self.asyncdl.t3.stop()
                self.exit.set()
                await self.asyncdl.WorkersRun.check_to_stop()

            else:
                try:
                    await self.asyncdl.init_callback(url_key)
                finally:
                    async with self.alock:
                        self.running.remove(url_key)
                        if self.waiting and (len(self.running) < self.max):
                            url_key2 = self.waiting.popleft()
                            self.running.add(url_key2)
                            self.tasks.update(
                                {self.asyncdl.add_task(self._task(url_key2)): url_key2})

        except Exception as e:
            self.logger.exception(f"{_pre} error {str(e)}")


class AsyncDL:
    def __init__(self, args):
        self.args = args
        logger.quiet = self.args.quiet

        self.background_tasks = set()

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

        self.max_index_playlist = 0

        # contadores sobre número de workers init, workers run y workers manip

        self.totalbytes2dl = 0
        self.launch_time = datetime.now()

        self.ex_winit = ThreadPoolExecutor(thread_name_prefix="ex_wkinit")

        self.sync_to_async = partial(
            sync_to_async, thread_sensitive=False, executor=self.ex_winit)

        self.lock = Lock()

        self.t1 = Timer(
            "execution", text="[timers] Time init workers extract info: {:.2f}", logger=logger.info)
        self.t2 = Timer("execution", text="[timers] Time DL: {:.2f}", logger=logger.info)
        self.t3 = Timer("execution", text="[timers] Time init workers: {:.2f}", logger=logger.info)

        logger.info(f"Hi, lets dl!\n{args}")
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
            logger.debug(f"[pending_all_tasks] {pending_tasks}\n{print_tasks(pending_tasks)}")
        except Exception as e:
            logger.exception(f"[print_pending_tasks]: error: {str(e)}")

    def add_task(self, coro: Union[Coroutine, asyncio.Task], *, name: Union[None, str] = None) -> asyncio.Task:
        if not isinstance(coro, asyncio.Task):
            _task = asyncio.create_task(coro, name=name)
        else:
            _task = coro

        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    async def get_list_videos(self):

        _pre = "[get_list_videos]"
        logger.debug(f"{_pre} start")

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
                            _temp.add(re.sub(r"#\d+$", "", _url))

                _url_list_caplinks = list(_temp)
                logger.info(f"{_pre} video list caplinks:\n{_url_list_caplinks}")
                with open(prevfilecaplinks, "a") as file:
                    _text = "\n".join(_url_list_caplinks)
                    file.write(f"\n\n[{self.launch_time.strftime('%Y-%m-%d %H:%M')}]\n{_text}")

                with open(filecaplinks, "w") as file:
                    file.write("")

                _url_list["caplinks"] = _url_list_caplinks

            if self.args.collection:
                if self.STOP.is_set():
                    raise Exception("STOP")
                _url_list_cli = list(
                    dict.fromkeys(
                        list(map(lambda x: re.sub(r"#\d+$", "", x), self.args.collection))))
                logger.info(f"{_pre} video list cli:\n{_url_list_cli}")
                _url_list["cli"] = _url_list_cli

            if self.args.collection_files:

                def get_info_json(file):
                    try:
                        with open(file, "r") as f:
                            return json.loads(js_to_json(f.read()))
                    except Exception as e:
                        logger.error(f"{_pre} Error:{str(e)}")
                        return {}

                _file_list_videos = []
                for file in self.args.collection_files:
                    info_video = get_info_json(file)
                    if info_video:
                        if info_video.get("_type", "video") != "playlist":
                            _file_list_videos.append(info_video)
                        elif _entries := info_video.get("entries"):
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
                                    "".join([
                                        f"{_url}: has not been added to video list",
                                        f"because it gets same video than {_same_video_url}"
                                    ])
                                )
                                await self._prepare_for_dl(_url)

                            else:
                                await self._prepare_for_dl(_url)
                                self.list_videos.append(self.info_videos[_url]["video_info"])
                        else:
                            logger.warning(f"{_url}: already in info_videos")
                    else:
                        await self._prepare_entry_pl_for_dl(_vid)

            logger.debug(f"{_pre} list videos: \n{_for_print_videos(self.list_videos)}")

            logger.debug(
                "".join([
                    f"{_pre} Initial # urls:\n\tCLI[{len(_url_list_cli )}]\n\t",
                    f"CAP[{len(_url_list_caplinks)}]"]))

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

                                await self.WorkersInit.add_init(_elurl)

                        else:
                            if not self.url_pl_list.get(_elurl):
                                self.url_pl_list[_elurl] = {"source": _source}

                url_list = list(self.info_videos.keys())

                logger.debug(
                    f"{_pre}[url_list] urls not pl [{len(url_list)}]\n{url_list}")

                if self.url_pl_list:
                    logger.debug(
                        "".join([
                            "{_pre}[url_playlist_list] urls that ",
                            f"are pl [{len(self.url_pl_list)}]\n{self.url_pl_list}"
                        ])
                    )
                    self._url_pl_entries = []
                    self._count_pl = 0
                    self.url_pl_list2 = []

                    if self.STOP.is_set():
                        raise Exception("STOP")

                    self.url_pl_queue = asyncio.Queue()

                    for url in self.url_pl_list:
                        self.url_pl_queue.put_nowait(url)

                    tasks_pl_list = []
                    for _ in range(min(self.init_nworkers, len(self.url_pl_list))):
                        self.url_pl_queue.put_nowait("KILL")
                        tasks_pl_list.append(
                            self.add_task(self.process_playlist()))

                    logger.debug(
                        f"{_pre}[url_playlist_list] initial playlists: {len(self.url_pl_list)}")

                    await asyncio.wait(tasks_pl_list)

                    logger.debug(
                        f"{_pre}[url_playlist_list] from initial playlists: {len(self.url_pl_list2)}")

                    if self.STOP.is_set():
                        raise Exception("STOP")

                    if self.url_pl_list2:
                        self.url_pl_queue = asyncio.Queue()
                        for url in self.url_pl_list2:
                            self.url_pl_queue.put_nowait(url)
                        tasks_pl_list2 = []
                        for _ in range(min(self.init_nworkers, len(self.url_pl_list2))):
                            self.url_pl_queue.put_nowait("KILL")
                            tasks_pl_list2.append(
                                self.add_task(self.process_playlist()))

                        await asyncio.wait(tasks_pl_list2)

                    logger.info(f"{_pre} entries from playlists: {len(self._url_pl_entries)}")
                    logger.debug(f"{_pre}\n{_for_print_videos(self._url_pl_entries)}")

        except BaseException as e:
            logger.exception(f"{_pre}: Error {str(e)}")

        finally:
            await self.WorkersInit.add_init("KILL")
            if not self.STOP.is_set():
                self.t1.stop()

    async def process_playlist(self):
        _pre = "[get_list_videos][process_playlist]"
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
                _total_pl = len(self.url_pl_list) + len(self.url_pl_list2)
                logger.info(
                    f"{_pre}[{self._count_pl}/{_total_pl}] processing")

                try:
                    _info = self.ytdl.sanitize_info(
                        await self.ytdl.async_extract_info(_url, download=False))
                    if not _info:
                        raise Exception("no info")
                except Exception as e:
                    logger.warning(f"{_pre} {str(e)}")

                    _info = {"_type": "error", "url": _url, "error": str(e)}
                    await self._prepare_entry_pl_for_dl(_info)
                    self._url_pl_entries += [_info]
                    continue

                if _info.get("_type", "video") != "playlist":
                    if not _info.get("original_url"):
                        _info.update({"original_url": _url})

                    await self._prepare_entry_pl_for_dl(_info)
                    self._url_pl_entries += [_info]

                else:

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

                        if _info.get("extractor_key") in CONF_PLAYLIST_INTERL_URLS:
                            _temp_aldl = []
                            _temp_nodl = []

                            for _ent in _info["entries"]:
                                if not await self.async_check_if_aldl(_ent, test=True):
                                    _temp_nodl.append(_ent)
                                else:
                                    _temp_aldl.append(_ent)

                            _info["entries"] = get_list_interl(_temp_nodl, self, _pre) + _temp_aldl

                    for _ent in _info["entries"]:
                        if self.STOP.is_set():
                            raise Exception("STOP")

                        if _ent.get("_type", "video") == "video" and not _ent.get("error"):
                            if not _ent.get("original_url"):
                                _ent.update({"original_url": _url})

                            if (_ent.get(
                                "extractor_key", _ent.get("ie_key", ""))).lower() == "generic" and (
                                _ent.get("n_entries", 0) <= 1
                            ):
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
                                        "".join([
                                            f"{_pre}[{_ent['playlist_index']}]: nentries > 1, ",
                                            f"webpage_url == original_url: {_wurl}"
                                        ])
                                    )

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
                                logger.error(f"{_pre} {_ent['url']} no video entries - {str(e)}")

            except BaseException as e:
                logger.exception(f"{_pre} {str(e)}")
                if isinstance(e, KeyboardInterrupt):
                    raise

    def _check_if_aldl(self, info_dict, test=False):
        if not (_id := info_dict.get("id")) or not (_title := info_dict.get("title")):
            return False

        _pre = f"[check_if_aldl][{_id}][{_title}]"

        try:
            _title = sanitize_filename(_title[:MAXLEN_TITLE], restricted=True).upper()
            _id = sanitize_filename(_id, restricted=True).replace("_", "").replace("-", "")
            vid_name = f"{_id}_{_title}"

            if not (vid_path_str := self.videos_cached.get(vid_name)):
                if self.args.deep_aldl:
                    if not (vid_path_str := self.videos_cached.get(_id)):
                        return False
                    else:
                        logger.warning(f"{_pre} found with ID already DL")
                else:
                    return False
            else:
                logger.debug(f"{_pre} already DL")  # video en local

            if test:
                return True

            vid_path = Path(vid_path_str)

            if not self.args.nosymlinks:
                if self.args.path:
                    _folderpath = Path(self.args.path)
                else:
                    _folderpath = Path(Path.home(), "testing", self.launch_time.strftime("%Y%m%d"))
                    if self.args.use_path_pl:
                        _pltitle = info_dict.get("playlist") or info_dict.get("playlist_title")
                        _plid = info_dict.get('playlist_id')
                        if _pltitle and _plid:
                            _base = f"{_plid}_{sanitize_filename(_pltitle, restricted=True)}_{info_dict.get('extractor_key')}"
                            _folderpath = Path(Path.home(), "testing", _base)

                _folderpath.mkdir(parents=True, exist_ok=True)
                file_aldl = Path(_folderpath, vid_path.name)
                if file_aldl not in _folderpath.iterdir():
                    file_aldl.symlink_to(vid_path)
                    try:
                        mtime = int(vid_path.stat().st_mtime)
                        syncos.utime(file_aldl, (int(time.time()), mtime), follow_symlinks=False)
                    except Exception as e:
                        logger.debug(
                            "".join([
                                f"{_pre} [{str(file_aldl)}] -> ",
                                f"[{str(vid_path)}] error when copying times {str(e)}"
                            ]))

            return vid_path_str

        except Exception as e:
            logger.warning(f"{_pre} error {str(e)}")

    async def async_check_if_aldl(self, info_dict, test=False):
        return await self.sync_to_async(self._check_if_aldl)(info_dict, test=test)

    def _check_if_same_video(self, url_to_check: str) -> Union[str, None]:
        info = self.info_videos[url_to_check]["video_info"]
        if (
            not info.get("_type", "video") == "video"
            or not (_id := info.get("id"))
            # or not (_title := info.get("title"))
        ):
            return

        for urlkey, _vid in self.info_videos.items():
            if urlkey != url_to_check:
                if (
                    _vid["video_info"].get("_type", "video") == "video"
                    and (_vid["video_info"].get("id", "") == _id)
                    # and (_vid["video_info"].get("title", "")) == _title
                ):
                    return urlkey

    async def async_check_if_same_video(self, url_to_check):
        return await self.sync_to_async(self._check_if_same_video)(url_to_check)

    async def _prepare_for_dl(self, url: str, put: bool = True) -> bool:
        self.info_videos[url].update({"todl": True})

        video_info = self.info_videos[url]["video_info"]

        if _id := video_info.get("id"):
            video_info["id"] = sanitize_filename(
                _id, restricted=True).replace("_", "").replace("-", "")

        if _title := video_info.get("title"):
            video_info["title"] = sanitize_filename(
                _title[:MAXLEN_TITLE], restricted=True)

        if not video_info.get("filesize", None):
            video_info["filesize"] = 0

        if _path := await self.async_check_if_aldl(video_info):
            self.info_videos[url].update({"aldl": _path, "status": "done"})
            logger.debug(
                "".join([
                    "[prepare_for_dl] ",
                    f"[{self.info_videos[url]['video_info'].get('id')}]",
                    f"[{self.info_videos[url]['video_info'].get('title')}] already DL"
                ]))

        if all([self.info_videos[url].get("todl"), not self.info_videos[url].get("aldl"),
                not self.info_videos[url].get("samevideo"), self.info_videos[url].get("status") != "prenok"]):

            async with self.alock:
                self.videos_to_dl.append(url)
                if put:
                    await self.WorkersInit.add_init(url)
                self.num_videos_to_check += 1
                self.num_videos_pending += 1
            return True
        else:
            return False

    async def _prepare_entry_pl_for_dl(self, entry: dict) -> None:

        _errors_ytdl = ["not found", "404", "flagged", "403", "410",
                        "suspended", "unavailable", "disabled"]
        _pre = "[prepare_entry_pl_for_dl]"

        try:
            _type = entry.get("_type", "video")
            if _type == "playlist":
                logger.warning(f"{_pre} PLAYLIST IN PLAYLIST:{entry}")
                return
            elif _type == "error":
                _errorurl = entry.get("url")
                if not _errorurl:
                    return
                _error = str(entry.get("error", "no video entry"))
                if not self.info_videos.get(_errorurl):
                    self.info_videos[_errorurl] = {
                        "source": self.url_pl_list.get(_errorurl, {}).get("source") or "playlist",
                        "video_info": {},
                        "status": "prenok",
                        "todl": True,
                        "error": [_error],
                    }
                    if any(_ in _error.lower() for _ in _errors_ytdl):
                        self.list_notvalid_urls.append(_errorurl)
                    elif "unsupported url" in _error.lower():
                        self.list_unsup_urls.append(_errorurl)
                    else:
                        self.list_urls_to_check.append((_errorurl, _error))
                    self.list_initnok.append((_errorurl, _error))
                else:
                    logger.warning(f"{_pre} {_errorurl}: already in info_videos")
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
                        "".join([
                            f"{_pre} {_url}: has not been added",
                            f" to video list because it gets same video than {_same_video_url}"
                        ])
                    )

                    await self._prepare_for_dl(_url)

                else:
                    await self._prepare_for_dl(_url)
                    self.list_videos.append(self.info_videos[_url]["video_info"])
            else:
                logger.warning(f"{_pre} {_url}: already in info_videos")

        except Exception as e:
            logger.exception(f"{_pre} error {str(e)} with entry\n{entry}")

    async def get_dl(self, url_key):
        if not self.args.nodl:

            async def async_videodl_init(*args, **kwargs) -> VideoDownloader:
                if not self.is_ready_to_dl.is_set():
                    await self.is_ready_to_dl.async_wait()
                return await self.sync_to_async(VideoDownloader)(*args, **kwargs)

            dl = await async_videodl_init(
                self.info_videos[url_key]["video_info"], self.ytdl,
                self.nwsetup, self.args)

            _pre = "".join([
                f"[init_callback][get_dl]:[{dl.info_dict.get('id')}]",
                f"[{dl.info_dict.get('title')}][{url_key}]:"
            ])

            logger.debug(f"{_pre} {dl.info_dl}")

            if dl.info_dl.get("status", "") == "error":
                raise Exception("no DL init")

            if _filesize := dl.info_dl.get("filesize"):
                self.info_videos[url_key]["video_info"]["filesize"] = _filesize
                async with self.alock:
                    self.totalbytes2dl += _filesize

            async with self.alock:
                self.getlistvid_first.set()
                if _index := dl.info_dict.get("__interl_index"):
                    dl.index = _index
                else:
                    _index = max(self.max_index_playlist, max(list(self.list_dl.keys()) or [0]))
                    dl.index = _index + 1

                self.list_dl.update({dl.index: dl})
                self.info_videos[url_key].update(
                    {
                        "status": "initok",
                        "filename": str(dl.info_dl.get("filename")),
                        "dl": str(dl),
                        "dl_index": dl.index,
                    }
                )

                await self.WorkersRun.add_dl(dl, url_key)

    async def init_callback(self, url_key):
        # worker que lanza la creación de los objetos VideoDownloaders,
        # uno por video
        async with self.alock:
            _pending = self.num_videos_pending
            _to_check = self.num_videos_to_check

        vid = self.info_videos[url_key]["video_info"]
        _pre = f"[init_callback]:[{url_key}][{_pending}/{_to_check}]:"

        logger.debug(f"{_pre} extracting info\n{vid}")

        try:
            if vid.get("_type", "video") != "video":
                _check_prepare = False
                try:
                    _ext_info = try_get(vid.get("original_url"), lambda x: {"original_url": x}) or {}
                    logger.debug(f"{_pre} extra_info={_ext_info or vid}")
                    info = self.ytdl.sanitize_info(await self.ytdl.async_extract_info(
                        vid["url"], download=False, extra_info=_ext_info))
                    if not info:
                        raise Exception("no info video")
                    if not info.get("release_timestamp") and (_mtime := vid.get("release_timestamp")):
                        info["release_timestamp"] = _mtime
                        info["release_date"] = vid.get("release_date")

                    self.info_videos[url_key]["video_info"] = info

                    logger.debug(f"{_pre} info extracted\n{_for_print(info)}")

                except Exception as e:
                    _errors_ytdl = [
                        "not found", "404", "flagged", "403", "410",
                        "suspended", "unavailable", "disabled"
                    ]
                    if "unsupported url" in str(e).lower():
                        self.list_unsup_urls.append(url_key)
                        _error = "unsupported_url"
                    elif any(_ in str(e).lower() for _ in _errors_ytdl):
                        _error = "not_valid_url"
                        self.list_notvalid_urls.append(url_key)
                    else:
                        _error = str(e)
                        self.list_urls_to_check.append((url_key, _error))

                    self.list_initnok.append((url_key, _error))
                    self.info_videos[url_key]["error"].append(_error)
                    self.info_videos[url_key]["status"] = "initnok"

                    logger.error(f"{_pre} init nok - {_error}")

                    return

            else:
                info = vid
                _check_prepare = True

            if (_type := info.get("_type", "video")) == "video":
                if not self.STOP.is_set():
                    if _check_prepare or await self._prepare_for_dl(url_key, put=False):
                        await self.get_dl(url_key)
                        self.list_videos.append(self.info_videos[url_key]["video_info"])

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
                                    "ie_key": _entry.get("ie_key") or _entry.get("extractor_key"),
                                    "error": [],
                                }

                                if _same_video_url := await self.async_check_if_same_video(_url):
                                    self.info_videos[_url].update({"samevideo": _same_video_url})
                                    logger.warning(
                                        "".join([
                                            f"{_pre}[{_url}]: has not been added to",
                                            f"video list because it gets same video than {_same_video_url}"
                                        ])
                                    )

                                    await self._prepare_for_dl(_url, put=False)

                                else:
                                    try:
                                        if not self.STOP.is_set():
                                            if await self._prepare_for_dl(_url, put=False):
                                                self.list_videos.append(self.info_videos[_url]["video_info"])
                                                await self.get_dl(_url)
                                    except Exception:
                                        raise

                        except Exception as e:
                            self.list_initnok.append((_entry, f"Error:{str(e)}"))
                            logger.error(f"{_pre}[{_url}] init nok - Error:{str(e)}")
                            self.list_urls_to_check.append((_url, str(e)))
                            self.info_videos[_url]["error"].append(f"DL constructor error:{str(e)}")
                            self.info_videos[_url]["status"] = "initnok"

        except Exception as e:
            self.list_initnok.append((vid, f"Error:{str(e)}"))
            logger.debug(f"{_pre} init nok - Error:{str(e)}")
            self.list_urls_to_check.append((url_key, str(e)))
            self.info_videos[url_key]["error"].append(f"DL constructor error:{str(e)}")
            self.info_videos[url_key]["status"] = "initnok"
        finally:
            async with self.alock:
                self.num_videos_pending -= 1

    async def run_callback(self, dl, url_key):
        try:
            self.list_pasres.discard(dl.index)
            if dl.info_dl["status"] == "init_manipulating":
                logger.debug(f"[run_callback] start to manip {dl.info_dl['title']}")
                task_run_manip = self.add_task(dl.run_manip())

                done, _ = await asyncio.wait([task_run_manip])

                for d in done:
                    try:
                        d.result()
                    except Exception as e:
                        logger.exception(
                            "".join([
                                f"[run_callback] [{dl.info_dict['title']}]: ",
                                f"Error with video manipulation - {str(e)}"
                            ])
                        )

                        self.info_videos[url_key]["error"].append(
                            f"\n error with video manipulation {str(e)}")

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
                    "".join([
                        f"[run_callback][{url_key}]: error when dl video, can't go",
                        f"por manipulation - {dl.info_dl.get('error_message')}"
                    ])
                )

                self.info_videos[url_key]["error"].append(
                    f"error when dl video: {dl.info_dl.get('error_message')}"
                )
                self.info_videos[url_key]["status"] = "nok"

            else:
                logger.error(f"[run_callback][{url_key}]: STATUS NOT EXPECTED: {dl.info_dl['status']}")

                self.info_videos[url_key]["error"].append(
                    f"error when dl video: {dl.info_dl.get('error_message')}"
                )
                self.info_videos[url_key]["status"] = "nok"

        except Exception as e:
            logger.exception(f"[run_callback][{url_key}]: error {str(e)}")

    async def async_ex(self):
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            asyncio.get_running_loop().add_signal_handler(
                s, lambda s=s: asyncio.create_task(self.shutdown(s))
            )

        try:
            self.STOP = MySyncAsyncEvent("MAINSTOP")
            self.getlistvid_first = MySyncAsyncEvent("first")
            self.end_dl = MySyncAsyncEvent("enddl")
            self.alock = asyncio.Lock()

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

            tasks_to_wait.update({self.add_task(self.get_list_videos()): "task_get_videos"})

            if not self.args.nodl:
                _res = await async_waitfortasks(
                    events=(self.getlistvid_first, self.end_dl, self.STOP),
                    background_tasks=self.background_tasks)

                if traverse_obj(_res, ("condition", "event")) == "first":
                    self.FEgui = FrontEndGUI(self)

                    tasks_to_wait.update(
                        {self.add_task(self.end_dl.async_wait()): "task_workers_run"}
                    )

            if tasks_to_wait:
                await asyncio.wait(tasks_to_wait)

        except BaseException as e:
            logger.error(f"[async_ex] {str(e)}")
            raise
        finally:
            await self.shutdown()
            self.get_results_info()
            logger.info("[async_ex] BYE")

    async def shutdown(self, signal=None):
        try:
            logger.info(f"[shutdown] signal {signal}")

            self.print_pending_tasks()

            if not self.STOP.is_set():
                self.t2.stop()
                if hasattr(self, "FEgui"):
                    logger.info(f"[shutdown] {self.FEgui.get_dl_media()}")
                self.STOP.set()
                await self.ytdl.stop()
                await asyncio.sleep(0)
                if self.list_dl:
                    for _, dl in self.list_dl.items():
                        await dl.stop("exit")
                        await asyncio.sleep(0)

            await self.close()
            await asyncio.sleep(0)
            self.print_pending_tasks()
            _pending_tasks = [
                task
                for task in asyncio.all_tasks()
                if task is not asyncio.current_task() and "async_ex" not in repr(task.get_coro())
            ]
            if _pending_tasks:
                list(map(lambda task: task.cancel(), _pending_tasks))
                await asyncio.wait(_pending_tasks)

        except Exception as e:
            logger.exception(f"[shutdown] {str(e)}")

    async def close(self):
        try:
            logger.debug("[close] start to close")

            try:
                logger.debug("[close] start to close countdowns")
                from asynchlsdownloader import AsyncHLSDownloader

                if AsyncHLSDownloader._COUNTDOWNS:
                    AsyncHLSDownloader._COUNTDOWNS.clean()
            except Exception as e:
                logger.exception(f"[close] asyncdlhls countdown {str(e)}")

            try:
                if hasattr(self, "FEgui"):
                    logger.debug("[close] start to close fegui")
                    await self.FEgui.close()
            except BaseException as e:
                logger.exception(f"[close] {str(e)}")

            try:
                if hasattr(self, "nwsetup"):
                    logger.debug("[close] start to close nw")
                    if not self.nwsetup.init_ready.is_set():
                        await self.nwsetup.init_ready.async_wait()
                    await self.nwsetup.close()
            except BaseException as e:
                logger.exception(f"[close] {str(e)}")

            try:
                self.ytdl.close()
            except BaseException as e:
                logger.exception(f"[close] {str(e)}")

            if self.list_dl:
                for _, vdl in self.list_dl.items():
                    try:
                        vdl.close()
                    except Exception as e:
                        logger.exception(f"[close] {str(e)}")

            # waits for upt local
            await self.localstorage.aready()

        except BaseException as e:
            logger.error(f"[close] error {str(e)}. Lets kill processes")
            kill_processes(logger=logger, rpcport=self.args.rpcport)

    def get_results_info(self):
        _DOMAINS_CONF_PRINT = ["nakedsword.com", "onlyfans.com", "pornhub"]
        col = shutil.get_terminal_size().columns

        def _getter(url: str, vid: dict) -> str:
            webpageurl = cast(str, traverse_obj(vid, ("video_info", "webpage_url")))
            originalurl = cast(str, traverse_obj(vid, ("video_info", "original_url")))
            # playlisturl = cast(str, traverse_obj(vid, ("video_info", "playlist_url")))

            if webpageurl and any([_ in webpageurl for _ in _DOMAINS_CONF_PRINT]):
                return webpageurl or originalurl or url
            else:
                return originalurl or webpageurl or url

        def _print_list_videos():
            try:
                list_videos = [_getter(url, vid) for url, vid in self.info_videos.items() if vid.get("todl")]

                list_videos_str = [[url] for url in list_videos] if list_videos else []

                list_videos2dl = [
                    _getter(url, vid) for url, vid in self.info_videos.items()
                    if all([not vid.get("aldl"), not vid.get("samevideo"),
                            vid.get("todl"), vid.get("status") != "prenok"])
                ]

                list_videos2dl_str = (
                    [
                        [
                            vid["video_info"].get("id", ""),
                            vid["video_info"].get("title", ""),
                            naturalsize(none_to_zero(vid["video_info"].get("filesize", 0))),
                            _getter(url, vid),
                            vid.get("status"),
                        ]
                        for url, vid in self.info_videos.items()
                        if all([not vid.get("aldl"), not vid.get("samevideo"),
                                vid.get("todl"), vid.get("status") != "prenok"])
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
                            vid["video_info"].get("id", ""),
                            vid["video_info"].get("title", ""),
                            _getter(url, vid),
                            vid["aldl"],
                        ]
                        for url, vid in self.info_videos.items()
                        if vid.get("aldl") and vid.get("todl")
                    ]
                    if list_videosaldl
                    else []
                )

                list_videossamevideo = [
                    _getter(url, vid) for url, vid in self.info_videos.items() if vid.get("samevideo")
                ]
                list_videossamevideo_str = (
                    [
                        [
                            vid["video_info"].get("id", ""),
                            vid["video_info"].get("title", ""),
                            _getter(url, vid),
                            vid["samevideo"],
                        ]
                        for url, vid in self.info_videos.items()
                        if vid.get("samevideo")
                    ]
                    if list_videossamevideo
                    else []
                )

                logger.info(
                    "".join([
                        f"Total videos [{len(list_videos)}]\nTo DL ",
                        f"[{(_tv2dl := len(list_videos2dl))}]\nAlready DL [{len(list_videosaldl)}]\n",
                        f"Same requests [{len(list_videossamevideo)}]"
                    ])
                )

                logger.info(f"Total bytes to DL: [{naturalsize(self.totalbytes2dl)}]")

                _columns = ["URL"]
                tab_tv = (
                    render_res_table(
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
                    render_res_table(
                        list_videos2dl_str,
                        showindex=True,
                        headers=_columns,
                        tablefmt="simple",
                        maxcolwidths=[None, col // 6, col // 4, None, col // 2, None],
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
                    logger.exception(f"[print_videos] {str(e)}")

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
                logger.exception(str(e))
                return {}

        _videos_url_notsupported = self.list_unsup_urls
        _videos_url_notvalid = self.list_notvalid_urls
        _videos_url_tocheck = (
            [_url for _url, _ in self.list_urls_to_check]
            if self.list_urls_to_check else []
        )

        _videos_url_tocheck_str = (
            [f"{_url}:{_error}" for _url, _error in self.list_urls_to_check]
            if self.list_urls_to_check
            else []
        )

        videos_okdl = []
        videos_kodl = []
        videos_koinit = []

        for url, video in self.info_videos.items():
            if not video.get("aldl") and not video.get("samevideo") and video.get("todl"):
                if video["status"] == "done":
                    videos_okdl.append(_getter(url, video))
                else:
                    if (video["status"] == "initnok") or (video["status"] == "prenok"):
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
            render_res_table(
                info_dict["videosaldl"]["str"],
                showindex=True,
                headers=_columnsaldl,
                tablefmt="simple",
                maxcolwidths=[None, col // 6, col // 6, col // 3, col // 3],
            )
            if info_dict["videosaldl"]["str"]
            else None
        )
        _columnssamevideo = ["ID", "Title", "URL", "Same URL"]
        tab_vsamevideo = (
            render_res_table(
                info_dict["videossamevideo"]["str"],
                showindex=True,
                headers=_columnssamevideo,
                tablefmt="simple",
                maxcolwidths=[None, col // 6, col // 6, col // 3, col // 3],
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
                    list, list(dict.fromkeys(list(map(lambda x: re.sub(r"#\d+$", "", x), videos_kodl)))))
                logger.info(f"%no%\n\n{videos_kodl}\n[{_path_str}-u {' -u '.join(_videos_kodl_uniq_url)}")
            else:
                logger.info("Videos TOTAL ERROR DL: []")
            if videos_koinit:
                _videos_koinit_uniq_url = cast(
                    list, list(dict.fromkeys(list(map(lambda x: re.sub(r"#\d+$", "", x), videos_koinit)))))
                logger.info("Videos ERROR INIT DL:")
                logger.info(
                    f"%no%\n\n{videos_koinit}\n[{_path_str}-u {' -u '.join(_videos_koinit_uniq_url)}]")
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
            logger.exception(f"[get_results] {str(e)}")

        logger.debug(f"[info_videos]\n{_for_print_videos(self.info_videos)}")

        return info_dict
