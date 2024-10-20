import asyncio
import json
import logging
import os as syncos
import re
import shutil
import signal
import time
from asyncio import Task
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from pathlib import Path

from codetiming import Timer

from utils import (
    MAXLEN_TITLE,
    PATH_LOGS,
    AsyncDLError,
    Coroutine,
    FrontEndGUI,
    LocalVideos,
    MySyncAsyncEvent,
    NWSetUp,
    Optional,
    StatusStop,
    Union,
    _for_print,
    _for_print_videos,
    await_for_any,
    cast,
    init_ytdl,
    js_to_json,
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
from workers import WorkersInit, WorkersRun

logger = mylogger(logging.getLogger("asyncdl"))


@dataclass
class Timers:
    t1 = Timer(
        "execution",
        text="[timers] Time init workers extract info: {:.2f}",
        logger=logger.info,
    )
    t2 = Timer("execution", text="[timers] Time DL: {:.2f}", logger=logger.info)
    t3 = Timer(
        "execution", text="[timers] Time init workers: {:.2f}", logger=logger.info
    )

    def start(self):
        self.t1.start()
        self.t2.start()
        self.t3.start()


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

        self.task_run_manip = {}

        self.num_videos_to_check = 0
        self.num_videos_pending = 0

        self.list_pasres = set()

        self.max_index_playlist = 0

        self.launch_time = datetime.now()

        self.ex_winit = ThreadPoolExecutor(thread_name_prefix="ex_wkinit")

        self.sync_to_async = partial(
            sync_to_async, thread_sensitive=False, executor=self.ex_winit
        )

        self.alock = asyncio.Lock()
        self.STOP = MySyncAsyncEvent("MAINSTOP")
        self.in_shutdown = MySyncAsyncEvent("SHUTDOWN")
        self.getlistvid_first = MySyncAsyncEvent("first")
        self.end_dl = MySyncAsyncEvent("enddl")

        self.timers = Timers()

        logger.info(f"Hi, lets dl!\n{args}")
        self.localstorage = LocalVideos(self)

    async def cancel_all_dl(self):
        self.STOP.set()
        await asyncio.sleep(0)
        await self.ytdl.stop()
        if not self.end_dl.is_set():
            self.WorkersRun.max_workers = 0
            self.WorkersRun.waiting.clear()
            if self.list_dl:
                for _, dl in self.list_dl.items():
                    await dl.stop("exit")
                    await asyncio.sleep(0)
                await asyncio.sleep(2)
            await self.WorkersRun.check_to_stop()

    def print_pending_tasks(self):
        try:
            pending_tasks = asyncio.all_tasks()
            logger.debug(
                f"[pending_all_tasks] {pending_tasks}\n{print_tasks(pending_tasks)}"
            )
        except Exception as e:
            logger.error(f"[print_pending_tasks]: error: {repr(e)}")

    def add_task(self, coro: Union[Coroutine, Task], *, name: Optional[str] = None) -> Task:
        if not isinstance(coro, Task):
            _task = asyncio.create_task(coro, name=name)
        else:
            _task = coro
        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    def build_info_video(self, source, vinfo, **kwargs):
        
        error = kwargs.pop('error', None)

        return {
            "source": source,
            "video_info": vinfo,
            "status": "prenok" if error else "init",
            "aldl": False,
            "todl": True,
            "error": [error] if error else [],
            **kwargs
        }

    async def get_list_videos(self):
        _pre = "[get_list_videos]"
        logger.debug(f"{_pre} start")

        async def get_info_files():
            try:
                def get_info_json(file):
                    try:
                        with open(file, "r") as f:
                            return json.loads(js_to_json(f.read()))
                    except Exception as e:
                        logger.error(f"{_pre} Error:{repr(e)}")
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
                        if _url not in self.info_videos:
                            self.info_videos[_url] = self.build_info_video(
                                "file_cli", _vid)
                            if _same_video_url := await self.async_check_if_same_video(_url):
                                self.info_videos[_url].update(
                                    {"samevideo": _same_video_url})
                                logger.warning(
                                    f"{_pre} {_url}: entry same video {_same_video_url}")
                                await self._prepare_for_dl(_url)
                            else:
                                await self._prepare_for_dl(_url)
                                self.list_videos.append(
                                    self.info_videos[_url]["video_info"])
                        else:
                            logger.warning(
                                f"{_pre} {_url}: already in info_videos:"
                                + f"\n{self.info_videos[_url]}")
                    else:
                        await self._prepare_entry_pl_for_dl(_vid)
            except Exception as e:
                logger.exception(f"{_pre}: Error {repr(e)}")

        try:
            _url_list_caplinks = []
            _url_list_cli = []
            self.url_pl_list = {}
            _url_list = {}

            filecaplinks = Path(PATH_LOGS, "captured_links.txt")
            prevfilecaplinks = Path(PATH_LOGS, "prev_captured_links.txt")

            if self.args.caplinks and filecaplinks.exists():
                _temp = set()
                with open(filecaplinks, "r") as file:
                    for _url in file:
                        if _url := _url.strip():
                            _temp.add(re.sub(r"#\d+$", "", _url))
                _url_list_caplinks = list(_temp)
                logger.info(f"{_pre} video list caplinks:\n{_url_list_caplinks}")
                with open(prevfilecaplinks, "a") as file:
                    _text = "\n".join(_url_list_caplinks)
                    file.write(
                        f"\n\n[{self.launch_time.strftime('%Y-%m-%d %H:%M')}]\n{_text}")
                with open(filecaplinks, "w") as file:
                    file.write("")
                _url_list["caplinks"] = _url_list_caplinks

            if self.args.collection:
                if self.STOP.is_set():
                    raise StatusStop()
                _url_list_cli = list(
                    dict.fromkeys(
                        list(
                            map(lambda x: re.sub(r"#\d+$", "", x), self.args.collection)
                        )
                    )
                )
                logger.info(f"{_pre} video list cli:\n{_url_list_cli}")
                _url_list["cli"] = _url_list_cli

            if self.args.collection_files:
                await get_info_files()

            logger.debug(f"{_pre} list videos: \n{_for_print_videos(self.list_videos)}")
            logger.debug(
                f"{_pre} Initial # urls:\n\tCLI[{len(_url_list_cli )}]\n\t"
                + f"CAP[{len(_url_list_caplinks)}]")

            if _url_list:
                for _source, _ulist in _url_list.items():
                    for _elurl in _ulist:
                        if self.STOP.is_set():
                            raise StatusStop()

                        is_pl, ie_key = self.ytdl.is_playlist(_elurl)
                        if not is_pl:
                            _entry = {
                                "_type": "url",
                                "url": _elurl,
                                "extractor_key": ie_key}
                            if not self.info_videos.get(_elurl):
                                self.info_videos[_elurl] = self.build_info_video(
                                    _source, _entry, **{"extractor_key": ie_key})
                                await self.WorkersInit.add_init(_elurl)
                            else:
                                logger.warning(
                                    f"{_pre} {_url}: already in info_videos:\n{self.info_videos[_url]}")
                        else:
                            if not self.url_pl_list.get(_elurl):
                                self.url_pl_list[_elurl] = {"source": _source}

                logger.debug(
                    f"{_pre}[url_list] urls not pl [{len(self.info_videos)}]\n"
                    + f"{list(self.info_videos.keys())}")

                if self.url_pl_list:
                    logger.debug(
                        f"{_pre}[url_playlist_list] urls that are pl "
                        + f"[{len(self.url_pl_list)}]\n{self.url_pl_list}]"
                    )
                    self._url_pl_entries = []
                    self._count_pl = 0
                    self.url_pl_list2 = []
                    self.url_pl_queue = asyncio.Queue()

                    for url in self.url_pl_list:
                        self.url_pl_queue.put_nowait(url)

                    tasks_pl_list = []
                    _workers_pl_list = min(self.init_nworkers, len(self.url_pl_list))
                    for _ in range(_workers_pl_list):
                        self.url_pl_queue.put_nowait("KILL")
                    for _ in range(_workers_pl_list):
                        tasks_pl_list.append(self.add_task(self.process_playlist()))

                    await asyncio.wait(tasks_pl_list)

                    logger.debug(
                        f"{_pre}[url_playlist_list] from initial playlists: "
                        f"{len(self.url_pl_list2)}")

                    if self.STOP.is_set():
                        raise StatusStop()

                    if self.url_pl_list2:
                        self.url_pl_queue = asyncio.Queue()
                        for url in self.url_pl_list2:
                            self.url_pl_queue.put_nowait(url)
                        tasks_pl_list2 = []
                        _workers_pl_list2 = min(
                            self.init_nworkers, len(self.url_pl_list2)
                        )
                        for _ in range(_workers_pl_list2):
                            self.url_pl_queue.put_nowait("KILL")
                        for _ in range(_workers_pl_list2):
                            tasks_pl_list2.append(
                                self.add_task(self.process_playlist())
                            )
                        await asyncio.wait(tasks_pl_list2)

                    logger.info(
                        f"{_pre} entries from playlists: "
                        + f"{len(self._url_pl_entries)}")
                    logger.debug(f"{_pre}\n{_for_print_videos(self._url_pl_entries)}")

        except Exception as e:
            logger.exception(f"{_pre}: Error {repr(e)}")
        finally:
            await self.WorkersInit.add_init("KILL")
            if not self.STOP.is_set():
                self.timers.t1.stop()

    async def process_playlist(self):
        _pre = "[get_list_videos][process_playlist]"

        async def _get_info(_url):
            try:
                if not (
                    _info := self.ytdl.sanitize_info(
                        await self.ytdl.async_extract_info(_url, download=False))
                ):
                    raise AsyncDLError(f"{_pre} no info")
            except Exception as e:
                logger.exception(f"{_pre} {repr(e)}")
                _info = {"_type": "error", "url": _url, "error": repr(e)}
            return _info

        try:
            while True:
                try:
                    while True:
                        try:
                            if (
                                _url := self.url_pl_queue.get_nowait()
                            ) == "KILL":
                                return
                            async with self.alock:
                                self._count_pl += 1
                            break
                        except asyncio.QueueEmpty:
                            await asyncio.sleep(0)
                        finally:
                            if self.STOP.is_set():
                                raise StatusStop()

                    _pre = f"[get_list_videos][process_playlist][{_url}]"
                    _total_pl = len(self.url_pl_list) + len(self.url_pl_list2)
                    logger.info(f"{_pre}[{self._count_pl}/{_total_pl}] processing")

                    _info = await _get_info(_url)

                    if _info.get("_type", "video") != "playlist":
                        if not _info.get("original_url"):
                            _info |= {"original_url": _url}
                        if _info.get("error"):
                            _info["_type"] = "error"
                        await self._prepare_entry_pl_for_dl(_info)
                        async with self.alock:
                            self._url_pl_entries += [_info]
                    else:
                        _info_pl = self.get_info_pl(_info)
                        _entries_ok = []
                        for _ent in _info["entries"]:
                            if self.STOP.is_set():
                                raise StatusStop()
                            if not _ent.get("original_url"):
                                _ent.update({"original_url": _url})
                            if _ent.get("error") or (_ent.get("_type", "video") == "video" and not _ent.get("format")):
                                _ent["_type"] = "error"
                                await self._prepare_entry_pl_for_dl(_ent, **_info_pl)
                                async with self.alock:
                                    self._url_pl_entries.append(_ent)
                            else:
                                _entries_ok.append(_ent)

                        _info["entries"] = _entries_ok

                        for _ent in _info["entries"]:
                            if self.STOP.is_set():
                                raise StatusStop()
                            if _ent.get("_type", "video") == "video":
                                if (
                                    _ent.get("extractor_key", "").lower() == "generic"
                                    and _ent.get("n_entries", 0) <= 1
                                ):
                                    _ent.pop("playlist", None)
                                    _ent.pop("playlist_index", None)
                                    _ent.pop("n_entries", None)
                                    _ent.pop("playlist", None)
                                    _ent.pop("playlist_id", None)
                                    _ent.pop("playlist_title", None)

                                if (_wurl := _ent["webpage_url"]) == _ent[
                                    "original_url"
                                ]:
                                    if _ent.get("n_entries", 0) > 1:
                                        _ent.update(
                                            {
                                                "webpage_url": f"{_wurl}?index={_ent['playlist_index']}"
                                            }
                                        )
                                        logger.warning(
                                            f"{_pre}[{_ent['playlist_index']}]: nentries > 1, "
                                            + f"webpage_url == original_url: {_wurl}"
                                        )

                                await self._prepare_entry_pl_for_dl(_ent, **_info_pl)
                                async with self.alock:
                                    self._url_pl_entries += [_ent]

                            else:
                                try:
                                    if not try_get(
                                        self.ytdl.is_playlist(_ent.get("url")),
                                        lambda x: x[0],
                                    ):
                                        await self._prepare_entry_pl_for_dl(_ent, **_info_pl)
                                        async with self.alock:
                                            self._url_pl_entries.append(_ent)
                                    else:
                                        async with self.alock:
                                            self.url_pl_list2.append(_ent["url"])
                                except Exception as e:
                                    logger.error(
                                        f"{_pre} {_ent['url']} no video entries - {repr(e)}"
                                    )

                except Exception as e:
                    logger.exception(f"{_pre} {repr(e)}")

        except Exception as e:
            logger.error(f"{_pre} outer exception {repr(e)}")
        finally:
            logger.debug(f"{_pre} bye worker")

    def _check_if_aldl(self, info_dict, test=False):
        _id, _title, _legacy_title = self.get_info(info_dict)
        if not _id or not _title:
            return False
        _pre = f"[check_if_aldl][{_id}][{_title}]"

        try:
            vid_name = f"{_id}_{_title.upper()}"
            if vid_path_str := self.videos_cached.get(vid_name):
                logger.info(f"{_pre} already DL")  # video en local
            elif _legacy_title and (
                vid_path_str := self.videos_cached.get(
                    (vid_name := f"{_id}_{_legacy_title.upper()}")
                )
            ):
                _pre += f"[{_legacy_title}]"
                logger.info(f"{_pre} already DL with legacy title")  # video en local
            elif self.args.deep_aldl:
                if not (vid_path_str := self.videos_cached.get(_id)):
                    return False
                else:
                    logger.warning(f"{_pre} found with ID already DL")
            else:
                return False

            if test:
                return True

            if not self.args.nosymlinks:
                vid_path = Path(vid_path_str)

                if self.args.path:
                    _folderpath = Path(self.args.path)
                else:
                    _folderpath = Path(
                        Path.home(), "testing", self.launch_time.strftime("%Y%m%d")
                    )
                    if self.args.use_path_pl:
                        _pltitle = info_dict.get("playlist") or info_dict.get(
                            "playlist_title"
                        )
                        _plid = info_dict.get("playlist_id")
                        if _pltitle and _plid:
                            _temp = sanitize_filename(_pltitle, restricted=True)
                            _base = f"{_plid}_{_temp}"
                            _folderpath = Path(Path.home(), "testing", _base)

                _folderpath.mkdir(parents=True, exist_ok=True)
                file_aldl = Path(_folderpath, vid_path.name)
                if file_aldl not in _folderpath.iterdir():
                    file_aldl.symlink_to(vid_path)
                    try:
                        mtime = int(vid_path.stat().st_mtime)
                        syncos.utime(
                            file_aldl, (int(time.time()), mtime), follow_symlinks=False
                        )
                    except Exception as e:
                        logger.warning(
                            f"{_pre} [{str(file_aldl)}] -> "
                            + f"[{str(vid_path)}] error when copying times {repr(e)}"
                        )

            return vid_path_str

        except Exception as e:
            logger.warning(f"{_pre} error {repr(e)}")

    async def async_check_if_aldl(self, info_dict, test=False):
        return await self.sync_to_async(self._check_if_aldl)(info_dict, test=test)

    def _check_if_same_video(self, url_to_check: str) -> Optional[str]:
        info = self.info_videos[url_to_check]["video_info"]
        if info.get("_type", "video") != "video" or not (_id := info.get("id")):
            return

        for urlkey, _vid in self.info_videos.items():
            if (
                urlkey != url_to_check
                and _vid["video_info"].get("_type", "video") == "video"
                and (_vid["video_info"].get("id", "") == _id)
            ):
                return urlkey

    async def async_check_if_same_video(self, url_to_check):
        return await self.sync_to_async(self._check_if_same_video)(url_to_check)

    def get_info(self, video_info):
        if _id := video_info.get("id"):
            _id = video_info["id"] = (
                sanitize_filename(_id, restricted=True)
                .replace("_", "")
                .replace("-", "")
            )

        if _title := video_info.get("title"):
            _title = video_info["title"] = sanitize_filename(
                _title[:MAXLEN_TITLE], restricted=True
            )

        if _legacy_title := video_info.get("_legacy_title"):
            _legacy_title = video_info["_legacy_title"] = sanitize_filename(
                _legacy_title[:MAXLEN_TITLE], restricted=True
            )

        return (_id, _title, _legacy_title)
    
    def get_info_pl(self, video_info):
        _path_pl = None
        _url_pl = None
        if (_type := video_info.get('_type', 'video')) == 'playlist':
            _ptitle = sanitize_filename(video_info.get('title', ''), restricted=True)
            _pid = video_info.get('id')
            if _ptitle and _pid:
                _path_pl = f'{_pid}_{_ptitle}'
            _url_pl = video_info.get('original_url') or video_info.get('webpage_url')
        elif _type == 'video':
            _ptitle = sanitize_filename(video_info.get('playlist_title', ''), restricted=True)
            _pid = video_info.get('playlist_id')
            if _ptitle and _pid:
                _path_pl = f'{_pid}_{_ptitle}'
            _url_pl = video_info.get('playlist_url') or video_info('original_url')
        return {'path_pl': _path_pl, 'url_pl': _url_pl}

    async def _prepare_for_dl(self, url: str, put: bool = True) -> bool:
        self.info_videos[url].update({"todl": True})
        video_info = self.info_videos[url]["video_info"]

        _id, _title, _ = self.get_info(video_info)

        if not video_info.get("filesize", None):
            video_info["filesize"] = 0

        if _path := await self.async_check_if_aldl(video_info):
            self.info_videos[url].update({"aldl": _path, "status": "done"})
            logger.debug(f"[prepare_for_dl] [{_id}][{_title}] already DL")

        if all(
            [
                self.info_videos[url].get("todl"),
                not self.info_videos[url].get("aldl"),
                not self.info_videos[url].get("samevideo"),
                self.info_videos[url].get("status") != "prenok",
            ]
        ):
            if put:
                await self.WorkersInit.add_init(url)
            async with self.alock:
                self.num_videos_to_check += 1
                self.num_videos_pending += 1
            return True
        else:
            return False

    async def _prepare_entry_pl_for_dl(self, entry: dict, **info_pl) -> None:
        _pre = "[prepare_entry_pl_for_dl]"

        try:
            _type = entry.get("_type", "video")
            if _type == "playlist":
                logger.warning(f"{_pre} PLAYLIST IN PLAYLIST:{entry}")
                return
            elif _type == "error":
                if not (_errorurl := (entry.get("url") or (entry.get("original_url")))):
                    return
                _error = entry.get("error", "no video entry")
                if not self.info_videos.get(_errorurl):
                    self.info_videos[_errorurl] = self.build_info_video(
                        self.url_pl_list.get(
                            _errorurl, {}).get("source") or "playlist", {}, error=_error, **info_pl)
                    self._handle_error(_errorurl, _error)

                else:
                    logger.warning(f"{_pre} {_errorurl}: already in info_videos")
                return
            elif _type == "video":
                _url = entry.get("webpage_url") or entry["url"]
            else:  # url, url_transparent
                _url = entry["url"]

            if not self.info_videos.get(_url):  # es decir, los nuevos videos
                self.info_videos[_url] = self.build_info_video(
                    "playlist", entry, extractor_key=entry.get("extractor_key"), **info_pl)

                if _same_video_url := await self.async_check_if_same_video(_url):
                    self.info_videos[_url] |= {"samevideo": _same_video_url}
                    logger.warning(
                        f"{_pre} {_url}: has not been added"
                        + f" to video list because it gets same video than {_same_video_url}"
                    )

                    await self._prepare_for_dl(_url)

                else:
                    await self._prepare_for_dl(_url)
                    self.list_videos.append(self.info_videos[_url]["video_info"])
            else:
                logger.warning(
                    f"{_pre} {_url}: already in info_videos:\n{self.info_videos[_url]}"
                )

        except Exception as e:
            logger.error(f"{_pre} error {repr(e)} with entry\n{entry}")

    async def get_dl(self, url_key):
        if self.args.nodl:
            return

        async def async_videodl_init(*args, **kwargs) -> VideoDownloader:
            if not self.is_ready_to_dl.is_set():
                await self.is_ready_to_dl.async_wait()
            if not self.STOP.is_set():
                return await self.sync_to_async(VideoDownloader)(*args, **kwargs)

        dl = await async_videodl_init(
            self.info_videos[url_key]["video_info"], self.ytdl, self.nwsetup, self.args
        )

        _pre = (
            f"[init_callback][get_dl]:[{self.info_videos[url_key]['video_info'].get('id')}]"
            + f"[{self.info_videos[url_key]['video_info'].get('title')}][{url_key}]:"
        )

        if not dl or dl.info_dl.get("status", "") == "error":
            raise AsyncDLError(f"{_pre} no DL init")

        if _filesize := dl.info_dl.get("filesize"):
            self.info_videos[url_key]["video_info"]["filesize"] = _filesize

        async with self.alock:
            self.getlistvid_first.set()
            if _index := dl.info_dict.get("__interl_index"):
                dl.index = int(_index)
            else:
                _index = max(
                    self.max_index_playlist, max(list(self.list_dl.keys()) or [0])
                )
                dl.index = _index + 1
            self.list_dl.update({dl.index: dl})
            self.info_videos[url_key] |= {
                "status": "initok",
                "filename": str(dl.info_dl.get("filename")),
                "dl": str(dl),
                "dl_index": dl.index,
            }

        await self.WorkersRun.add_dl(dl, url_key)

    def _handle_error(self, _urlkey, strerr):
        _errors_ytdl = [
            "not found",
            "404",
            "flagged",
            "403",
            "410",
            "suspended",
            "unavailable",
            "disabled",
        ]
        if "unsupported url" in strerr.lower():
            self.list_unsup_urls.append(_urlkey)
            _error = "unsupported_url"
        elif any(_ in strerr.lower() for _ in _errors_ytdl):
            _error = "not_valid_url"
            self.list_notvalid_urls.append(_urlkey)
        else:
            _error = strerr
            self.list_urls_to_check.append((_urlkey, _error))

        self.list_initnok.append((_urlkey, _error))

        if self.info_videos[_urlkey]["status"] != 'prenok':
            self.info_videos[_urlkey]["error"].append(_error)
            self.info_videos[_urlkey]["status"] = "initnok"

        return _error

    async def init_callback(self, url_key):
        _pending = self.num_videos_pending
        _to_check = self.num_videos_to_check
        _pre = f"[init_callback]:[{url_key}][{_pending}/{_to_check}]:"

        vid = self.info_videos[url_key]["video_info"]

        logger.debug(f"{_pre} extracting info\n{vid}")

        try:
            if vid.get("_type", "video") != "video":
                _check_prepare = False
                try:
                    info = self.ytdl.sanitize_info(
                        await self.ytdl.async_extract_info(vid["url"], download=False)
                    )
                    if not info:
                        raise AsyncDLError(f"{_pre} no info video")
                    if not info.get("release_timestamp") and (
                        _mtime := vid.get("release_timestamp")
                    ):
                        info["release_timestamp"] = _mtime
                        info["release_date"] = vid.get("release_date")
                    if _orig_url := vid.get("original_url"):
                        info["original_url"] = _orig_url
                    self.info_videos[url_key]["video_info"] = info
                    logger.debug(f"{_pre} info extracted\n{_for_print(info)}")
                except Exception as e:
                    _error = self._handle_error(url_key, repr(e))
                    logger.exception(f"{_pre} init nok - {_error}")
                    return
            else:
                info = vid
                _check_prepare = True

            if (_type := info.get("_type", "video")) == "video":
                if _check_prepare or await self._prepare_for_dl(url_key, put=False):
                    await self.get_dl(url_key)
                    self.list_videos.append(self.info_videos[url_key]["video_info"])
            elif _type == "playlist":
                logger.warning(f"{_pre} playlist en worker_init")

        except Exception as e:
            self.list_initnok.append((vid, f"Error:{repr(e)}"))
            logger.exception(f"{_pre} init nok - Error:{repr(e)}")
            self.list_urls_to_check.append((url_key, repr(e)))
            self.info_videos[url_key]["error"].append(f"DL constructor error:{repr(e)}")
            self.info_videos[url_key]["status"] = "initnok"
        finally:
            async with self.alock:
                self.num_videos_pending -= 1

    def run_callback(self, dl, url_key):
        try:
            self.list_pasres.discard(dl.index)
            if dl.info_dl["status"] == "init_manipulating":
                if _filesize := dl.info_dl.get("filesize"):
                    self.info_videos[url_key]["video_info"]["filesize"] = _filesize
                logger.debug(f"[run_callback] start to manip {dl.info_dl['title']}")
                self.task_run_manip[self.add_task(dl.run_manip())] = {
                    "url": url_key,
                    "dl": dl}

            elif dl.info_dl["status"] == "stop":
                logger.debug(f"[run_callback][{url_key}]: STOPPED")
                self.info_videos[url_key]["error"].append("dl stopped")
                self.info_videos[url_key]["status"] = "nok"

            elif dl.info_dl["status"] == "error":
                logger.error(
                    f"[run_callback][{url_key}]: error when dl video, can't go"
                    + f"por manipulation - {dl.info_dl.get('error_message')}")
                self.info_videos[url_key]["error"].append(
                    f"error when dl video: {dl.info_dl.get('error_message')}")
                self.info_videos[url_key]["status"] = "nok"

            else:
                logger.error(
                    f"[run_callback][{url_key}]: STATUS NOT EXPECTED: {dl.info_dl['status']}")
                self.info_videos[url_key]["error"].append(
                    f"error when dl video: {dl.info_dl.get('error_message')}")
                self.info_videos[url_key]["status"] = "nok"

        except Exception as e:
            logger.error(f"[run_callback][{url_key}]: error {repr(e)}")
            self.info_videos[url_key]["error"].append(
                f"error in run callback: {repr(e)}")
            self.info_videos[url_key]["status"] = "nok"

    async def async_ex(self):
        for s in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(
                s, lambda s=s: asyncio.create_task(self.shutdown(sig=s)))

        self.timers.start()

        try:
            self.WorkersInit = WorkersInit(self)
            self.WorkersRun = WorkersRun(self)

            if not self.args.nodl:
                self.nwsetup = NWSetUp(self)
                self.is_ready_to_dl = self.nwsetup.init_ready

            await self.localstorage.aready()

            tasks_to_wait = [self.add_task(self.get_list_videos(), name="get_videos")]

            if not self.args.nodl:
                _res = await await_for_any(
                    [self.getlistvid_first, self.end_dl, self.STOP])
                logger.info(f"[async_ex] {_res}\n")
                if self.STOP.is_set():
                    raise StatusStop()
                if "first" in _res.get("event"):
                    self.FEgui = FrontEndGUI(self)
                    tasks_to_wait.append(self.add_task(
                        self.end_dl.async_wait(),
                        name="workers_run_end_dl"))

            if tasks_to_wait:
                await asyncio.wait(tasks_to_wait)
            if self.task_run_manip:
                done, _ = await asyncio.wait(self.task_run_manip)
                for _task in done:
                    url_key, dl = list(self.task_run_manip[_task].values())
                    if e := _task.exception():
                        _msg_error = f"Error with video manipulation - {repr(e)}"
                        logger.error(f"[run_callback] [{dl.info_dict['title']}]: {_msg_error}")
                        self.info_videos[url_key]["error"].append(_msg_error)
                    if dl.info_dl["status"] == "done":
                        self.info_videos[url_key].update({"status": "done"})
                    else:
                        self.info_videos[url_key].update({"status": "nok"})
        except Exception as e:
            logger.error(f"[async_ex] {repr(e)}")
        finally:
            await self.shutdown()
            logger.info("[async_ex] BYE")

    async def shutdown(self, sig=None):
        async with self.alock:
            if self.in_shutdown.is_set():
                return
            self.in_shutdown.set()

        try:
            logger.info(f"[shutdown] signal {sig}")

            if not self.STOP.is_set():
                self.timers.t2.stop()
                await self.cancel_all_dl()
            try:
                await self.close()
            except Exception as e:
                logger.error(f"[shutdown] close - {repr(e)}")

        except Exception as e:
            logger.error(f"[shutdown] {repr(e)}")

    async def close(self):
        logger.info("[close] start to close")

        try:
            _task_to_wait = [
                self.add_task(self.localstorage.aready(), name="localstorage_ready")]
            if hasattr(self, "nwsetup"):
                _task_to_wait.append(
                    self.add_task(self.nwsetup.close(), name="nwsetup_close"))
            if hasattr(self, "FEgui"):
                logger.info(f"[shutdown] {self.FEgui.get_dl_media()}")
                _task_to_wait.append(
                    self.add_task(self.FEgui.close(), name="fegui_close"))

            try:
                logger.debug("[close] start to close ytdl")
                self.ytdl.close()
                logger.debug("[close] close ytdl done")
            except Exception as e:
                logger.error(f"[close] ytdl - {repr(e)}")

            if self.list_dl:
                for _, vdl in self.list_dl.items():
                    try:
                        vdl.close()
                    except Exception as e:
                        logger.error(f"[close] {repr(e)}")

            await asyncio.wait(_task_to_wait)

        except Exception as e:
            logger.exception(f"[close] error {repr(e)}. Lets kill processes")
        finally:
            logger.info("[close] bye")

    def get_results_info(self):
        from collections import defaultdict
        from itertools import chain
        _DOMAINS_CONF_PRINT = [
            "nakedsword.com",
            "onlyfans.com",
            "pornhub",
            "sflix.to",
            "theflixertv.to",
            "newprimewire.li",
            "sketchysex.com",
            "facedownassup.com",
            "fratx.com",
            "eporner.com"
        ]
        col = shutil.get_terminal_size().columns

        def _getter(url: str, vid: dict) -> str:
            webpageurl = cast(str, traverse_obj(vid, ("video_info", "webpage_url")))
            originalurl = cast(str, traverse_obj(vid, ("video_info", "original_url")))
            if webpageurl and any(_ in webpageurl for _ in _DOMAINS_CONF_PRINT):
                return webpageurl
            else:
                return originalurl or webpageurl or url

        def _print_list_videos():
            try:
                list_videos = [
                    _getter(url, vid)
                    for url, vid in self.info_videos.items()
                    if vid.get("todl")]

                list_videos_str = [[url] for url in list_videos] if list_videos else []

                list_videos2dl = [
                    _getter(url, vid)
                    for url, vid in self.info_videos.items()
                    if all(
                        [
                            not vid.get("aldl"),
                            not vid.get("samevideo"),
                            vid.get("todl"),
                            vid.get("status") != "prenok",
                        ]
                    )
                ]

                list_videos2dl_str = (
                    [
                        [
                            vid["video_info"].get("id", ""),
                            vid["video_info"].get("title", ""),
                            naturalsize(
                                none_to_zero(vid["video_info"].get("filesize", 0))
                            ),
                            _getter(url, vid),
                            vid.get("status"),
                        ]
                        for url, vid in self.info_videos.items()
                        if all(
                            [
                                not vid.get("aldl"),
                                not vid.get("samevideo"),
                                vid.get("todl"),
                                vid.get("status") != "prenok",
                            ]
                        )
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
                    _getter(url, vid)
                    for url, vid in self.info_videos.items()
                    if vid.get("samevideo")
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
                    f"Total videos [{len(list_videos)}]\nTo DL "
                    + f"[{(_tv2dl := len(list_videos2dl))}]\nAlready DL [{len(list_videosaldl)}]\n"
                    + f"Same requests [{len(list_videossamevideo)}]"
                )

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
                    logger.error(f"[print_videos] {repr(e)}")

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
                logger.error(repr(e))
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
        videos_kodl = defaultdict(list)
        videos_koinit = defaultdict(list)

        for url, video in self.info_videos.items():
            if (
                not video.get("aldl")
                and not video.get("samevideo")
                and video.get("todl")
            ):
                if video["status"] == "done":
                    videos_okdl.append(_getter(url, video))
                else:
                    if video["status"] in ["initnok", "prenok"]:
                        videos_kodl[video.get('path_pl') or 'no_pl'].append(_getter(url, video))
                        videos_koinit[video.get('path_pl') or 'no_pl'].append(_getter(url, video))
                    elif video["status"] == "initok":
                        if self.args.nodl:
                            videos_okdl.append(_getter(url, video))
                    else:
                        videos_kodl[video.get('path_pl') or 'no_pl'].append(_getter(url, video))

        if videos_okdl:
            self.localstorage.upt_local()

        info_dict = _print_list_videos()

        info_dict.update(
            {
                "videosokdl": {"urls": videos_okdl},
                "videoskodl": {"by_pl": videos_kodl, "urls": list(chain(*list(videos_kodl.values())))},
                "videoskoinit":  {"by_pl": videos_koinit, "urls": list(chain(*list(videos_koinit.values())))},
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

        _path_str = f"--path {self.args.path} " if self.args.path and not self.args.use_path_pl else ""
        try:
            logger.info("****************************************************")
            logger.info("****************************************************")
            logger.info("*********** FINAL SUMMARY **************************")
            logger.info("****************************************************")
            logger.info("****************************************************\n\n")
            logger.info(f"Request to DL: [{len(info_dict['videos']['urls'])}]\n\n")
            logger.info(
                f"         Already DL: [{len(info_dict['videosaldl']['urls'])}]"
            )
            logger.info(
                f"         Same requests: [{len(info_dict['videossamevideo']['urls'])}]"
            )
            logger.info(
                f"         Videos to DL: [{len(info_dict['videos2dl']['urls'])}]\n\n"
            )
            logger.info(f"                 OK DL: [{len(videos_okdl)}]")
            logger.info(f"                 ERROR DL: [{len(info_dict['videoskodl']['urls'])}]")
            logger.info(f"                     ERROR init DL: [{len(info_dict['videoskoinit']['urls'])}]")
            logger.info(
                f"                         UNSUP URLS: [{len(_videos_url_notsupported)}]"
            )
            logger.info(
                f"                         NOTVALID URLS: [{len(_videos_url_notvalid)}]"
            )
            logger.info(
                f"                         TO CHECK URLS: [{len(_videos_url_tocheck)}]\n\n"
            )
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
                if _path_str:
                    _videos_kodl_uniq_url = cast(
                        list,
                        list(
                            dict.fromkeys(
                                list(map(lambda x: re.sub(r"#\d+$", "", x), info_dict['videoskodl']['urls']))
                            )
                        ),
                    )
                    logger.info(
                        f"%no%\n\n{info_dict['videoskodl']['urls']}\n[{_path_str}-u {' -u '.join(_videos_kodl_uniq_url)}"
                    )
                else:
                    for path_pl, videos_urls in info_dict['videoskodl']['by_pl'].items():
                        _videos_pl = list(map(lambda x: re.sub(r"#\d+$", "", x), videos_urls))
                        _path_str = f'--path {path_pl} ' if path_pl != 'no_pl' else ''
                        _header = f':{path_pl} :' if path_pl != 'no_pl' else ''
                        logger.info(
                            f"%no%\n\n{_header}{_videos_pl}\n[{_path_str}-u {' -u '.join(_videos_pl)}"
                        )
            else:
                logger.info("Videos TOTAL ERROR DL: []")

            if videos_koinit:
                logger.info("Videos ERROR INIT DL:")
                if _path_str:
                    _videos_koinit_uniq_url = cast(
                        list,
                        list(
                            dict.fromkeys(
                                list(map(lambda x: re.sub(r"#\d+$", "", x), info_dict['videoskoinit']['urls']))
                            )
                        ),
                    )
                    logger.info(
                        f"%no%\n\n{info_dict['videoskoinit']['urls']}\n[{_path_str}-u {' -u '.join(_videos_koinit_uniq_url)}]"
                    )
                else:
                    for path_pl, videos_urls in info_dict['videoskoinit']['by_pl'].items():
                        _videos_pl = list(map(lambda x: re.sub(r"#\d+$", "", x), videos_urls))
                        _path_str = f'--path {path_pl} ' if path_pl != 'no_pl' else ''
                        _header = f':{path_pl} :' if path_pl != 'no_pl' else ''
                        logger.info(
                            f"%no%\n\n{_header}{_videos_pl}\n[{_path_str}-u {' -u '.join(_videos_pl)}"
                        )

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
            logger.error(f"[get_results] {repr(e)}")

        logger.debug(f"[info_videos]\n{_for_print_videos(self.info_videos)}")

        return info_dict
