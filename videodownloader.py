import asyncio
import logging
import shlex
import shutil
import subprocess
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from queue import Queue
from threading import Lock


import aiofiles.os
import os

import httpx
import m3u8
from yt_dlp.utils import determine_protocol, sanitize_filename

import xattr

from asyncaria2cdownloader import AsyncARIA2CDownloader
from asyncdashdownloader import AsyncDASHDownloader
from asynchlsdownloader import AsyncHLSDownloader
from asynchttpdownloader import AsyncHTTPDownloader
from asyncsaldownloader import AsyncSALDownloader
from asyncnativedownloader import AsyncNativeDownloader

from utils import (
    async_suppress,
    naturalsize,
    prepend_extension,
    sync_to_async,
    traverse_obj,
    try_get,
    variadic,
    Union,
    cast,
    Optional,
    async_waitfortasks,
    MySyncAsyncEvent,
    async_lock,
    CONF_HTTP_DL,
    CONF_DRM
)

from pywidevine.cdm import Cdm
from pywidevine.device import Device
from pywidevine.pssh import PSSH

logger = logging.getLogger("video_DL")


class AsyncDLError:
    def __init__(self, info_dict, msg_error=None):
        self.status = "error"
        self.error_message = msg_error
        self.info_dict = info_dict
        self.filesize = info_dict.get("filesize")
        self.down_size = 0


class VideoDownloader:
    _PLNS = {}
    _QUEUE = Queue()
    _LOCK = Lock()
    _HOSTS_DL = {}
    _CDM = None

    def __init__(self, video_dict, ytdl, nwsetup, args):
        self.background_tasks = set()
        self.hosts_dl = VideoDownloader._HOSTS_DL
        self.master_hosts_alock = partial(async_lock, VideoDownloader._LOCK)
        self.master_hosts_lock = VideoDownloader._LOCK
        self.args = args

        self._index = None  # for printing

        self.info_dict = video_dict

        _date_file = datetime.now().strftime("%Y%m%d")

        if not self.args.path:
            _download_path = Path(Path.home(), "testing", _date_file, self.info_dict["id"])
        else:
            _download_path = Path(self.args.path, self.info_dict["id"])

        self.premsg = f"[{self.info_dict ['id']}][{self.info_dict ['title']}]"

        self.pause_event = MySyncAsyncEvent("pause")
        self.resume_event = MySyncAsyncEvent("resume")
        self.stop_event = MySyncAsyncEvent("stop")
        self.end_tasks = MySyncAsyncEvent("end_tasks")
        self.reset_event = MySyncAsyncEvent("reset")

        self.alock = asyncio.Lock()

        self.ex_videodl = ThreadPoolExecutor(thread_name_prefix="ex_videodl")

        self.info_dl = {
            "id": self.info_dict["id"],
            "n_workers": self.args.parts,
            "rpcport": self.args.rpcport,
            "auto_pasres": False,
            "webpage_url": self.info_dict.get("webpage_url"),
            "title": self.info_dict.get("title"),
            "ytdl": ytdl,
            "date_file": _date_file,
            "download_path": _download_path,
            "filename": Path(
                _download_path.parent,
                str(self.info_dict["id"])
                + "_"
                + sanitize_filename(self.info_dict["title"], restricted=True)
                + "."
                + self.info_dict.get("ext", "mp4"),
            ),
            "fromplns": VideoDownloader._PLNS,
            "error_message": "",
            "nwsetup": nwsetup,
        }

        self._types = ""
        downloaders = []

        _new_info_dict = self.info_dict | {"filename": self.info_dl["filename"], "download_path": self.info_dl["download_path"]}

        dl = self._get_dl(_new_info_dict)
        downloaders.extend(variadic(dl))

        res = sorted(list(set([dl.status for dl in downloaders])))
        if any([_ in res for _ in ("done", "init_manipulating")]):
            _status = "init_manipulating"
        elif "error" in res:
            _status = "error"
        else:
            _status = "init"

        self.info_dl.update(
            {
                "downloaders": downloaders,
                "downloaded_subtitles": {},
                "filesize": sum([dl.filesize for dl in downloaders if hasattr(dl, "filesize")]),
                "down_size": sum([dl.down_size for dl in downloaders]),
                "status": _status,
            }
        )

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    @index.deleter
    def index(self):
        self._index = None

    def close(self):
        self.ex_videodl.shutdown(wait=False, cancel_futures=True)
        for dl in self.info_dl["downloaders"]:
            if hasattr(dl, "ex_dl"):
                dl.ex_dl.shutdown(wait=False, cancel_futures=True)

    def create_drm_cdm(self):
        with open(CONF_DRM['private_key']) as fp:
            _private_key = fp.read()
        with open(CONF_DRM['client_id'], "rb") as fp:
            _client_id = fp.read()

        device = Device(
            type_=Device.Types.ANDROID,
            security_level=3,
            flags={},
            client_id=_client_id,
            private_key=_private_key)

        return Cdm.from_device(device)

    def get_key_drm(self, pssh: str, licurl: str) -> Optional[str]:

        if VideoDownloader._CDM:
            session_id = VideoDownloader._CDM.open()
            _pssh = PSSH(pssh)
            challenge = VideoDownloader._CDM.get_license_challenge(session_id, _pssh)

            if "onlyfans" in self.info_dict["extractor_key"].lower():
                ie = self.info_dl["ytdl"].get_info_extractor(self.info_dict["extractor_key"])
                licence = cast(bytes, ie.getlicense(licurl, challenge))
                VideoDownloader._CDM.parse_license(session_id, licence)
                if keys := VideoDownloader._CDM.get_keys(session_id):
                    if _key := keys[-1].kid.hex:
                        return f"{_key}:{_key}"

    def _get_dl(self, info_dict):
        def _determine_type(info):
            protocol = determine_protocol(info)
            if "dash" in info.get("container", ""):
                return "dash"
            else:
                return protocol

        _streams = True
        if not (_info := info_dict.get("requested_formats")):
            _info = [info_dict]
            _streams = False
        elif info_dict.get("_has_drm") or self.info_dict.get(
            "has_drm"
        ):  # or 'dash' in info_dict.get('format_note', '').lower():
            dl = AsyncNativeDownloader(info_dict, self)
            self._types = "NATIVE"
            with VideoDownloader._LOCK:
                if not VideoDownloader._CDM:
                    VideoDownloader._CDM = self.create_drm_cdm()
            logger.debug(f"{self.premsg}[get_dl] DL type DASH with DRM")
            return dl
        else:
            for f in _info:
                f.update(
                    {
                        "id": info_dict["id"],
                        "title": info_dict["title"],
                        "_filename": info_dict["filename"],
                        "download_path": info_dict["download_path"],
                        "original_url": info_dict.get("original_url"),
                        "webpage_url": info_dict.get("webpage_url"),
                        "extractor_key": info_dict.get("extractor_key"),
                        "extractor": info_dict.get("extractor"),
                    }
                )

        res_dl = []
        _types = []
        for n, info in enumerate(_info):
            try:
                type_protocol = _determine_type(info)
                if type_protocol in ("http", "https"):
                    if all(
                        [
                            self.args.http_downloader == "saldl",
                            self.args.http_downloader == "aria2c" and not self.args.aria2c,
                            info.get("extractor").lower() not in CONF_HTTP_DL["ARIA2C"]["extractors"],
                            (info.get("filesize") or 0) > CONF_HTTP_DL["ARIA2C"]["max_filesize"],
                        ]
                    ):
                        dl = AsyncSALDownloader(info, self)
                        _types.append("SAL")
                        logger.debug(
                            f"[{info['id']}][{info['title']}]" + f"[{info['format_id']}][get_dl] DL type SAL"
                        )
                        if dl.auto_pasres:
                            self.info_dl.update({"auto_pasres": True})
                    elif any([self.args.aria2c]):
                        dl = AsyncARIA2CDownloader(self.info_dl["rpcport"], self.args, info, self)
                        _types.append("ARIA2")
                        logger.debug(
                            f"[{info['id']}][{info['title']}]"
                            + f"[{info['format_id']}][get_dl] DL type ARIA2C"
                        )
                        if dl.auto_pasres:
                            self.info_dl.update({"auto_pasres": True})
                    else:
                        dl = AsyncHTTPDownloader(info, self)
                        _types.append("HTTP")
                        logger.debug(f"{self.premsg}[{info['format_id']}][get_dl] DL type HTTP")
                        if dl.auto_pasres:
                            self.info_dl.update({"auto_pasres": True})

                elif type_protocol in ("m3u8", "m3u8_native"):
                    dl = AsyncHLSDownloader(self.args, info, self)  # self.args.enproxy,
                    _types.append("HLS")
                    logger.debug(f"{self.premsg}[{info['format_id']}][get_dl] DL type HLS")
                    if dl.auto_pasres:
                        self.info_dl.update({"auto_pasres": True})

                elif type_protocol in ("http_dash_segments", "dash"):
                    if _streams:
                        _str = n
                    else:
                        _str = None
                    dl = AsyncDASHDownloader(info, self, stream=_str)
                    _types.append("DASH")
                    logger.debug(f"{self.premsg}[{info['format_id']}]" + "[get_dl] DL type DASH")
                else:
                    logger.error(f"{self.premsg}[{info['format_id']}]" + ":protocol not supported")
                    raise NotImplementedError("protocol not supported")

                res_dl.append(dl)
            except Exception as e:
                logger.exception(f"{self.premsg}[{info['format_id']}] {repr(e)}")
                res_dl.append(AsyncDLError(info, repr(e)))

        self._types = " - ".join(_types)
        return res_dl

    async def change_numvidworkers(self, n):
        if self.info_dl["status"] in ("downloading", "init"):
            for dl in self.info_dl["downloaders"]:
                dl.n_workers = n
                if "aria2" in str(type(dl)).lower():
                    dl.opts.set("split", dl.n_workers)

            await self.reset("manual")

            logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: " + f"workers set to {n}")

    async def reset_from_console(self):
        await self.reset("hard")

    async def reset(self, cause: Union[str, None] = None):
        if self.info_dl["status"] == "downloading":
            _wait_tasks = []
            if not self.reset_event.is_set():
                self.reset_event.set(cause)
                await asyncio.sleep(0)
                for dl in self.info_dl["downloaders"]:
                    if "asynchls" in str(type(dl)).lower() and getattr(dl, "tasks", None):
                        _tasks = [
                            _task
                            for _task in dl.tasks
                            if not _task.done()
                            and not _task.cancelled()
                            and _task not in [asyncio.current_task()]
                        ]
                        if _tasks:
                            for _t in _tasks:
                                _t.cancel()

                            _wait_tasks.extend(_tasks)

            else:
                self.reset_event.set(cause)

            await asyncio.sleep(0)

            return _wait_tasks

    async def reset_plns(self, cause: Union[str, None] = "403", plns=None):
        if not plns:
            self.info_dl["fromplns"]["ALL"]["reset"].clear()

            plid_total = self.info_dl["fromplns"]["ALL"]["downloading"]

        else:
            plid_total = [plns]

        _wait_all_tasks = []

        for plid in plid_total:
            dict_dl = traverse_obj(self.info_dl["fromplns"], (plid, "downloaders"))
            list_dl = traverse_obj(self.info_dl["fromplns"], (plid, "downloading"))
            list_reset = traverse_obj(self.info_dl["fromplns"], (plid, "in_reset"))

            if list_dl and dict_dl:
                self.info_dl["fromplns"]["ALL"]["in_reset"].add(plid)
                self.info_dl["fromplns"][plid]["reset"].clear()
                plns = [dl for key, dl in dict_dl.items() if key in list_dl]  # type: ignore
                for dl, key in zip(plns, list_dl):  # type: ignore
                    if _tasks := await dl.reset(cause):
                        _wait_all_tasks.extend(_tasks)
                    list_reset.add(key)  # type: ignore
                    await asyncio.sleep(0)

            #  self.info_dl['fromplns'][plid]['in_reset'] = list_reset

            await asyncio.sleep(0)

        return _wait_all_tasks

    async def back_from_reset_plns(self, premsg, plns=None):
        _tasks_all = []
        if not plns:
            plid_total = self.info_dl["fromplns"]["ALL"]["in_reset"]
        else:
            plid_total = [plns]

        for plid in plid_total:
            dict_dl = traverse_obj(self.info_dl["fromplns"], (plid, "downloaders"))
            list_reset = traverse_obj(self.info_dl["fromplns"], (plid, "in_reset"))
            if list_reset and dict_dl:
                plns = [dl for key, dl in dict_dl.items() if key in list_reset]  # type: ignore
                _tasks_all.extend([asyncio.create_task(dl.end_tasks.async_wait()) for dl in plns])

        logger.debug(f"{premsg} endtasks {_tasks_all}")

        if _tasks_all:
            for _task in _tasks_all:
                self.background_tasks.add(_task)
                _task.add_done_callback(self.background_tasks.discard)
            await async_waitfortasks(
                _tasks_all, events=self.stop_event, background_tasks=self.background_tasks
            )

    async def stop(self, cause=None):
        if self.info_dl["status"] in ("done", "error"):
            return

        try:
            if not cause:
                if "aria2" not in str(type(self.info_dl["downloaders"][0])).lower():
                    self.info_dl["status"] = "stop"
                    for dl in self.info_dl["downloaders"]:
                        dl.status = "stop"

            elif cause == "exit":
                self.info_dl["status"] = "stop"
                for dl in self.info_dl["downloaders"]:
                    dl.status = "stop"

            logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}]: " + f"stop - {cause}")

            self.stop_event.set(cause)
            await asyncio.sleep(0)

            if cause == "exit" and self.reset_event.is_set():
                self.reset_event.clear()
                await asyncio.sleep(0)

        except Exception as e:
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}]: " + f"{repr(e)}")

    async def pause(self):
        if self.info_dl["status"] == "downloading":
            # if self.pause_event.is_set():
            #     self.pause_event.clear()
            #     await asyncio.sleep(0)
            # else:
            #     self.resume_event.clear()
            #     self.pause_event.set()
            #     await asyncio.sleep(0)
            if not self.pause_event.is_set():
                self.pause_event.set()
                self.resume_event.clear()
                await asyncio.sleep(0)

    async def resume(self):
        if self.info_dl["status"] == "downloading":
            if not self.resume_event.is_set():
                self.resume_event.set()
                # self.pause_event.clear()
                await asyncio.sleep(0)
            # else:
            #     self.resume_event.clear()
            #     await asyncio.sleep(0)

    async def reinit(self):
        self.pause_event.clear()
        self.resume_event.clear()
        self.stop_event.clear()
        self.end_tasks.clear()
        self.reset_event.clear()
        self.info_dl["status"] = "init"
        await asyncio.sleep(0)
        for dl in self.info_dl["downloaders"]:
            dl.status = "init"
            if hasattr(dl, "update_uri"):
                await dl.update_uri()
            if "hls" in str(type(dl)).lower():
                await sync_to_async(dl.init, thread_sensitive=False, executor=self.ex_videodl)()

    async def run_dl(self):
        self.info_dl["ytdl"].params["stop_dl"][str(self.index)] = self.stop_event
        logger.debug(
            f"[{self.info_dict['id']}][{self.info_dict['title']}]: [run_dl] "
            + f"[stop_dl] {self.info_dl['ytdl'].params['stop_dl']}"
        )

        try:
            if self.info_dl["status"] != "stop":
                self.info_dl["status"] = "downloading"
                logger.debug(
                    f"[{self.info_dict['id']}][{self.info_dict['title']}]: "
                    + f"[run_dl] status {[dl.status for dl in self.info_dl['downloaders']]}"
                )
                tasks_run_0 = [
                    asyncio.create_task(dl.fetch_async())
                    for i, dl in enumerate(self.info_dl["downloaders"])
                    if i == 0 and dl.status not in ("init_manipulating", "done")
                ]

                logger.debug(
                    f"[{self.info_dict['id']}][{self.info_dict['title']}]: "
                    + f"[run_dl] tasks run {len(tasks_run_0)}"
                )

                done = set()

                if tasks_run_0:
                    for _task in tasks_run_0:
                        self.background_tasks.add(_task)
                        _task.add_done_callback(self.background_tasks.discard)
                    done, _ = await asyncio.wait(tasks_run_0)

                if len(self.info_dl["downloaders"]) > 1:
                    tasks_run_1 = [
                        asyncio.create_task(dl.fetch_async())
                        for i, dl in enumerate(self.info_dl["downloaders"])
                        if i == 1 and dl.status not in ("init_manipulating", "done")
                    ]
                    if tasks_run_1:
                        for _task in tasks_run_1:
                            self.background_tasks.add(_task)
                            _task.add_done_callback(self.background_tasks.discard)
                        done1, _ = await asyncio.wait(tasks_run_1)
                        done = done.union(done1)

                if done:
                    for d in done:
                        try:
                            d.result()
                        except Exception as e:
                            logger.exception(
                                f"[{self.info_dict['id']}]"
                                + f"[{self.info_dict['title']}]: [run_dl] error fetch_async: {repr(e)}"
                            )

                if self.stop_event.is_set():
                    logger.info(
                        f"[{self.info_dict['id']}][{self.info_dict['title']}]"
                        + ": [run_dl] salida tasks with stop event"
                    )

                else:
                    res = sorted(list(set([dl.status for dl in self.info_dl["downloaders"]])))

                    logger.debug(
                        f"[{self.info_dict['id']}][{self.info_dict['title']}]"
                        + f": [run_dl] salida tasks {res}"
                    )

                    if "error" in res:
                        self.info_dl["status"] = "error"
                        self.info_dl["error_message"] = "\n".join(
                            [
                                dl.error_message
                                for dl in self.info_dl["downloaders"]
                                if hasattr(dl, "error_message")
                            ]
                        )

                    else:
                        self.info_dl["status"] = "init_manipulating"

        except Exception as e:
            logger.exception(
                f"[{self.info_dict['id']}][{self.info_dict['title']}]:" + f"[run_dl] error when DL {repr(e)}"
            )

            self.info_dl["status"] = "error"

    def _get_subts_files(self):
        def _dl_subt(url):
            subt_url = url
            if ".m3u8" in url:
                m3u8obj = m3u8.load(url, headers=self.info_dict.get("http_headers"))
                subt_url = urlparse.urljoin(m3u8obj.segments[0]._base_uri, m3u8obj.segments[0].uri)

            return httpx.get(subt_url, headers=self.info_dict.get("http_headers")).text

        _subts = self.info_dict.get("subtitles") or self.info_dict.get("requested_subtitles")

        if not _subts:
            return

        _langs = {}

        for key in list(_subts.keys()):
            if key.startswith("es"):
                _langs["es"] = key
            if key.startswith("en"):
                _langs["en"] = key

        if not _langs:
            return

        for _lang in _langs:
            if any(["srt" in list(el.values()) for el in _subts[_langs[_lang]]]):
                _format = "srt"
            elif any(["ttml" in list(el.values()) for el in _subts[_langs[_lang]]]):
                _format = "ttml"
            elif any(["vtt" in list(el.values()) for el in _subts[_langs[_lang]]]):
                _format = "vtt"
            else:
                return

            for el in _subts[_langs[_lang]]:
                if el["ext"] == _format:
                    try:
                        _subts_file = Path(
                            self.info_dl["filename"].absolute().parent,
                            f"{self.info_dl['filename'].stem}.{_lang}.{_format}",
                        )

                        _content = _dl_subt(el["url"])

                        with open(_subts_file, "w") as f:
                            f.write(_content)

                        logger.info(
                            f"[{self.info_dict['id']}][{self.info_dict['title']}]:"
                            + f"subs file for [{_lang}] downloaded in {_format} format"
                        )

                        if _format == "ttml":
                            _final_subts_file = Path(str(_subts_file).replace(f".{_format}", ".srt"))
                            cmd = f'tt convert -i "{_subts_file}" -o {_final_subts_file}"'
                            logger.info(
                                f"[{self.info_dict['id']}]"
                                + f"[{self.info_dict['title']}]: convert subt - {cmd}"
                            )
                            res = subprocess.run(shlex.split(cmd), encoding="utf-8", capture_output=True)
                            logger.info(
                                f"[{self.info_dict['id']}]"
                                + f"[{self.info_dict['title']}]: subs file conversion result {res.returncode}"
                            )
                            if res.returncode == 0 and _final_subts_file.exists():
                                _subts_file.unlink()
                                logger.info(
                                    f"[{self.info_dict['id']}]"
                                    + f"[{self.info_dict['title']}]: subs file for [{_lang}] in {_format} "
                                    + "format converted to srt format"
                                )
                                self.info_dl["downloaded_subtitles"].update({_lang: _final_subts_file})
                        elif _format == "vtt":
                            _final_subts_file = Path(str(_subts_file).replace(f".{_format}", ".srt"))
                            cmd = (
                                "ffmpeg -y -loglevel repeat+info -i file:"
                                + f'"{_subts_file}" -f srt -movflags +faststart file:"{_final_subts_file}"'
                            )
                            logger.info(
                                f"[{self.info_dict['id']}][{self.info_dict['title']}]: convert subt - {cmd}"
                            )
                            res = self.syncpostffmpeg(cmd)
                            logger.info(
                                f"[{self.info_dict['id']}]"
                                + f"[{self.info_dict['title']}]: subs file conversion result {res.returncode}"
                            )
                            if res.returncode == 0 and _final_subts_file.exists():
                                _subts_file.unlink()
                                logger.info(
                                    f"[{self.info_dict['id']}]"
                                    + f"[{self.info_dict['title']}]: subs file for [{_lang}] in {_format} "
                                    + "format converted to srt format"
                                )
                                self.info_dl["downloaded_subtitles"].update({_lang: _final_subts_file})

                        else:
                            self.info_dl["downloaded_subtitles"].update({_lang: _subts_file})

                        break

                    except Exception as e:
                        logger.exception(
                            f"[{self.info_dict['id']}][{self.info_dict['title']}]:"
                            + f"couldnt generate subtitle file: {repr(e)}"
                        )

    async def run_manip(self):
        aget_subts_files = sync_to_async(
            self._get_subts_files, thread_sensitive=False, executor=self.ex_videodl
        )
        apostffmpeg = sync_to_async(self.syncpostffmpeg, thread_sensitive=False, executor=self.ex_videodl)
        armtree = sync_to_async(
            partial(shutil.rmtree, ignore_errors=True), thread_sensitive=False, executor=self.ex_videodl
        )
        amove = sync_to_async(shutil.move, thread_sensitive=False, executor=self.ex_videodl)
        autime = sync_to_async(os.utime, thread_sensitive=False, executor=self.ex_videodl)

        blocking_tasks = []

        try:
            if not self.alock:
                self.alock = asyncio.Lock()

            self.info_dl["status"] = "manipulating"
            for dl in self.info_dl["downloaders"]:
                if dl.status == "init_manipulating":
                    dl.status = "manipulating"

            blocking_tasks = [
                asyncio.create_task(dl.ensamble_file())
                for dl in self.info_dl["downloaders"]
                if (
                    not any(
                        _ in str(type(dl)).lower() for _ in ("aria2", "ffmpeg", "saldownloader", "native")
                    )
                    and dl.status == "manipulating"
                )
            ]

            if self.args.subt and (
                self.info_dict.get("subtitles") or self.info_dict.get("requested_subtitles")
            ):
                blocking_tasks += [asyncio.create_task(aget_subts_files())]

            if blocking_tasks:
                for _task in blocking_tasks:
                    self.background_tasks.add(_task)
                    _task.add_done_callback(self.background_tasks.discard)
                done, _ = await asyncio.wait(blocking_tasks)

                for d in done:
                    try:
                        d.result()
                    except Exception as e:
                        logger.exception(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: [run_manip] result de dl.ensamble_file: "
                            + f"{repr(e)}"
                        )

            res = True

            for dl in self.info_dl["downloaders"]:
                _exists = all([await aiofiles.os.path.exists(_file) for _file in variadic(dl.filename)])
                res = res and _exists and dl.status == "done"
                logger.debug(
                    f"[{self.info_dict['id']}][{self.info_dict['title']}] "
                    + f"{dl.filename} exists: [{_exists}] status: [{dl.status}]"
                )

            if res:
                temp_filename = prepend_extension(str(self.info_dl["filename"]), "temp")

                if self._types == "NATIVE":
                    _video_file_temp = prepend_extension(
                        (_video_file := str(self.info_dl["downloaders"][0].filename[0])), "temp"
                    )
                    _audio_file_temp = prepend_extension(
                        (_audio_file := str(self.info_dl["downloaders"][0].filename[1])), "temp"
                    )
                    _pssh = cast(str, try_get(
                        traverse_obj(self.info_dict, ("_drm", "pssh")),
                        lambda x: list(sorted(x, key=lambda y: len(y)))[0],
                    ))
                    _licurl = cast(str, traverse_obj(self.info_dict, ("_drm", "licurl")))
                    _key = self.get_key_drm(_pssh, _licurl)

                    cmds = [
                        f"mp4decrypt --key {_key} {_video_file} {_video_file_temp}",
                        f"mp4decrypt --key {_key} {_audio_file} {_audio_file_temp}",
                    ]

                    procs = [await apostffmpeg(_cmd) for _cmd in cmds]
                    rcs = [proc.returncode for proc in procs]
                    logger.debug(
                        f"[{self.info_dict['id']}]" + f"[{self.info_dict['title']}]: {cmds}\n[rc] {rcs}"
                    )

                    rc = -1
                    if sum(rcs) == 0:
                        cmd = (
                            f'ffmpeg -y -loglevel repeat+info -i file:"{_video_file_temp}"'
                            + f' -i file:"{_audio_file_temp}" -vcodec copy -acodec copy file:"{temp_filename}"'
                        )
                        proc = await apostffmpeg(cmd)

                        rc = proc.returncode

                        logger.debug(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}"
                        )

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
                        logger.debug(
                            f"[{self.info_dict['id']}]" + f"[{self.info_dict['title']}]: DL video file OK"
                        )

                    async with async_suppress(OSError):
                        await aiofiles.os.remove(_video_file_temp)
                    async with async_suppress(OSError):
                        await aiofiles.os.remove(_audio_file_temp)

                elif len(self.info_dl["downloaders"]) == 1:
                    rc = -1
                    # usamos ffmpeg para cambiar contenedor
                    # ts del DL de HLS de un sólo stream a mp4
                    if "ts" in self.info_dl["downloaders"][0].filename.suffix:
                        cmd = (
                            "ffmpeg -y -probesize max -loglevel "
                            + f"repeat+info -i file:\"{str(self.info_dl['downloaders'][0].filename)}\""
                            + f' -c copy -map 0 -dn -f mp4 -bsf:a aac_adtstoasc file:"{temp_filename}"'
                        )

                        proc = await apostffmpeg(cmd)
                        logger.debug(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}"
                        )

                        rc = proc.returncode

                    else:
                        rc = -1
                        try:
                            res = await amove(self.info_dl["downloaders"][0].filename, temp_filename)
                            if res == temp_filename:
                                rc = 0

                        except Exception as e:
                            logger.exception(
                                f"[{self.info_dict['id']}]"
                                + f"[{self.info_dict['title']}]: error when manipulating {repr(e)}"
                            )

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
                        #  self.info_dl['status'] = "done"
                        logger.debug(
                            f"[{self.info_dict['id']}]" + f"[{self.info_dict['title']}]: DL video file OK"
                        )

                    else:
                        self.info_dl["status"] = "error"
                        raise Exception(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: error move file: {rc}"
                        )

                else:
                    cmd = (
                        "ffmpeg -y -loglevel repeat+info -i file:"
                        + f"\"{str(self.info_dl['downloaders'][0].filename)}\" -i file:"
                        + f"\"{str(self.info_dl['downloaders'][1].filename)}\" -c copy -map 0:v:0 "
                        + "-map 1:a:0 -bsf:a:0 aac_adtstoasc "
                        + f'-movflags +faststart file:"{temp_filename}"'
                    )

                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]:{cmd}")

                    rc = -1

                    proc = await apostffmpeg(cmd)

                    logger.debug(
                        f"[{self.info_dict['id']}][{self.info_dict['title']}]"
                        + f": ffmpeg rc[{proc.returncode}]\n{proc.stdout}"
                    )

                    rc = proc.returncode

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
                        #  self.info_dl['status'] = "done"
                        for dl in self.info_dl["downloaders"]:
                            for _file in variadic(dl.filename):
                                async with async_suppress(OSError):
                                    await aiofiles.os.remove(_file)

                        logger.debug(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: Streams merged for: {self.info_dl['filename']}"
                        )
                        logger.debug(
                            f"[{self.info_dict['id']}]" + f"[{self.info_dict['title']}]: DL video file OK"
                        )

                    else:
                        self.info_dl["status"] = "error"
                        raise Exception(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: error merge, ffmpeg error: {rc}"
                        )

                if self.info_dl["downloaded_subtitles"]:
                    try:
                        if len(self.info_dl["downloaded_subtitles"]) > 1:
                            lang = "es"
                            subtfile = self.info_dl["downloaded_subtitles"]["es"]
                        else:
                            lang, subtfile = list(self.info_dl["downloaded_subtitles"].items())[0]

                        embed_filename = prepend_extension(self.info_dl["filename"], "embed")

                        cmd = (
                            "ffmpeg -y -loglevel repeat+info -i "
                            + f'file:"{temp_filename}" -i file:"{str(subtfile)}" -map 0 -dn '
                            + "-ignore_unknown -c copy -c:s mov_text -map -0:s -map 1:0 "
                            + f"-metadata:s:s:0 language={lang} -movflags +faststart file:"
                            + f'"{embed_filename}"'
                        )

                        proc = await apostffmpeg(cmd)
                        logger.debug(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}"
                        )
                        if proc.returncode == 0:
                            await aiofiles.os.replace(embed_filename, self.info_dl["filename"])
                            await aiofiles.os.remove(temp_filename)
                            self.info_dl["status"] = "done"
                    except Exception as e:
                        logger.exception(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: error embeding subtitles {repr(e)}"
                        )

                else:
                    try:
                        await aiofiles.os.replace(temp_filename, self.info_dl["filename"])
                        self.info_dl["status"] = "done"
                    except Exception as e:
                        logger.exception(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: error replacing {repr(e)}"
                        )

                try:
                    await armtree(self.info_dl["download_path"])
                except Exception as e:
                    logger.exception(
                        f"[{self.info_dict['id']}][{self.info_dict['title']}]:" + f"error rmtree {repr(e)}"
                    )

                try:
                    if _meta := self.info_dict.get("meta_comment"):
                        temp_filename = prepend_extension(str(self.info_dl["filename"]), "temp")

                        cmd = (
                            "ffmpeg -y -loglevel repeat+info -i "
                            + f"file:\"{str(self.info_dl['filename'])}\" -map 0 -dn -ignore_unknown "
                            + f"-c copy -write_id3v1 1 -metadata 'comment={_meta}' -movflags +faststart "
                            + f'file:"{temp_filename}"'
                        )

                        proc = await apostffmpeg(cmd)
                        logger.debug(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}"
                        )
                        if proc.returncode == 0:
                            await aiofiles.os.replace(temp_filename, self.info_dl["filename"])

                        xattr.setxattr(
                            self.info_dl["filename"], "user.dublincore.description", _meta.encode()
                        )

                except Exception as e:
                    logger.exception(
                        f"[{self.info_dict['id']}][{self.info_dict['title']}]: error setxattr {repr(e)}"
                    )

                try:
                    if mtime := self.info_dict.get("release_timestamp"):
                        await autime(self.info_dl["filename"], (int(datetime.now().timestamp()), mtime))
                except Exception as e:
                    logger.exception(
                        f"[{self.info_dict['id']}][{self.info_dict['title']}]:" + f"error mtime {repr(e)}"
                    )

            else:
                self.info_dl["status"] = "error"

        except Exception as e:
            logger.exception(
                f"[{self.info_dict['id']}][{self.info_dict['title']}]:" + f"error when manipulating {repr(e)}"
            )
            if blocking_tasks:
                for t in blocking_tasks:
                    t.cancel()
                await asyncio.wait(blocking_tasks)
            raise
        finally:
            await asyncio.sleep(0)

    def syncpostffmpeg(self, cmd):
        try:
            res = subprocess.run(shlex.split(cmd), encoding="utf-8", capture_output=True, timeout=120)
            return res
        except Exception as e:
            return subprocess.CompletedProcess(shlex.split(cmd), 1, stdout=None, stderr=repr(e))

    def print_hookup(self):
        msg = ""
        for dl in self.info_dl["downloaders"]:
            msg += f"  {dl.print_hookup()}"
        msg += "\n"

        if self.info_dl["status"] == "downloading":
            _maxlen = 40
        else:
            _maxlen = 10

        _title = (
            self.info_dict["title"]
            if ((_len := len(self.info_dict["title"])) >= _len)
            else self.info_dict["title"] + " " * (_maxlen - _len)
        )

        _pre = f"[{self.index}][{self.info_dict['id']}][{_title[:_maxlen]}]:"

        def _progress_dl():
            return f"{naturalsize(self.info_dl['down_size'], format_='.2f')} [{naturalsize(self.info_dl['filesize'], format_='.2f')}]"

        if self.info_dl["status"] == "done":
            return f"{_pre} Completed [{naturalsize(self.info_dl['filename'].stat().st_size, format_='.2f')}]\n {msg}\n"
        elif self.info_dl["status"] == "init":
            return f"{_pre} Waiting to DL [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n"
        elif self.info_dl["status"] == "init_manipulating":
            return f"{_pre} Waiting to create file [{naturalsize(self.info_dl['filesize'], format_='.2f')}]\n {msg}\n"
        elif self.info_dl["status"] == "error":
            return f"{_pre} ERROR {_progress_dl()}\n {msg}\n"
        elif self.info_dl["status"] == "stop":
            return f"{_pre} STOPPED {_progress_dl()}\n {msg}\n"
        elif self.info_dl["status"] == "downloading":
            if self.pause_event.is_set() and not self.resume_event.is_set():
                status = "PAUSED"
            else:
                status = "Downloading"
            return f"{_pre} {status} {_progress_dl()}\n {msg}\n"
        elif self.info_dl["status"] == "manipulating":
            return f"{_pre} Ensambling/Merging {_progress_dl()}\n {msg}\n"
