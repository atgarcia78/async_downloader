import asyncio
import logging
import os
import shlex
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from queue import Queue
from threading import Lock

import aiofiles.os
import xattr
from pywidevine.cdm import Cdm
from pywidevine.device import Device
from pywidevine.pssh import PSSH
from yt_dlp.utils import determine_protocol, sanitize_filename

from asyncaria2cdownloader import AsyncARIA2CDownloader
from asynchlsdownloader import AsyncHLSDownloader
from asynchttpdownloader import AsyncHTTPDownloader
from asyncnativedownloader import AsyncNativeDownloader
from utils import (
    CONF_DRM,
    Callable,
    Coroutine,
    InfoDL,
    MySyncAsyncEvent,
    Optional,
    Union,
    async_lock,
    async_suppress,
    cast,
    get_xml,
    naturalsize,
    prepend_extension,
    sync_to_async,
    translate_srt,
    traverse_obj,
    try_get,
    validate_drm_lic,
    variadic,
)

logger = logging.getLogger("video_DL")


class AsyncDLError:
    def __init__(self, info_dict, msg_error=None):
        self.status = "error"
        self.error_message = msg_error
        self.info_dict = info_dict
        self.filesize = info_dict.get("filesize")
        self.down_size = 0


class VideoDownloader:
    _DIC_DL = {}
    _QUEUE = Queue()
    _LOCK = Lock()
    _CDM = None

    def __init__(self, video_dict, ytdl, nwsetup, args):
        self.background_tasks = set()
        self.master_hosts_alock = partial(async_lock, VideoDownloader._LOCK)
        self.master_hosts_lock = VideoDownloader._LOCK
        self.args = args

        self._index = None  # for printing

        self.info_dict = video_dict

        _date_file = datetime.now().strftime("%Y%m%d")

        if not self.args.path:

            _download_path = Path(Path.home(), "testing", _date_file, self.info_dict["id"])
            if self.args.use_path_pl:

                _pltitle = self.info_dict.get("playlist") or self.info_dict.get("playlist_title")
                _plid = self.info_dict.get('playlist_id')
                if _pltitle and _plid:
                    _base = f"{_plid}_{sanitize_filename(_pltitle, restricted=True)}_{self.info_dict.get('extractor_key')}"
                    _download_path = Path(Path.home(), "testing", _base, self.info_dict["id"])
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
        self.sync_to_async = partial(
            sync_to_async, thread_sensitive=False, executor=self.ex_videodl)

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
            "error_message": "",
            "nwsetup": nwsetup
        }

        self._types = ""
        downloaders = []

        _new_info_dict = self.info_dict | {"filename": self.info_dl["filename"],
                                           "download_path": self.info_dl["download_path"]}

        self.total_sizes = {
            "filesize": 0,
            "down_size": 0}

        self._infodl = InfoDL(
            self.pause_event, self.resume_event, self.stop_event,
            self.end_tasks, self.reset_event, self.total_sizes, nwsetup)

        dl = self._get_dl(_new_info_dict)
        downloaders.extend(variadic(dl))

        res = sorted(list(set([dl.status for dl in downloaders])))
        if any([_ in res for _ in ("done", "init_manipulating")]):
            _status = "init_manipulating"
        elif "error" in res:
            _status = "error"
        else:
            _status = "init"

        _filesize = sum([dl.filesize for dl in downloaders if getattr(dl, "filesize", None)])
        _down_size = sum([dl.down_size for dl in downloaders])

        self.total_sizes.update({"filesize": _filesize, "down_size": _down_size})

        self.info_dl.update(
            {
                "downloaders": downloaders,
                "downloaded_subtitles": {},
                "filesize": _filesize,
                "down_size": _down_size,
                "status": _status,
            }
        )

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value
        for dl in self.info_dl["downloaders"]:
            dl.pos = value
        VideoDownloader._DIC_DL.update({value: self})

    @index.deleter
    def index(self):
        self._index = None

    def close(self):
        self.ex_videodl.shutdown(wait=False, cancel_futures=True)
        for dl in self.info_dl["downloaders"]:
            if hasattr(dl, "ex_dl"):
                dl.ex_dl.shutdown(wait=False, cancel_futures=True)

    def clear_events(self):
        self.pause_event.clear()
        self.resume_event.clear()
        self.stop_event.clear()
        self.end_tasks.clear()
        self.reset_event.clear()

    @classmethod
    def create_drm_cdm(cls):
        with open(CONF_DRM['private_key']) as fpriv:
            _private_key = fpriv.read()
        with open(CONF_DRM['client_id'], "rb") as fpid:
            _client_id = fpid.read()

        device = Device(
            type_=Device.Types.ANDROID,
            security_level=3,
            flags={},
            client_id=_client_id,
            private_key=_private_key)

        return Cdm.from_device(device)

    @classmethod
    def _get_key_drm(
            cls, lic_url: str, pssh: Optional[str] = None,
            func_validate: Optional[Callable] = None, mpd_url: Optional[str] = None):

        if not pssh and mpd_url:
            if (mpd_xml := get_xml(mpd_url)):
                if (_list_pssh := cast(list[str], list(set(list(map(
                        lambda x: x.text, list(mpd_xml.iterfind('.//{urn:mpeg:cenc:2013}pssh')))))))):
                    _list_pssh.sort(key=len)
                    pssh = _list_pssh[0]
        if pssh:
            with VideoDownloader._LOCK:
                if not VideoDownloader._CDM:
                    VideoDownloader._CDM = VideoDownloader.create_drm_cdm()

            session_id = VideoDownloader._CDM.open()
            challenge = VideoDownloader._CDM.get_license_challenge(session_id, PSSH(pssh))
            _validate_lic = func_validate or validate_drm_lic
            VideoDownloader._CDM.parse_license(session_id, _validate_lic(lic_url, challenge))
            if (keys := VideoDownloader._CDM.get_keys(session_id)):
                for key in keys:
                    if key.type == 'CONTENT':
                        return f"{key.kid.hex}:{key.key.hex()}"

    def get_key_drm(self, licurl: str, pssh: str):

        from yt_dlp.extractor.onlyfans import OnlyFansBaseIE

        _func_validate = None
        if "onlyfans" in self.info_dict["extractor_key"].lower():
            _func_validate = OnlyFansBaseIE.validate_drm_lic
        return VideoDownloader._get_key_drm(licurl, pssh=pssh, func_validate=_func_validate)

    def _get_dl(self, info_dict):

        def _determine_type(info):
            protocol = determine_protocol(info)
            if "dash" in info.get("container", ""):
                return "dash"
            else:
                return protocol

        if not (_info := info_dict.get("requested_formats")):
            _info = [info_dict]
        elif info_dict.get("_has_drm") or self.info_dict.get("has_drm"):
            dl = AsyncNativeDownloader(
                self.args, self.info_dl["ytdl"], info_dict, self._infodl, drm=True)
            self._types = "NATIVE_DRM"
            logger.debug(f"{self.premsg}[get_dl] DL type DASH with DRM")
            return dl
        elif info_dict.get("extractor_key") == "Youtube":
            dl = AsyncNativeDownloader(
                self.args, self.info_dl["ytdl"], info_dict, self._infodl, drm=False)
            self._types = "NATIVE"
            logger.debug(f"{self.premsg}[get_dl] DL type youtue")
            return dl
        else:
            for f in _info:
                f.update({
                    "id": info_dict["id"],
                    "title": info_dict["title"],
                    "_filename": info_dict["filename"],
                    "download_path": info_dict["download_path"],
                    "original_url": info_dict.get("original_url"),
                    "webpage_url": info_dict.get("webpage_url"),
                    "extractor_key": info_dict.get("extractor_key"),
                    "extractor": info_dict.get("extractor")})

        res_dl = []
        _types = []
        for _, info in enumerate(_info):
            try:
                type_protocol = _determine_type(info)
                # si uno de los dl tiene que ser dash, hacemos un sólo dl native
                if type_protocol in ("http_dash_segments", "dash"):
                    dl = AsyncNativeDownloader(
                        self.args, self.info_dl["ytdl"], info_dict, self._infodl, drm=False)
                    self._types = "NATIVE"
                    logger.debug(f"{self.premsg}[get_dl] DL type youtue")
                    return dl

                elif type_protocol in ("http", "https"):
                    if any([self.args.aria2c]):
                        dl = AsyncARIA2CDownloader(
                            self.info_dl["rpcport"], self.args, self.info_dl["ytdl"], info, self._infodl)
                        _types.append("ARIA2")
                        logger.debug(f"{self.premsg}[{info['format_id']}][get_dl] DL type ARIA2C")
                        if dl.auto_pasres:
                            self.info_dl.update({"auto_pasres": True})
                    else:
                        dl = AsyncHTTPDownloader(info, self)
                        _types.append("HTTP")
                        logger.debug(f"{self.premsg}[{info['format_id']}][get_dl] DL type HTTP")
                        if dl.auto_pasres:
                            self.info_dl.update({"auto_pasres": True})

                elif type_protocol in ("m3u8", "m3u8_native"):
                    dl = AsyncHLSDownloader(
                        self.args, self.info_dl["ytdl"], info, self._infodl)  # self.args.enproxy,
                    _types.append("HLS")
                    logger.debug(f"{self.premsg}[{info['format_id']}][get_dl] DL type HLS")
                    if dl.auto_pasres:
                        self.info_dl.update({"auto_pasres": True})

                else:
                    logger.error(
                        f"{self.premsg}[{info['format_id']}]:protocol not supported")
                    raise NotImplementedError("protocol not supported")

                res_dl.append(dl)

            except Exception as e:
                logger.exception(f"{self.premsg}[{info['format_id']}] {repr(e)}")
                res_dl.append(AsyncDLError(info, repr(e)))

        self._types = " - ".join(_types)
        return res_dl

    def add_task(
            self, coro: Union[Coroutine, asyncio.Task], *, name: Optional[str] = None) -> asyncio.Task:

        if not isinstance(coro, asyncio.Task):
            _task = asyncio.create_task(coro, name=name)
        else:
            _task = coro

        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    async def change_numvidworkers(self, n):
        if self.info_dl["status"] in ("downloading", "init"):
            for dl in self.info_dl["downloaders"]:
                dl.n_workers = n
                if "aria2" in str(type(dl)).lower():
                    dl.opts.set("split", dl.n_workers)

            await self.reset("manual")

            logger.info(f"{self.premsg}: workers set to {n}")

    async def reset_from_console(self):
        await self.reset("hard")

    async def reset(self, cause: Union[str, None] = None, wait=True):
        if self.info_dl["status"] == "downloading":
            _wait_tasks = []
            if not self.reset_event.is_set():
                self.reset_event.set(cause)
                await asyncio.sleep(0)
                for dl in self.info_dl["downloaders"]:
                    if "asynchls" in str(type(dl)).lower() and getattr(dl, "tasks", None):
                        if _tasks := [
                            _task for _task in dl.tasks
                            if not _task.done() and not _task.cancelled()
                            and _task not in [asyncio.current_task()]
                        ]:
                            for _t in _tasks:
                                _t.cancel()
                            _wait_tasks.extend(_tasks)
            else:
                self.reset_event.set(cause)

            if wait and _wait_tasks:
                await asyncio.wait(_wait_tasks)

            return _wait_tasks

    async def stop(self, cause=None, wait=True):
        if self.info_dl["status"] in ("done", "error") or self.stop_event.is_set() == "exit":
            return

        try:

            self.info_dl["status"] = "stop"
            for dl in self.info_dl["downloaders"]:
                dl.status = "stop"

            self.stop_event.set(cause)
            await asyncio.sleep(0)

            if cause == "exit":
                if self.reset_event.is_set():
                    self.reset_event.clear()
                    await asyncio.sleep(0)
                _wait_tasks = []
                for dl in self.info_dl["downloaders"]:
                    if "asynchls" in str(type(dl)).lower() and getattr(dl, "tasks", None):
                        if _tasks := [
                            _task for _task in dl.tasks
                            if not _task.done() and not _task.cancelled()
                            and _task not in [asyncio.current_task()]
                        ]:
                            for _t in _tasks:
                                _t.cancel()
                            _wait_tasks.extend(_tasks)
                if wait and _wait_tasks:
                    await asyncio.wait(_wait_tasks)

        except Exception as e:
            logger.exception(f"{self.premsg}: " + f"{repr(e)}")

    async def pause(self):
        if self.info_dl["status"] == "downloading":
            if not self.pause_event.is_set():
                self.pause_event.set()
                self.resume_event.clear()
                await asyncio.sleep(0)

    async def resume(self):
        if self.info_dl["status"] == "downloading":
            if not self.resume_event.is_set():
                self.resume_event.set()
                await asyncio.sleep(0)

    async def reinit(self):
        self.clear_events()
        self.info_dl["status"] = "init"
        await asyncio.sleep(0)
        for dl in self.info_dl["downloaders"]:
            dl.status = "init"
            if hasattr(dl, "update_uri"):
                await dl.update_uri()
            if "hls" in str(type(dl)).lower():
                await self.sync_to_async(dl.init)()

    async def run_dl(self):
        self.info_dl["ytdl"].params["stop_dl"][str(self.index)] = self.stop_event
        logger.debug(
            f"{self.premsg}: [run_dl] "
            + f"[stop_dl] {self.info_dl['ytdl'].params['stop_dl']}")

        try:
            if self.info_dl["status"] != "stop":
                self.info_dl["status"] = "downloading"
                logger.debug(
                    f"{self.premsg}[run_dl] status {[dl.status for dl in self.info_dl['downloaders']]}")
                tasks_run_0 = [
                    self.add_task(dl.fetch_async(), name=f'fetch_async_{i}')
                    for i, dl in enumerate(self.info_dl["downloaders"])
                    if i == 0 and dl.status not in ("init_manipulating", "done")
                ]

                logger.debug(f"{self.premsg}[run_dl] tasks run {len(tasks_run_0)}")

                done = set()

                if tasks_run_0:
                    done, _ = await asyncio.wait(tasks_run_0)

                if len(self.info_dl["downloaders"]) > 1:
                    tasks_run_1 = [
                        self.add_task(dl.fetch_async(), name=f'fetch_async_{i}')
                        for i, dl in enumerate(self.info_dl["downloaders"])
                        if i == 1 and dl.status not in ("init_manipulating", "done")
                    ]
                    if tasks_run_1:
                        done1, _ = await asyncio.wait(tasks_run_1)
                        done = done.union(done1)

                if done:
                    for d in done:
                        try:
                            d.result()
                        except Exception as e:
                            logger.exception(f"{self.premsg}[run_dl] error fetch_async: {repr(e)}")

                if self.stop_event.is_set():
                    logger.debug(f"{self.premsg}[run_dl] salida tasks with stop event - {self.info_dl['status']}")
                    self.info_dl["status"] = "stop"

                else:
                    res = sorted(list(set([dl.status for dl in self.info_dl["downloaders"]])))

                    logger.debug(f"{self.premsg}[run_dl] salida tasks {res}")

                    if "error" in res:
                        self.info_dl["status"] = "error"
                        self.info_dl["error_message"] = "\n".join(
                            [
                                dl.error_message
                                for dl in self.info_dl["downloaders"]
                                if hasattr(dl, "error_message")
                            ])

                    else:
                        self.info_dl["status"] = "init_manipulating"

        except Exception as e:
            logger.exception(f"{self.premsg}[run_dl] error when DL {repr(e)}")

            self.info_dl["status"] = "error"

    def _get_subts_files(self):
        def _dl_subt():
            cmd = [
                "yt-dlp",
                "-P",
                self.info_dl['filename'].absolute().parent,
                "-o",
                f"{self.info_dl['filename'].stem}.%(ext)s",
                "--no-download",
                self.info_dict["webpage_url"]]
            proc = subprocess.run(cmd, encoding="utf-8", capture_output=True)
            return proc

        _subts = self.info_dict.get("requested_subtitles")

        if not _subts:
            return

        res = _dl_subt()

        logger.info(f"{self.premsg}: subs dl and converted to srt, rc[{res.returncode}]")

        _final_subts = {}

        for key, val in _subts.items():
            if key.startswith("es"):
                _final_subts['es'] = val
            if key.startswith("en"):
                _final_subts['en'] = val
            if key == "ca":
                _final_subts['ca'] = val

        if not _final_subts:
            return

        for _lang, _ in _final_subts.items():
            try:
                _subts_file = Path(
                    self.info_dl["filename"].absolute().parent,
                    f"{self.info_dl['filename'].stem}.{_lang}.srt")

                logger.info(f"{self.premsg}: {str(_subts_file)} exists[{_subts_file.exists()}]")
                if _subts_file.exists():
                    self.info_dl["downloaded_subtitles"].update({_lang: _subts_file})

            except Exception as e:
                logger.exception(f"{self.premsg} couldnt generate subtitle file: {repr(e)}")

        if 'ca' in self.info_dl["downloaded_subtitles"] and 'es' not in self.info_dl["downloaded_subtitles"]:
            logger.info(f"{self.premsg}: subs will translate from [ca, srt] to [es, srt]")
            _subs_file = Path(
                self.info_dl["filename"].absolute().parent,
                f"{self.info_dl['filename'].stem}.es.srt")
            with open(_subs_file, 'w') as f:
                f.write(translate_srt(self.info_dl["downloaded_subtitles"]['ca'], 'ca', 'es'))
            self.info_dl["downloaded_subtitles"]['es'] = _subs_file
            logger.info(f"{self.premsg}: subs file [es, srt] ready")

    async def run_manip(self):
        aget_subts_files = self.sync_to_async(self._get_subts_files)
        apostffmpeg = self.sync_to_async(self.syncpostffmpeg)
        armtree = self.sync_to_async(partial(shutil.rmtree, ignore_errors=True))
        amove = self.sync_to_async(shutil.move)
        autime = self.sync_to_async(os.utime)

        blocking_tasks = []

        try:
            if not self.alock:
                self.alock = asyncio.Lock()

            self.info_dl["status"] = "manipulating"
            for dl in self.info_dl["downloaders"]:
                if dl.status == "init_manipulating":
                    dl.status = "manipulating"

            blocking_tasks = [
                self.add_task(dl.ensamble_file(), name=f'ensamble_file_{dl.premsg}')
                for dl in self.info_dl["downloaders"]
                if (
                    not any(
                        _ in str(type(dl)).lower() for _ in ("aria2", "ffmpeg", "saldownloader", "native")
                    )
                    and dl.status == "manipulating"
                )
            ]

            if self.args.subt and self.info_dict.get("requested_subtitles"):
                blocking_tasks += [self.add_task(aget_subts_files(), name='get_subts')]

            if blocking_tasks:

                done, _ = await asyncio.wait(blocking_tasks)

                for d in done:
                    try:
                        d.result()
                    except Exception as e:
                        logger.exception(
                            f"{self.premsg}[run_manip] result de dl.ensamble_file: "
                            + f"{repr(e)}")

            res = True

            for dl in self.info_dl["downloaders"]:
                _exists = all([await aiofiles.os.path.exists(_file) for _file in variadic(dl.filename)])
                res = res and _exists and dl.status == "done"
                logger.debug(
                    f"{self.premsg} "
                    + f"{dl.filename} exists: [{_exists}] status: [{dl.status}]")

            if res:
                temp_filename = prepend_extension(str(self.info_dl["filename"]), "temp")

                if self._types == "NATIVE_DRM":
                    _video_file_temp = prepend_extension(
                        (_video_file := str(self.info_dl["downloaders"][0].filename[0])), "temp")
                    _audio_file_temp = prepend_extension(
                        (_audio_file := str(self.info_dl["downloaders"][0].filename[1])), "temp")

                    _pssh = cast(str, try_get(
                        traverse_obj(self.info_dict, ("_drm", "pssh")),
                        lambda x: list(sorted(x, key=lambda y: len(y)))[0]))
                    _licurl = cast(str, traverse_obj(self.info_dict, ("_drm", "licurl")))

                    _key = None
                    if not _pssh or not _licurl or not (_key := self.get_key_drm(_licurl, _pssh)):

                        raise Exception(
                            f"{self.premsg}: error processing DRM info - licurl[{_licurl}] pssh[{_pssh}] key[{_key}]")

                    cmds = [
                        f"mp4decrypt --key {_key} {_video_file} {_video_file_temp}",
                        f"mp4decrypt --key {_key} {_audio_file} {_audio_file_temp}"]

                    procs = [await apostffmpeg(_cmd) for _cmd in cmds]
                    rcs = [proc.returncode for proc in procs]
                    logger.debug(
                        f"{self.premsg}: {cmds}\n[rc] {rcs}")

                    rc = -1
                    if sum(rcs) == 0:
                        cmd = (
                            f'ffmpeg -y -loglevel repeat+info -i file:"{_video_file_temp}"'
                            + f' -i file:"{_audio_file_temp}" -vcodec copy -acodec copy file:"{temp_filename}"')
                        proc = await apostffmpeg(cmd)

                        rc = proc.returncode

                        logger.debug(
                            f"{self.premsg}: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}")

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
                        logger.debug(f"{self.premsg}: DL video file OK")

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
                            + f' -c copy -map 0 -dn -f mp4 -bsf:a aac_adtstoasc file:"{temp_filename}"')

                        proc = await apostffmpeg(cmd)
                        logger.debug(
                            f"{self.premsg}: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}")

                        rc = proc.returncode

                    else:
                        rc = -1
                        try:
                            res = await amove(self.info_dl["downloaders"][0].filename, temp_filename)
                            if res == temp_filename:
                                rc = 0

                        except Exception as e:
                            logger.exception(
                                f"{self.premsg}: error when manipulating {repr(e)}")

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
                        logger.debug(f"{self.premsg}: DL video file OK")

                    else:
                        self.info_dl["status"] = "error"
                        raise Exception(
                            f"{self.premsg}: error move file: {rc}")

                else:
                    cmd = (
                        "ffmpeg -y -loglevel repeat+info -i file:"
                        + f"\"{str(self.info_dl['downloaders'][0].filename)}\" -i file:"
                        + f"\"{str(self.info_dl['downloaders'][1].filename)}\" -c copy -map 0:v:0 "
                        + "-map 1:a:0 -bsf:a:0 aac_adtstoasc "
                        + f'-movflags +faststart file:"{temp_filename}"')

                    logger.debug(f"{self.premsg}:{cmd}")

                    rc = -1

                    proc = await apostffmpeg(cmd)

                    logger.debug(
                        f"{self.premsg}"
                        + f": ffmpeg rc[{proc.returncode}]\n{proc.stdout}")

                    rc = proc.returncode

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
                        #  self.info_dl['status'] = "done"
                        for dl in self.info_dl["downloaders"]:
                            for _file in variadic(dl.filename):
                                async with async_suppress(OSError):
                                    await aiofiles.os.remove(_file)

                        logger.debug(
                            f"{self.premsg}: "
                            + f"Streams merged for: {self.info_dl['filename']}")
                        logger.debug(
                            f"{self.premsg}: DL video file OK")

                    else:
                        self.info_dl["status"] = "error"
                        raise Exception(
                            f"{self.premsg}: error merge, ffmpeg error: {rc}")

                if self.info_dl["downloaded_subtitles"]:
                    try:

                        maplang = {'en': 'eng', 'es': 'spa', 'ca': 'cat'}
                        subtfiles = []
                        subtlang = []

                        embed_filename = prepend_extension(str(self.info_dl["filename"]), "embed")

                        def _make_embed_cmd():
                            for i, (_lang, _file) in enumerate(self.info_dl["downloaded_subtitles"].items()):
                                subtfiles.append(f"-i file:'{_file}'")
                                subtlang.append(f"-map {i+1}:0 -metadata:s:s:{i} language={maplang.get(_lang, _lang)}")
                            return (
                                f"ffmpeg -y -loglevel repeat+info -i file:'{temp_filename}' {' '.join(subtfiles)} " +
                                f"-map 0 -dn -ignore_unknown -c copy -c:s mov_text -map -0:s {' '.join(subtlang)} " +
                                f"-movflags +faststart {embed_filename}")

                        proc = await apostffmpeg(cmd := _make_embed_cmd())
                        logger.debug(
                            f"{self.premsg}: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}")
                        if proc.returncode == 0:
                            await aiofiles.os.replace(embed_filename, self.info_dl["filename"])
                            async with async_suppress(OSError):
                                await aiofiles.os.remove(temp_filename)
                            self.info_dl["status"] = "done"
                            for _file in self.info_dl["downloaded_subtitles"].values():
                                async with async_suppress(OSError):
                                    await aiofiles.os.remove(_file)

                    except Exception as e:
                        logger.exception(f"{self.premsg}: error embeding subtitles {repr(e)}")

                else:
                    try:
                        await aiofiles.os.replace(temp_filename, self.info_dl["filename"])
                        self.info_dl["status"] = "done"
                    except Exception as e:
                        logger.exception(f"{self.premsg} error replacing {repr(e)}")

                try:
                    await armtree(self.info_dl["download_path"])
                except Exception as e:
                    logger.exception(
                        f"{self.premsg} error rmtree {repr(e)}")

                try:
                    if _meta := self.info_dict.get("meta_comment"):
                        temp_filename = prepend_extension(str(self.info_dl["filename"]), "temp")

                        cmd = (
                            "ffmpeg -y -loglevel repeat+info -i "
                            + f"file:\"{str(self.info_dl['filename'])}\" -map 0 -dn -ignore_unknown "
                            + f"-c copy -write_id3v1 1 -metadata 'comment={_meta}' -movflags +faststart "
                            + f'file:"{temp_filename}"')

                        proc = await apostffmpeg(cmd)
                        logger.debug(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}")
                        if proc.returncode == 0:
                            await aiofiles.os.replace(temp_filename, self.info_dl["filename"])

                        xattr.setxattr(
                            str(self.info_dl["filename"]), "user.dublincore.description", _meta.encode())

                except Exception as e:
                    logger.exception(
                        f"{self.premsg}: error setxattr {repr(e)}")

                try:
                    if mtime := self.info_dict.get("release_timestamp"):
                        await autime(
                            self.info_dl["filename"], (int(datetime.now().timestamp()), mtime))
                except Exception as e:
                    logger.exception(f"{self.premsg} error mtime {repr(e)}")

            else:
                self.info_dl["status"] = "error"

        except Exception as e:
            logger.exception(f"{self.premsg} error when manipulating {repr(e)}")
            if blocking_tasks:
                for t in blocking_tasks:
                    t.cancel()
                await asyncio.wait(blocking_tasks)
            raise
        finally:
            await asyncio.sleep(0)

    def syncpostffmpeg(self, cmd):
        try:
            res = subprocess.run(
                shlex.split(cmd), encoding="utf-8", capture_output=True, timeout=120)
            return res
        except Exception as e:
            return subprocess.CompletedProcess(
                shlex.split(cmd), 1, stdout=None, stderr=repr(e))

    def print_hookup(self):

        def _pre(_maxlen=10):
            _title = (
                self.info_dict["title"]
                if ((_len := len(self.info_dict["title"])) >= _len)
                else self.info_dict["title"] + " " * (_maxlen - _len))
            return f"[{self.index}][{self.info_dict['id']}][{_title[:_maxlen]}]:"

        def _get_msg():
            msg = ""
            for dl in self.info_dl["downloaders"]:
                msg += f"  {dl.print_hookup()}"
            msg += "\n"
            return msg

        def _filesize_str():
            return f"[{naturalsize(self.total_sizes['filesize'], format_='.2f')}]"

        def _progress_dl():
            return f"{naturalsize(self.total_sizes['down_size'], format_='.2f')} {_filesize_str()}"

        if self.info_dl["status"] == "done":
            if not (_size_str := getattr(self, '_size_str', None)):
                self._size_str = f"{naturalsize(self.info_dl['filename'].stat().st_size, format_='.2f')}"
            return f"{_pre()} Completed [{_size_str}]\n {_get_msg()}\n"
        elif self.info_dl["status"] == "init":
            return f"{_pre()} Waiting to DL [{_filesize_str()}]\n {_get_msg()}\n"
        elif self.info_dl["status"] == "init_manipulating":
            return f"{_pre()} Waiting to create file [{_filesize_str()}]\n {_get_msg()}\n"
        elif self.info_dl["status"] == "error":
            return f"{_pre()} ERROR {_progress_dl()}\n {_get_msg()}\n"
        elif self.info_dl["status"] == "stop":
            return f"{_pre()} STOPPED {_progress_dl()}\n {_get_msg()}\n"
        elif self.info_dl["status"] == "downloading":
            if self.pause_event.is_set() and not self.resume_event.is_set():
                status = "PAUSED"
            else:
                status = "Downloading"
            return f"{_pre(40)} {status} {_progress_dl()}\n {_get_msg()}\n"
        elif self.info_dl["status"] == "manipulating":
            return f"{_pre()} Ensambling/Merging {_progress_dl()}\n {_get_msg()}\n"
