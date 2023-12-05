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
from pywidevine.device import Device, DeviceTypes
from pywidevine.pssh import PSSH
from yt_dlp.utils import sanitize_filename

from asyncaria2cdownloader import AsyncARIA2CDownloader
from asynchlsdownloader import AsyncHLSDownloader
from asynchttpdownloader import AsyncHTTPDownloader
from asyncnativedownloader import AsyncNativeDownloader
from utils import (
    CONF_DRM,
    AsyncDLError,
    Callable,
    Coroutine,
    InfoDL,
    MySyncAsyncEvent,
    Optional,
    Union,
    async_suppress,
    cast,
    get_protocol,
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


class AsyncErrorDownloader:
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
        self.args = args

        self._index = None  # for printing

        self.info_dict = video_dict

        _date_file = datetime.now().strftime("%Y%m%d")

        if not self.args.path:

            _base = _date_file
            if self.args.use_path_pl:
                _pltitle = try_get(
                    self.info_dict.get("playlist") or self.info_dict.get("playlist_title"),
                    lambda x: sanitize_filename(x, restricted=True))
                _plid = self.info_dict.get('playlist_id')
                if _pltitle and _plid:
                    _base = f"{_plid}_{_pltitle}_{self.info_dict.get('extractor_key')}"

            _download_path = Path(Path.home(), "testing", _base, self.info_dict["id"])

        else:
            _download_path = Path(self.args.path, self.info_dict["id"])

        self.premsg = f"[{self.info_dict ['id']}][{self.info_dict ['title']}]"

        self.pause_event = MySyncAsyncEvent("pause")
        self.resume_event = MySyncAsyncEvent("resume")
        self.stop_event = MySyncAsyncEvent("stop")
        self.end_tasks = MySyncAsyncEvent("end_tasks")
        self.reset_event = MySyncAsyncEvent("reset")

        # self.alock = asyncio.Lock()

        self.ex_videodl = ThreadPoolExecutor(thread_name_prefix="ex_videodl")
        self.sync_to_async = partial(
            sync_to_async, thread_sensitive=False, executor=self.ex_videodl)

        _title = sanitize_filename(self.info_dict["title"], restricted=True)

        self.info_dl = {
            "id": self.info_dict["id"],
            "n_workers": self.args.parts,
            "rpcport": self.args.rpcport,
            "auto_pasres": False,
            "webpage_url": self.info_dict.get("webpage_url"),
            "title": _title,
            "ytdl": ytdl,
            "date_file": _date_file,
            "download_path": _download_path,
            "filename": Path(
                _download_path.parent, f'{self.info_dict["id"]}_{_title}.{self.info_dict.get("ext", "mp4")}'),
            "error_message": "",
            "nwsetup": nwsetup
        }

        self._types = ""
        downloaders = []

        self.total_sizes = {
            "filesize": 0,
            "down_size": 0}

        self._infodl = InfoDL(
            self.pause_event, self.resume_event, self.stop_event,
            self.end_tasks, self.reset_event, self.total_sizes, nwsetup)

        downloaders.extend(variadic(self._get_dl(self.info_dict | {
            "filename": self.info_dl["filename"],
            "download_path": self.info_dl["download_path"]})))

        _all_status = [dl.status for dl in downloaders]
        if "error" in _all_status:
            _status = "error"
        elif all(el in ("done", "init_manipulating") for el in _all_status):
            _status = "init_manipulating"
        else:
            _status = "init"
        _filesize = sum(getattr(dl, "filesize", 0) for dl in downloaders)
        _down_size = sum(getattr(dl, "down_size", 0) for dl in downloaders)
        self.total_sizes |= {"filesize": _filesize, "down_size": _down_size}

        self.info_dl |= {
            "downloaders": downloaders,
            "downloaded_subtitles": {},
            "filesize": _filesize,
            "down_size": _down_size,
            "status": _status,
        }

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

    @classmethod
    def create_drm_cdm(cls):  # sourcery skip: path-read
        with open(CONF_DRM['private_key']) as fpriv:
            _private_key = fpriv.read()
        with open(CONF_DRM['client_id'], "rb") as fpid:
            _client_id = fpid.read()

        device = Device(
            type_=DeviceTypes.ANDROID,
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

        _drm = try_get(
            info_dict.get('_has_drm') or info_dict.get('has_drm'),
            lambda x: False if x is None else x)
        _dash = get_protocol(info_dict) == "dash"

        if _drm or _dash or info_dict.get("extractor_key") == "Youtube":
            dl = AsyncNativeDownloader(
                self.args, self.info_dl["ytdl"], info_dict, self._infodl, drm=_drm)
            self._types = "NATIVE_DRM" if _drm else "NATIVE"
            logger.debug(f"{self.premsg}[get_dl] DL type native drm[{_drm}]")
            return dl

        if not (_info := info_dict.get("requested_formats")):
            _info = [info_dict]
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
        for info in _info:
            try:
                type_protocol = get_protocol(info)
                if type_protocol in ("http", "https"):
                    if self.args.aria2c:
                        dl = AsyncARIA2CDownloader(
                            self.info_dl["rpcport"], self.args, self.info_dl["ytdl"], info, self._infodl)
                        _types.append("ARIA2")
                        logger.debug(f"{self.premsg}[{info['format_id']}][get_dl] DL type ARIA2C")

                    else:
                        dl = AsyncHTTPDownloader(info, self)
                        _types.append("HTTP")
                        logger.debug(f"{self.premsg}[{info['format_id']}][get_dl] DL type HTTP")

                elif type_protocol in ("m3u8", "m3u8_native"):
                    dl = AsyncHLSDownloader(
                        self.args, self.info_dl["ytdl"], info, self._infodl)  # self.args.enproxy,
                    _types.append("HLS")
                    logger.debug(f"{self.premsg}[{info['format_id']}][get_dl] DL type HLS")

                else:
                    logger.error(
                        f"{self.premsg}[{info['format_id']}]:protocol not supported")
                    raise NotImplementedError("protocol not supported")

                if dl.auto_pasres:
                    self.info_dl.update({"auto_pasres": True})
                res_dl.append(dl)

            except Exception as e:
                logger.exception(f"{self.premsg}[{info['format_id']}] {repr(e)}")
                res_dl.append(AsyncErrorDownloader(info, repr(e)))

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
        if self.info_dl["status"] != "downloading":
            return
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
        if self.info_dl["status"] == "downloading" and not self.pause_event.is_set():
            self.pause_event.set()
            self.resume_event.clear()
            await asyncio.sleep(0)

    async def resume(self):
        if self.info_dl["status"] == "downloading" and not self.resume_event.is_set():
            self.resume_event.set()
            await asyncio.sleep(0)

    async def reinit(self):
        self._infodl.clear()
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
                tasks_run = [
                    self.add_task(dl.fetch_async(), name=f'fetch_async_{i}')
                    for i, dl in enumerate(self.info_dl["downloaders"])
                    if dl.status not in ("init_manipulating", "done")]

                logger.debug(f"{self.premsg}[run_dl] tasks run {len(tasks_run)}")

                done, _ = await asyncio.wait(tasks_run)

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
                    res = [dl.status for dl in self.info_dl["downloaders"]]

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
                "yt-dlp", "-P", self.info_dl['filename'].absolute().parent,
                "-o", f"{self.info_dl['filename'].stem}.%(ext)s",
                "--no-download", self.info_dict["webpage_url"]]

            return subprocess.run(cmd, encoding="utf-8", capture_output=True)

        if not (_subts := self.info_dict.get("requested_subtitles")):
            return
        _final_subts = {}
        for key, val in _subts.items():
            if key.startswith("es"):
                _final_subts['es'] = val
            elif key.startswith("en"):
                _final_subts['en'] = val
            elif key == "ca":
                _final_subts['ca'] = val
        if not _final_subts:
            return

        res = _dl_subt()

        logger.info(f"{self.premsg}: subs dl and converted to srt, rc[{res.returncode}]")

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

    async def run_manip(self):  # sourcery skip: comprehension-to-generator
        aget_subts_files = self.sync_to_async(self._get_subts_files)
        apostffmpeg = self.sync_to_async(self.syncpostffmpeg)
        armtree = self.sync_to_async(partial(shutil.rmtree, ignore_errors=True))
        amove = self.sync_to_async(shutil.move)
        autime = self.sync_to_async(os.utime)

        blocking_tasks = []

        try:

            self.info_dl["status"] = "manipulating"
            for dl in self.info_dl["downloaders"]:
                if dl.status == "init_manipulating":
                    dl.status = "manipulating"

            blocking_tasks = [
                self.add_task(dl.ensamble_file(), name=f'ensamble_file_{dl.premsg}')
                for dl in self.info_dl["downloaders"]
                if all(_ not in str(type(dl)).lower() for _ in ("aria2", "native"))
                and dl.status == "manipulating"]

            if self.args.subt and self.info_dict.get("requested_subtitles"):
                blocking_tasks += [self.add_task(aget_subts_files(), name='get_subts')]

            if blocking_tasks:
                if _excep := try_get(
                        await asyncio.wait(blocking_tasks),
                        lambda x: [d.exception() for d in x[0] if d.exception()] if x[0] else None):
                    for e in _excep:
                        logger.error(
                            f"{self.premsg}[run_manip] result de ensamble: {repr(e)}")

            res = True

            for dl in self.info_dl["downloaders"]:
                _exists = all([
                    await aiofiles.os.path.exists(_file)
                    for _file in variadic(dl.filename)])
                res = res and _exists and dl.status == "done"
                logger.debug(
                    f"{self.premsg} "
                    + f"{dl.filename} exists: [{_exists}] status: [{dl.status}]")

            if res:
                temp_filename = prepend_extension(str(self.info_dl["filename"]), "temp")

                rc = -1

                if self._types == "NATIVE_DRM":

                    _crypt_files = list(map(str, self.info_dl["downloaders"][0].filename))
                    _temp_files = list(map(lambda x: prepend_extension(x, "tmp"), _crypt_files))

                    _pssh = cast(str, try_get(
                        traverse_obj(self.info_dict, ("_drm", "pssh")),
                        lambda x: list(sorted(x, key=lambda y: len(y)))[0]))
                    _licurl = cast(str, traverse_obj(self.info_dict, ("_drm", "licurl")))

                    _key = None
                    if not _pssh or not _licurl or not (_key := self.get_key_drm(_licurl, _pssh)):
                        raise AsyncDLError(
                            f"{self.premsg}: error DRM info - licurl[{_licurl}] pssh[{_pssh}] key[{_key}]")

                    cmds = [
                        f"mp4decrypt --key {_key} {_crypt_file} {_temp_file}"
                        for _crypt_file, _temp_file in zip(_crypt_files, _temp_files)]

                    logger.info(f"{self.premsg}: starting decryption files")
                    logger.debug(f"{self.premsg}: {cmds}")

                    procs = [await apostffmpeg(_cmd) for _cmd in cmds]
                    rcs = [proc.returncode for proc in procs]
                    logger.info(f"{self.premsg}: end decryption files with result [{rcs}]")

                    if sum(rcs) == 0:
                        if len(_temp_files) == 1:
                            if await amove(
                                    _temp_files[0],
                                    temp_filename) == temp_filename:
                                rc = 0

                        else:
                            cmd = "".join([
                                f'ffmpeg -y -loglevel repeat+info -i file:"{_temp_files[0]}"',
                                f' -i file:"{_temp_files[1]}" -vcodec copy -acodec copy file:"{temp_filename}"'])
                            proc = await apostffmpeg(cmd)
                            logger.debug(
                                f"{self.premsg}: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                                + f"{proc.stdout}\n[stderr]{proc.stderr}")
                            rc = proc.returncode

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
                        logger.debug(f"{self.premsg}: DL video file OK")

                    for _file in _temp_files:
                        async with async_suppress(OSError):
                            await aiofiles.os.remove(_file)

                elif len(self.info_dl["downloaders"]) == 1:
                    # usamos ffmpeg para cambiar contenedor
                    # ts del DL de HLS de un sólo stream a mp4
                    if "ts" in self.info_dl["downloaders"][0].filename.suffix:
                        cmd = "".join([
                            "ffmpeg -y -probesize max -loglevel ",
                            f"repeat+info -i file:\"{str(self.info_dl['downloaders'][0].filename)}\"",
                            f' -c copy -map 0 -dn -f mp4 -bsf:a aac_adtstoasc file:"{temp_filename}"'])

                        proc = await apostffmpeg(cmd)
                        logger.debug(
                            f"{self.premsg}: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}")

                        rc = proc.returncode

                    else:
                        try:
                            if await amove(
                                    self.info_dl["downloaders"][0].filename,
                                    temp_filename) == temp_filename:
                                rc = 0
                        except Exception as e:
                            logger.exception(
                                f"{self.premsg}: error when manipulating {repr(e)}")

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
                        logger.debug(f"{self.premsg}: DL video file OK")

                    else:
                        self.info_dl["status"] = "error"
                        raise AsyncDLError(
                            f"{self.premsg}: error move file: {rc}")

                else:
                    cmd = "".join([
                        "ffmpeg -y -loglevel repeat+info -i file:",
                        f"\"{str(self.info_dl['downloaders'][0].filename)}\" -i file:",
                        f"\"{str(self.info_dl['downloaders'][1].filename)}\" -c copy -map 0:v:0 ",
                        "-map 1:a:0 -bsf:a:0 aac_adtstoasc ",
                        f'-movflags +faststart file:"{temp_filename}"'])

                    proc = await apostffmpeg(cmd)

                    logger.debug(
                        f"{self.premsg}:{cmd}"
                        + f": ffmpeg rc[{proc.returncode}]\n{proc.stdout}")

                    rc = proc.returncode

                    if rc == 0 and (await aiofiles.os.path.exists(temp_filename)):
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
                        raise AsyncDLError(
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
                        rc = proc.returncode
                        if rc == 0:
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

                        cmd = "".join([
                            "ffmpeg -y -loglevel repeat+info -i ",
                            f"file:\"{str(self.info_dl['filename'])}\" -map 0 -dn -ignore_unknown ",
                            f"-c copy -write_id3v1 1 -metadata 'comment={_meta}' -movflags +faststart ",
                            f'file:"{temp_filename}"'])

                        proc = await apostffmpeg(cmd)
                        logger.debug(
                            f"[{self.info_dict['id']}]"
                            + f"[{self.info_dict['title']}]: {cmd}\n[rc] {proc.returncode}\n[stdout]\n"
                            + f"{proc.stdout}\n[stderr]{proc.stderr}")

                        rc = proc.returncode
                        if rc == 0:
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
            return subprocess.run(
                shlex.split(cmd),
                encoding="utf-8",
                capture_output=True,
                timeout=120,
            )
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
