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

import aiofiles.os
import xattr
from yt_dlp.utils import sanitize_filename

from asyncaria2cdownloader import AsyncARIA2CDownloader
from asynchlsdownloader import AsyncHLSDownloader
from asynchttpdownloader import AsyncHTTPDownloader
from asyncyoutubedownloader import AsyncYoutubeDownloader
from utils import (
    AsyncDLError,
    Coroutine,
    InfoDL,
    MySyncAsyncEvent,
    Optional,
    Tracker,
    Union,
    async_suppress,
    get_drm_xml,
    get_format_id,
    get_protocol,
    get_pssh_from_manifest,
    myYTDL,
    naturalsize,
    prepend_extension,
    sync_to_async,
    translate_srt,
    traverse_obj,
    try_get,
    variadic,
    get_metadata_video,
    get_metadata_video_subt
)

logger = logging.getLogger("videodl")


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

    def __init__(self, video_dict, ytdl, nwsetup, args):
        self.background_tasks = set()
        self.args = args

        self._index = None

        self.info_dict = video_dict

        _date_file = datetime.now().strftime("%Y%m%d")

        if not self.args.path:
            _base = _date_file
            if self.args.use_path_pl:
                _pltitle = try_get(
                    self.info_dict.get("playlist")
                    or self.info_dict.get("playlist_title"),
                    lambda x: sanitize_filename(x, restricted=True))
                _plid = self.info_dict.get("playlist_id")
                if _pltitle and _plid:
                    _base = f"{_plid}_{_pltitle}"
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
            sync_to_async, thread_sensitive=False, executor=self.ex_videodl
        )

        _title = sanitize_filename(self.info_dict["title"], restricted=True)

        self._status_manip_upt = {"progress": 0.0}

        self.info_dl = {
            "id": self.info_dict["id"],
            "n_workers": self.args.parts,
            "rpcport": self.args.rpcport,
            "auto_pasres": False,
            "webpage_url": self.info_dict.get("webpage_url"),
            "title": _title,
            "_title_print": self.info_dict.get("_title_print", _title),
            "ytdl": ytdl,
            "date_file": _date_file,
            "download_path": _download_path,
            "filename": Path(
                _download_path.parent,
                f'{self.info_dict["id"]}_{_title}.{self.info_dict.get("ext", "mp4")}',
            ),
            "error_message": "",
            "nwsetup": nwsetup,
        }

        self.total_sizes = {"filesize": 0, "down_size": 0}

        self._infodl = InfoDL(
            self.pause_event,
            self.resume_event,
            self.stop_event,
            self.end_tasks,
            self.reset_event,
            self.total_sizes,
            nwsetup,
            self.info_dict,
        )

        self._types = ""
        downloaders = []

        downloaders.extend(
            variadic(
                self._get_dl(
                    self.info_dict
                    | {
                        "filename": self.info_dl["filename"],
                        "download_path": self.info_dl["download_path"],
                    }
                )
            )
        )

        _all_status = [dl.status for dl in downloaders]
        if "error" in _all_status:
            _status = "error"
        elif all(el in ("done", "init_manipulating") for el in _all_status):
            _status = "init_manipulating"
        else:
            _status = "init"
        _filesize = sum(getattr(dl, "filesize", 0) or 0 for dl in downloaders)
        _down_size = sum(getattr(dl, "down_size", 0) or 0 for dl in downloaders)
        self.total_sizes |= {"filesize": _filesize, "down_size": _down_size}

        self.info_dl |= {
            "downloaders": downloaders,
            "downloaded_subtitles": {},
            "filesize": _filesize,
            "down_size": _down_size,
            "status": _status,
            "sub_status": None,
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

    def _get_dl(self, info_dict: dict):
        _drm = self.args.drm and (
            bool(info_dict.get("_has_drm")) or bool(info_dict.get("has_drm")))
        _protocol = get_protocol(info_dict)

        if (
            self.args.downloader_ytdl
            or _drm or _protocol == "dash"
            or info_dict.get("extractor_key") == "Youtube"
        ):
            try:
                dl = AsyncYoutubeDownloader(
                    self.args, self.info_dl["ytdl"], info_dict, self._infodl, drm=_drm)
                self._types = f"YTDL_DRM-{_protocol}" if _drm else f"YTDL-{_protocol}"
                logger.debug(f"{self.premsg}[get_dl] DL type: {self._types}")
                return dl
            except Exception as e:
                logger.error(
                    f"{self.premsg}[{info_dict['format_id']}] Error in init DL")
                return AsyncErrorDownloader(info_dict, repr(e))

        if not (_info := info_dict.get("requested_formats")):
            _info = [info_dict]
        else:
            for f in _info:
                f.update(
                    {
                        "id": info_dict["id"],
                        "title": info_dict["title"],
                        "filename": info_dict["filename"],
                        "download_path": info_dict["download_path"],
                        "original_url": info_dict.get("original_url"),
                        "webpage_url": info_dict.get("webpage_url"),
                        "extractor_key": info_dict.get("extractor_key"),
                        "extractor": info_dict.get("extractor"),
                    }
                )

        res_dl = []
        _types = []
        for info in _info:
            try:
                type_protocol = get_protocol(info)
                if type_protocol in ("http", "https"):
                    if self.args.aria2c:
                        dl = AsyncARIA2CDownloader(
                            self.args, self.info_dl["ytdl"], info, self._infodl)
                        _types.append("ARIA2")
                        logger.debug(
                            f"{self.premsg}[{info['format_id']}][get_dl] DL type ARIA2C")
                    else:
                        dl = AsyncHTTPDownloader(info, self)
                        _types.append("HTTP")
                        logger.debug(
                            f"{self.premsg}[{info['format_id']}][get_dl] DL type HTTP")

                elif type_protocol in ("m3u8", "m3u8_native"):
                    dl = AsyncHLSDownloader(
                        self.args, self.info_dl["ytdl"], info, self._infodl)
                    _types.append("HLS")
                    logger.debug(
                        f"{self.premsg}[{info['format_id']}][get_dl] DL type HLS")

                else:
                    logger.error(
                        f"{self.premsg}[{info['format_id']}]:protocol not supported")
                    raise NotImplementedError("protocol not supported")

                if dl.auto_pasres:
                    self.info_dl.update({"auto_pasres": True})
                res_dl.append(dl)

            except Exception as e:
                logger.error(f"{self.premsg}[{info['format_id']}] Error in init DL")
                res_dl.append(AsyncErrorDownloader(info, repr(e)))

        self._types = " - ".join(_types)
        return res_dl

    def add_task(
        self, coro: Union[Coroutine, asyncio.Task], *, name: Optional[str] = None
    ) -> asyncio.Task:
        if not isinstance(coro, asyncio.Task):
            _task = asyncio.create_task(coro, name=name)
        else:
            _task = coro
        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    async def change_numvidworkers(self, n: int):
        if self.info_dl["status"] in ("downloading", "init"):
            for dl in self.info_dl["downloaders"]:
                dl.n_workers = n
                if "aria2" in str(type(dl)).lower():
                    dl.opts.set("split", dl.n_workers)
            await self.reset(cause="hard")
            logger.info(f"{self.premsg}: workers set to {n}")

    async def reset_from_console(self):
        """
        when reset from console, if in pause the reset is declyned
        to ease managing the pauses of dls
        """
        # if self.pause_event.is_set():
        #     return []
        return await self.reset(cause="hard", wait=False)

    async def reset(self, cause: Optional[str] = None, wait=True):
        if self.info_dl["status"] != "downloading":
            return []
        _wait_tasks = []
        if not self.reset_event.is_set():
            logger.debug(f"{self.premsg}[reset] {cause}")
            self.reset_event.set(cause)
            await asyncio.sleep(0)
            if self.pause_event.is_set():
                await asyncio.sleep(0)
        elif cause == "403":
            self.reset_event.set(cause)

        _wait_tasks = []
        for dl in self.info_dl["downloaders"]:
            if "asynchls" in str(type(dl)).lower():
                _wait_tasks.extend(await dl._handle_reset(cause=cause))

        if wait and _wait_tasks:
            await asyncio.wait(_wait_tasks)

        return _wait_tasks

    async def stop(self, cause: Optional[str] = None, wait=True):
        if self.info_dl["status"] != "downloading":
            return
        try:
            logger.debug(f"{self.premsg}[stop]")
            self.info_dl["status"] = "stop"
            for dl in self.info_dl["downloaders"]:
                dl.status = "stop"

            self.stop_event.set(cause)
            await asyncio.sleep(0)
            if self.pause_event.is_set():
                await asyncio.sleep(0)

            if cause == "exit":
                _wait_tasks = []
                for dl in self.info_dl["downloaders"]:
                    if "asynchls" in str(type(dl)).lower() and getattr(
                        dl, "tasks", None
                    ):
                        if _tasks := [
                            _task
                            for _task in dl.tasks
                            if not _task.done()
                            and not _task.cancelled()
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
        if (
            self.info_dl["status"] == "downloading"
            and not self.pause_event.is_set()
            and not self.reset_event.is_set()
        ):
            self.pause_event.set()
            self.resume_event.clear()
            await asyncio.sleep(0)

    async def resume(self):
        if self.info_dl["status"] == "downloading" and self.pause_event.is_set():
            self.resume_event.set()
            await asyncio.sleep(0)

    async def reinit(self):
        self._infodl.clear()
        self.info_dl["status"] = "init"
        await asyncio.sleep(0)
        for dl in self.info_dl["downloaders"]:
            dl.status = "init"
            if "aria2" not in str(type(dl)).lower() and hasattr(dl, "update_uri"):
                await dl.update_uri()
                if "hls" in str(type(dl)).lower():
                    await self.sync_to_async(dl.init)()

    async def run_dl(self):
        self.info_dl["ytdl"].params["stop_dl"][str(self.index)] = self.stop_event
        logger.debug(
            f"{self.premsg}: [run_dl] "
            + f"[stop_dl] {self.info_dl['ytdl'].params['stop_dl']}"
        )

        try:
            if self.info_dl["status"] == "init":
                self.info_dl["status"] = "downloading"
                logger.debug(
                    f"{self.premsg}[run_dl] status"
                    + f"{[dl.status for dl in self.info_dl['downloaders']]}"
                )

                for i, dl in enumerate(self.info_dl["downloaders"]):
                    if dl.status not in ("init_manipulating", "done"):
                        _task = self.add_task(dl.fetch_async(), name=f"fetch_async_{i}")
                        if _excep := try_get(
                            await asyncio.wait([_task]),
                            lambda x: {
                                d.exception(): d._name for d in x[0] if d.exception()
                            }
                            if x[0]
                            else None,
                        ):
                            for error, label in _excep.items():
                                logger.error(
                                    f"{self.premsg}[run_dl] task[{label}]: {repr(error)}"
                                )

                if self.stop_event.is_set():
                    logger.debug(
                        f"{self.premsg}[run_dl] end run with stop - {self.info_dl['status']}"
                    )
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
                            ]
                        )

                    else:
                        self.info_dl["status"] = "init_manipulating"

        except Exception as e:
            logger.exception(f"{self.premsg}[run_dl] error when DL {repr(e)}")
            self.info_dl["status"] = "error"

    def _get_subts_files(self):
        def _dl_subt(subs):
            opts_upt = {
                "skip_download": True,
                "keepvideo": self.args.keep_videos,
                "format": self.info_dict["format_id"],
                "paths": {"home": str(self.info_dl["filename"].absolute().parent)},
                "outtmpl": {"default": f'{self.info_dl["filename"].stem}.%(ext)s'},
            }
            with myYTDL(
                params=(self.info_dl["ytdl"].params | opts_upt), silent=True
            ) as pytdl:
                _fmt = get_format_id(
                    self.info_dict,
                    try_get(self.info_dict.get("format_id"), lambda x: x.split("+")[0]),
                )
                pytdl.params["http_headers"] |= _fmt.get("http_headers") or {}
                if _cookies_str := _fmt.get("cookies"):
                    pytdl._load_cookies(_cookies_str, autoscope=False)
                _subs = {_lang: [_val] for _lang, _val in subs.items()}
                _info_dict = pytdl.sanitize_info(
                    pytdl.process_ie_result(
                        self.info_dict | {"subtitles": _subs}, download=True
                    )
                )
            return _info_dict

        try:
            if not (_subts := self.info_dict.get("requested_subtitles")):
                return

            _final_subts = {
                key: val
                for key, val in _subts.items()
                if any([key == "es", key == "en", key == "ca"])
            }

            # ytdl.params["subtitleslangs"]: ["all"]
            for _lang in ["es", "en"]:
                if _lang not in _final_subts:
                    for key, val in _subts.items():
                        if key.startswith(_lang):
                            _final_subts[_lang] = val
                            break

            logger.debug(f"{self.premsg}[get_subts] final_subts: {_final_subts}")
            if not _final_subts:
                return
            _info = _dl_subt(_final_subts)
            logger.debug(
                f"{self.premsg}[get_subts] info_dict_subt\n{_info.get('requested_subtitles')}"
            )
        except Exception as e:
            logger.exception(f"{self.premsg}[get_subts] {repr(e)}")

        for _lang in _final_subts:
            _subts_file = traverse_obj(
                _info, ("requested_subtitles", _lang, "filepath")
            )
            try:
                _exists = Path(_subts_file).exists() if _subts_file else False
                _size = Path(_subts_file).stat().st_size if _exists else -1
                logger.debug(
                    f"{self.premsg}[get_subts][{_lang}] file: {_subts_file} exists[{_exists}] size[{_size}]"
                )
                if _size > 0:
                    self.info_dl["downloaded_subtitles"][_lang] = _subts_file
                else:
                    raise Exception("error with subt file")
            except Exception as e:
                logger.exception(
                    f"{self.premsg}[get_subts][{_lang}]  couldnt generate subtitle file: {repr(e)}"
                )
                if _subts_file:
                    try:
                        os.remove(_subts_file)
                    except OSError:
                        pass

        if (
            "ca" in self.info_dl["downloaded_subtitles"]
            and "es" not in self.info_dl["downloaded_subtitles"]
        ):
            logger.debug(
                f"{self.premsg}: subs will translate from [ca, srt] to [es, srt]"
            )
            _subs_file = self.info_dl["downloaded_subtitles"]["ca"].replace(
                ".ca.srt", ".es.srt"
            )
            try:
                with open(_subs_file, "w") as f:
                    f.write(
                        translate_srt(
                            self.info_dl["downloaded_subtitles"]["ca"], "ca", "es"
                        )
                    )
                if Path(_subs_file).exists():
                    self.info_dl["downloaded_subtitles"]["es"] = _subs_file
                    logger.debug(f"{self.premsg}: subs file [es, srt] ready")
            except Exception as e:
                logger.exception(
                    f"{self.premsg}[get_subts] couldnt translate subtitle file from ca to es: {repr(e)}"
                )

    def _get_drm_xml(self) -> str:
        if not (_licurl := traverse_obj(self.info_dict, ("_drm", "licurl"))):
            raise AsyncDLError(f"{self.premsg}: error DRM info")
        if not (
            _pssh := try_get(
                traverse_obj(self.info_dict, ("_drm", "pssh")),
                lambda x: sorted(x, key=len)[0] if x else None,
            )
        ):
            _video_fmt_id = try_get(
                self.info_dict.get("format_id"), lambda x: x.split("+")[0]
            )
            _fmt = get_format_id(self.info_dict, _video_fmt_id)
            if _murl := _fmt.get("manifest_url"):
                kwargs = {"headers": _fmt.get("http_headers") or {}}
                if _cookies_str := _fmt.get("cookies"):
                    kwargs |= {"cookies": _cookies_str}
                _pssh = try_get(
                    get_pssh_from_manifest(manifest_url=_murl, **kwargs), lambda x: x[0]
                )

        logger.debug(f"{self.premsg} licurl[{_licurl}] - murl[{_murl}] pssh[{_pssh}]")
        if not _pssh:
            raise AsyncDLError(f"{self.premsg}: error DRM info")
        _func_validate = None
        if "onlyfans" in self.info_dict["extractor_key"].lower():
            ie = self.info_dl["ytdl"].get_extractor("OnlyFansPost")
            _func_validate = ie.validate_drm_lic
        _path_drm_file = str(Path(self.info_dl["download_path"], "drm.xml"))
        _keys = get_drm_xml(
            _licurl, _path_drm_file, pssh=_pssh, func_validate=_func_validate
        )
        logger.debug(f"{self.premsg}: drm keys[{_keys}] drm file[{_path_drm_file}]")
        return _path_drm_file

    def run_proc_tracker(self, cmd, queue, pattern):
        _tracker = Tracker(cmd, queue, pattern)
        return _tracker.track_progress()

    def run_proc(self, cmd):
        _cmd = None
        try:
            _cmd = shlex.split(cmd)
            return subprocess.run(
                _cmd,
                encoding="utf-8",
                capture_output=True,
                timeout=120,
            )
        except Exception as e:
            return subprocess.CompletedProcess(_cmd, 1, stdout=None, stderr=repr(e))

    async def run_manip(self):
        aget_subts_files = self.sync_to_async(self._get_subts_files)
        arunproc = self.sync_to_async(self.run_proc)
        arunproctracker = self.sync_to_async(self.run_proc_tracker)
        armtree = self.sync_to_async(partial(shutil.rmtree, ignore_errors=True))
        autime = self.sync_to_async(os.utime)
        aget_metadata_video = self.sync_to_async(get_metadata_video)

        async def check_files_exists():
            rc = True
            for dl in self.info_dl["downloaders"]:
                _exists = all(
                    [
                        await aiofiles.os.path.exists(_file)
                        for _file in variadic(dl.filename)
                    ]
                )
                rc = rc and _exists and dl.status == "done"
                logger.debug(
                    f"{self.premsg} "
                    + f"{dl.filename} exists: [{_exists}] status: [{dl.status}]"
                )

            if not rc:
                self.info_dl["status"] = "error"
                raise AsyncDLError(
                    f"{self.premsg}: error missing files from downloaders"
                )

        async def amove(orig, dst):
            rc = -1
            msg_error = ""
            _amove = self.sync_to_async(shutil.move)
            try:
                rc = try_get(await _amove(orig, dst), lambda x: 0 if (x == dst) else -1)
                if rc == -1:
                    msg_error = "result of move incorrect"
            except Exception as e:
                msg_error = {str(e)}
            if rc == -1:
                logger.error(
                    f"{self.premsg}: error move " + f"{orig} to {dst} - {msg_error}"
                )
            return rc

        async def embed_subt():
            try:
                embed_filename = prepend_extension(self.temp_filename, "embed")

                def _make_embed_gpac_cmd():
                    _part_cmd = " -add ".join(
                        [
                            f"{_file}:lang={_lang}:hdlr=sbtl"
                            for _lang, _file in self.info_dl[
                                "downloaded_subtitles"
                            ].items()
                        ]
                    )
                    return f"MP4Box -flat -add {_part_cmd} {self.temp_filename} -out {embed_filename}"

                logger.info(f"{self.premsg}: starting embed subt")
                proc = await arunproc(cmd := _make_embed_gpac_cmd())
                logger.debug(
                    f"{self.premsg}: subts embeded\n[cmd] {cmd}\n[rc] {proc.returncode}"
                )

                if (
                    (proc.returncode) == 0
                    and (await aiofiles.os.path.exists(embed_filename))
                    and await amove(embed_filename, self.temp_filename) == 0
                ):
                    logger.debug(f"{self.premsg}: subt embeded OK")
                    if not self.args.keep_videos:
                        for _file in self.info_dl["downloaded_subtitles"].values():
                            async with async_suppress(OSError):
                                await aiofiles.os.remove(_file)
                else:
                    logger.warning(f"{self.premsg}: error embeding subtitles")
                    async with async_suppress(OSError):
                        await aiofiles.os.remove(embed_filename)

            except Exception as e:
                logger.exception(f"{self.premsg}: error embeding subtitles {repr(e)}")

        async def embed_metadata():
            try:
                meta_filename = prepend_extension(self.temp_filename, "meta")
                _metadata = (
                    f"title={self.info_dict.get('title')}:"
                    + f"online_info={self.info_dict.get('webpage_url')}"
                )
                if _meta := self.info_dict.get("meta_comment"):
                    _metadata += f":comment={_meta}"

                cmd = f"MP4Box -flat -itags {_metadata} {self.temp_filename} -out {meta_filename}"
                logger.info(f"{self.premsg}: starting embed metadata")
                proc = await arunproc(cmd)
                logger.debug(
                    f"{self.premsg} embed metadata\n[cmd] {cmd}\n[rc] {proc.returncode}"
                )

                if not (
                    (proc.returncode) == 0
                    and (await aiofiles.os.path.exists(meta_filename))
                    and (await amove(meta_filename, self.temp_filename)) == 0
                ):
                    logger.warning(f"{self.premsg}: error embedding metadata")
                    async with async_suppress(OSError):
                        await aiofiles.os.remove(meta_filename)
                if _meta:
                    async with async_suppress(
                        Exception,
                        level=logging.WARNING,
                        logger=logger,
                        msg=f"{self.premsg}: error setxattr",
                    ):
                        xattr.setxattr(
                            str(self.temp_filename),
                            "user.dublincore.description",
                            _meta.encode(),
                        )

            except Exception as e:
                logger.exception(f"{self.premsg}: error in xattr area {repr(e)}")

        async def youtube_drm():
            rc = -1
            _crypt_files = list(map(str, self.info_dl["downloaders"][0].filename))
            _drm_xml = self._get_drm_xml()
            cmd = f"MP4Box -flat -decrypt {_drm_xml} -add {' -add '.join(_crypt_files)} -new {self.temp_filename} -proglf -logs=all@info:ncl"
            logger.info(f"{self.premsg}: starting decryption files")
            logger.debug(f"{self.premsg}: {cmd}")

            _rc = await arunproctracker(
                cmd, self._status_manip_upt, r"Decrypting:\s+(?P<progress>\S+)\s"
            )
            logger.debug(f"{self.premsg}: decrypt ends\n[cmd] {cmd}\n[rc] {rc}")

            if _rc == 0 and (await aiofiles.os.path.exists(self.temp_filename)):
                logger.debug(f"{self.premsg}: DL video file OK")
                rc = 0
                if not self.args.keep_videos:
                    for _file in _crypt_files:
                        async with async_suppress(OSError):
                            await aiofiles.os.remove(_file)
            if rc != 0:
                logger.error(f"{self.premsg}: error decryption files")
                self.info_dl["status"] = "error"
                raise AsyncDLError(f"{self.premsg}: error error decryption files")

        self.info_dl["status"] = "manipulating"

        blocking_tasks = {}

        try:
            blocking_tasks = {
                self.add_task(
                    dl.ensamble_file(), name=f"ensamble_file_{dl.premsg}"
                ): f"ensamble_file_{dl.premsg}"
                for dl in self.info_dl["downloaders"]
                if dl.status == "init_manipulating"
            }
            if blocking_tasks:
                self.info_dl["sub_status"] = "Ensambling file"
            if (
                self.args.subt
                and self.info_dict.get("requested_subtitles")
                and self._types != "YTD"
            ):
                blocking_tasks |= {
                    self.add_task(aget_subts_files(), name="get_subts"): "get_subs"
                }

            logger.debug(f"{self.premsg}[run_manip] blocking tasks\n{blocking_tasks}")

            if blocking_tasks and (
                _excep := try_get(
                    await asyncio.wait(list(blocking_tasks.keys())),
                    lambda x: {
                        d.exception(): blocking_tasks[d] for d in x[0] if d.exception()
                    },
                )
            ):
                for error, label in _excep.items():
                    logger.error(
                        f"{self.premsg}[run_manip] task[{label}]: {repr(error)}"
                    )

            logger.debug(f"{self.premsg}[run_manip] done blocking tasks")

            await check_files_exists()

            self.temp_filename = prepend_extension(
                str(self.info_dl["filename"]), "temp"
            )

            if self._types.startswith("YTDL_DRM"):
                self.info_dl["sub_status"] = "Decrypting files"
                await youtube_drm()

            elif len(self.info_dl["downloaders"]) == 1:
                # ts del DL de HLS de un sólo stream a mp4
                if "ts" in self.info_dl["downloaders"][0].filename.suffix:
                    cmd = (
                        "ffmpeg -y -probesize max -loglevel "
                        + f"repeat+info -i file:\"{str(self.info_dl['downloaders'][0].filename)}\""
                        + f' -c copy -map 0 -dn -f mp4 -bsf:a aac_adtstoasc -movflags +faststart file:"{self.temp_filename}"'
                    )
                    self.info_dl["sub_status"] = "Converting ts to mp4"
                    proc = await arunproc(cmd)
                    logger.debug(f"{self.premsg}: {cmd}\n[rc] {proc.returncode}")

                    rc = proc.returncode

                elif "matroska" in traverse_obj((_metainfo := await aget_metadata_video(str(self.info_dl['downloaders'][0].filename))), ('format', 'format_name')):
                    _subtl_cmd = ''
                    for _lang in (('en', 'eng'), ('es', 'spa')):
                        if _lang[0] not in self.info_dl["downloaded_subtitles"] and get_metadata_video_subt(_lang[1], _metainfo):
                            _subtl_cmd += f'-map 0:s:m:language:{_lang[1]} '
                    cmd = (
                        "ffmpeg -y -probesize max -loglevel "
                        + f"repeat+info -i file:\"{str(self.info_dl['downloaders'][0].filename)}\""
                        + f' -c copy -c:s mov_text -map 0:v -map 0:a {_subtl_cmd}-movflags +faststart file:"{self.temp_filename}"'
                    )
                    self.info_dl["sub_status"] = "Converting mkv to mp4"
                    proc = await arunproc(cmd)
                    logger.debug(f"{self.premsg}: {cmd}\n[rc] {proc.returncode}")

                    rc = proc.returncode

                else:
                    rc = await amove(
                        self.info_dl["downloaders"][0].filename, self.temp_filename
                    )

                if rc == 0 and (await aiofiles.os.path.exists(self.temp_filename)):
                    logger.debug(f"{self.premsg}: DL video file OK")
                else:
                    self.info_dl["status"] = "error"
                    raise AsyncDLError(f"{self.premsg}: error move file: {rc}")

            else:
                cmd = (
                    "ffmpeg -y -loglevel repeat+info -i file:"
                    + f"\"{str(self.info_dl['downloaders'][0].filename)}\" -i file:"
                    + f"\"{str(self.info_dl['downloaders'][1].filename)}\" -c copy -map 0:v:0 "
                    + f'-map 1:a:0 -bsf:a:0 aac_adtstoasc -movflags +faststart file:"{self.temp_filename}"'
                )
                self.info_dl["sub_status"] = "Merging streams"
                proc = await arunproc(cmd)

                logger.debug(f"{self.premsg}: {cmd}\n[rc] {proc.returncode}")

                if (proc.returncode) == 0 and (
                    await aiofiles.os.path.exists(self.temp_filename)
                ):
                    if not self.args.keep_videos:
                        for dl in self.info_dl["downloaders"]:
                            for _file in variadic(dl.filename):
                                async with async_suppress(OSError):
                                    await aiofiles.os.remove(_file)

                    logger.debug(
                        f"{self.premsg}: Streams merged for: {self.info_dl['filename']}"
                    )
                    logger.debug(f"{self.premsg}: DL video file OK")

                else:
                    self.info_dl["status"] = "error"
                    raise AsyncDLError(
                        f"{self.premsg}: error merge, ffmpeg error: {rc}"
                    )

            if self._types != "YOUTUBE" and self.info_dl["downloaded_subtitles"]:
                self.info_dl["sub_status"] = "Embeding subt"
                await embed_subt()

            if self._types != "YOUTUBE" and self.args.xattr:
                self.info_dl["sub_status"] = "Embeding metadata"
                await embed_metadata()

            if (
                await amove(self.temp_filename, self.info_dl["filename"])
            ) == 0 and self.info_dl["filename"].exists():
                self.info_dl["status"] = "done"
                if mtime := self.info_dict.get("release_timestamp"):
                    async with async_suppress(OSError):
                        await autime(
                            self.info_dl["filename"],
                            (int(datetime.now().timestamp()), mtime),
                        )

            else:
                self.info_dl["status"] = "error"
                async with async_suppress(OSError):
                    await aiofiles.os.remove(self.temp_filename)
                raise AsyncDLError(
                    f"{self.premsg}[{str(self.info_dl['filename'])}] doesn't exist"
                )

        except Exception as e:
            logger.exception(f"{self.premsg} error when manipulating {repr(e)}")
            self.info_dl["status"] = "error"
            if blocking_tasks:
                for t in blocking_tasks:
                    t.cancel()
                await asyncio.wait(blocking_tasks)
            raise
        finally:
            if not self.args.keep_videos:
                async with async_suppress(OSError):
                    await armtree(self.info_dl["download_path"])
            await asyncio.sleep(0)

    def print_hookup(self):
        def _pre(max_len=10):
            return f"[{self.index}][{self.info_dict['id']}][{self.info_dl['_title_print'][:max_len]:{max_len}}]"

        def _get_msg():
            msg = ""
            for dl in self.info_dl["downloaders"]:
                msg += f"  {dl.print_hookup()}"
            msg += "\n"
            return msg

        def _filesize_str():
            return f"{naturalsize(self.total_sizes['filesize'], format_='.2f')}"

        def _progress_dl():
            return f"{naturalsize(self.total_sizes['down_size'], format_='.2f')} {_filesize_str()}"

        def _status_manip():
            _status_manip = self.info_dl["sub_status"] or "Waiting for info"
            msg = _status_manip + "\n"
            if _status_manip == "Decrypting files":
                _progr = self._status_manip_upt.get("progress")
                msg += f"\tPR[{_progr:5.1f}%]"
            return msg

        if self.info_dl["status"] == "done":
            if not (_size_str := getattr(self, "_size_str", None)):
                self._size_str = f"{naturalsize(self.info_dl['filename'].stat().st_size, format_='.2f')}"
            return f"{_pre()} Completed [{_size_str}]\n {_get_msg()}\n"
        elif self.info_dl["status"] == "init":
            return f"{_pre()} Waiting to DL [{_filesize_str()}]\n {_get_msg()}\n"
        elif self.info_dl["status"] == "init_manipulating":
            return (
                f"{_pre()} Waiting to create file [{_filesize_str()}]\n {_get_msg()}\n"
            )
        elif self.info_dl["status"] == "error":
            return f"{_pre()} ERROR {_progress_dl()}\n {_get_msg()}\n"
        elif self.info_dl["status"] == "stop":
            return f"{_pre()} STOPPED {_progress_dl()}\n {_get_msg()}\n"
        elif self.info_dl["status"] == "downloading":
            if self.pause_event.is_set() and not self.resume_event.is_set():
                status = "PAUSED"
            else:
                status = "Downloading"
            return f"{_pre(max_len=40)} {status} {_progress_dl()}\n {_get_msg()}\n"
        elif self.info_dl["status"] == "manipulating":
            return f"{_pre()} {_status_manip()}\n"
