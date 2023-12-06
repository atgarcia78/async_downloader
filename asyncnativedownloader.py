import asyncio
import contextlib
import logging
import re
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import cast
from urllib.parse import unquote

from yt_dlp.utils import int_or_none, shell_quote

from utils import (
    CONF_INTERVAL_GUI,
    InfoDL,
    LockType,
    MySyncAsyncEvent,
    ProgressTimer,
    async_lock,
    get_host,
    getter_basic_config_extr,
    limiter_non,
    load_config_extractors,
    myYTDL,
    naturalsize,
    try_get,
)

logger = logging.getLogger("async_native")


class AsyncNativeDLErrorFatal(Exception):
    """Error during info extraction."""

    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncNativeDLError(Exception):
    """Error during info extraction."""

    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncNativeDownloader:
    _CLASSLOCK = asyncio.Lock()
    _CONFIG = load_config_extractors()

    def __init__(self, args: Namespace, ytdl: myYTDL, video_dict: dict, info_dl: InfoDL, drm=False):
        try:
            self.args = args
            self.drm = drm
            self.background_tasks = set()
            self.info_dict = video_dict.copy()
            self._vid_dl = info_dl
            self.ytdl = ytdl
            self.n_workers = self.args.parts
            self.download_path = self.info_dict["download_path"]
            self.download_path.mkdir(parents=True, exist_ok=True)
            self._filename = self.info_dict.get(
                "_filename", self.info_dict.get("filename"))

            if not (_req_fmts := self.info_dict.get("requested_formats")):
                _formats = [self.info_dict]
                self._streams = {self.info_dict['format_id']: 'video'}
            else:
                _formats = sorted(
                    _req_fmts,
                    key=lambda x: (x.get("resolution", "") == "audio_only" or x.get("ext", "") == "m4a"))
                self._streams = {_formats[0]['format_id']: 'video', _formats[1]['format_id']: 'audio'}

            if not self.drm:
                self.filename = Path(
                    self.download_path, f'{self._filename.stem}.{self.info_dict["ext"]}')
            else:
                self.filename = [
                    Path(self.download_path, f'{self._filename.stem}.f{fdict["format_id"]}.{fdict["ext"]}')
                    for fdict in _formats]

            self._host = get_host(unquote(_formats[0]["url"]))
            self.down_size = 0
            self.down_size_old = 0

            self.error_message = ""

            self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_natdl")

            self.special_extr = False

            def getter(x):
                value, key_text = getter_basic_config_extr(
                    x, AsyncNativeDownloader._CONFIG) or (None, None)

                if value and key_text:
                    self.special_extr = True
                    limit = value["ratelimit"].ratelimit(key_text, delay=True)
                    maxplits = value["maxsplits"]
                else:
                    limit = limiter_non.ratelimit("transp", delay=True)
                    maxplits = self.n_workers

                return (limit, maxplits)

            self._extractor = try_get(
                self.info_dict.get("extractor_key"), lambda x: x.lower())
            self.auto_pasres = False

            self._limit, self._conn = getter(self._extractor)
            if self._conn < 16:
                with self.ytdl.params.setdefault("lock", Lock()):
                    self.ytdl.params.setdefault("sem", {})
                    self.sem = cast(
                        LockType, self.ytdl.params["sem"].setdefault(self._host, Lock()))
            else:
                self.sem = contextlib.nullcontext()

            self.premsg = f'[{self.info_dict["id"]}][{self.info_dict["title"]}]'
            self.ready_check = MySyncAsyncEvent("readycheck")

            self.status = "init"

            # for parsing output ffmpeg
            _output_progress = r" - ".join([
                r"Progress\:\s*(?P<progress>\d+\.\d+)%",
                r"Downloaded\:\s*(?P<downloaded>\d+)",
                r"Speed\:\s*(?P<speed>(?:\d+(\.\d+)?|NA))"])
            if len(_formats) == 1:
                fmt = _formats[0]
                _pat_fmt = rf"{fmt['format_id']}"
                _pat_ext = rf"{fmt['ext']}"
            else:
                fmt0, fmt1 = _formats
                _pat_fmt = rf"(?:{fmt0['format_id']}|{fmt1['format_id']})"
                _pat_ext = rf"(?:{fmt0['ext']}|{fmt1['ext']})"
            _file_start = rf"\[download\]\s*Destination\:\s*.+\.f(?P<fmt>{_pat_fmt})\.{_pat_ext}$"
            self.progress_pattern = re.compile(rf"(?:({_output_progress})|({_file_start}))")

            self._buffer = []
            self._file = None
            self.dl_cont = {
                'video': {"progress": "--", "downloaded": "--", "speed": "--"},
                'audio': {"progress": "--", "downloaded": "--", "speed": "--"}}

            if (_filesize := (
                    self.info_dict.get('filesize') or self.info_dict.get('filesize_approx') or
                    self.info_dict.get('duration', 0) * self.info_dict.get('vbr', 0) * 1000 / 8)):
                self.filesize = _filesize

        except Exception as e:
            logger.exception(repr(e))
            raise

    def add_task(self, coro):
        _task = asyncio.create_task(coro)
        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    def _make_cmd(self) -> str:
        cmd = [
            "yt-dlp", "-P", str(self.download_path), "-o", f"{self._filename.stem}.%(ext)s",
            "-f", '+'.join(list(self._streams.keys())), self.info_dict["webpage_url"],
            "-v", "-N", str(self.n_workers), "--downloader", "native", "--newline", "--progress-template",
            " - ".join([
                "Progress:%(progress._percent_str)s",
                "Downloaded:%(progress.downloaded_bytes)s",
                "Speed:%(progress.speed)s"])
        ]
        if self.drm:
            cmd.append("--allow-unplayable-formats")
        _cmd = f'{shell_quote(cmd)} | egrep "(Destination|Progress)"'
        logger.info(f"{self.premsg}[cmd] {_cmd}")
        return _cmd

    async def async_terminate(self, pid, msg=None):
        premsg = "[async_term]"
        if msg:
            premsg += f" {msg}"
        logger.debug(f"{self.premsg}{premsg} terminate proc {self._proc[pid]}")
        async with AsyncNativeDownloader._CLASSLOCK:
            if self._proc[pid].returncode is None:
                self._proc[pid].terminate()
                await asyncio.sleep(0)
                await asyncio.wait(self._tasks[pid])

    async def async_start(self, msg=None):
        async with AsyncNativeDownloader._CLASSLOCK:
            async with self._limit:
                proc = await asyncio.create_subprocess_shell(
                    self._make_cmd(), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                await asyncio.sleep(0)
                self._proc[proc.pid] = proc
                self._tasks[proc.pid] = [self.add_task(self.read_stream(proc)), self.add_task(proc.wait())]
                return proc.pid

    async def event_handle(self, pid) -> dict:

        _events = [self._vid_dl.reset_event, self._vid_dl.stop_event, self.ready_check]
        _res = {}
        if _event := [_ev.name for _ev in _events if _ev.is_set()]:
            _res |= {"event": _event}
            if "readycheck" in _event:
                await asyncio.wait(self._tasks[pid])
                if (_rc := self._proc[pid].returncode) == 0:
                    self.status = "done"
                    await asyncio.sleep(0)
                    return {"status": "done"}
                else:
                    self.status = "error"
                    await asyncio.sleep(0)
                    return {"status": f"error returncode[{_rc}]"}
            else:
                await self.async_terminate(pid, str(_event))
                await asyncio.sleep(0)
                if "stop" in _event:
                    self.status = "stop"
                await asyncio.sleep(0)

        return _res

    def _parse(self, line):
        _line = line.decode("utf-8").strip(" \n")
        return try_get(
            re.search(self.progress_pattern, _line),
            lambda x: x.groupdict() if x else None,
        )

    def _parse_output(self, line):

        if not (_res := self._parse(line)):
            return
        if _fmt := _res.pop('fmt'):
            if self._file:
                self.dl_cont[self._file]["speed"] = "--"
            self.down_size_old = 0
            self._file = self._streams.get(_fmt)

        elif self._file:
            if (_dl_size := int_or_none(_res.get("downloaded"))) is not None:
                self.down_size += (_inc := _dl_size - self.down_size_old)
                self._vid_dl.total_sizes["down_size"] += _inc
                self.down_size_old = _dl_size
                self.dl_cont[self._file] |= _res

    async def read_stream(self, proc: asyncio.subprocess.Process):

        try:
            if proc.returncode is not None or not proc.stdout:
                if proc.stderr and (buffer := await proc.stderr.read()):  # type: ignore
                    self._buffer.append(buffer.decode("utf-8"))
                    self.status = "error"
                    return
            while proc.returncode is None:
                try:
                    if line := await proc.stdout.readline():
                        self._parse_output(line)
                except (asyncio.LimitOverrunError, ValueError) as e:
                    logger.exception(f"{self.premsg}[read stream] {repr(e)}")
                finally:
                    await asyncio.sleep(0)

        except Exception as e:
            logger.exception(f"{self.premsg}[read stream] {repr(e)}")
            raise
        finally:
            self.ready_check.set()

    async def fetch_async(self):
        self.status = "downloading"
        self._proc = {}
        self._tasks = {}
        _pre = f"{self.premsg}[fetch_async]"
        progress_timer = ProgressTimer()

        while True:
            async with async_lock(self.sem):
                self.ready_check.clear()
                self._vid_dl.clear()
                pid = await self.async_start()
                progress_timer.reset()
                try:
                    while True:
                        if progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):
                            _res = await self.event_handle(pid)
                            if self.status in ("done", "stop", "error") or "status" in _res:
                                logger.debug(
                                    f"{_pre} status[{self.status}] event_handle[{_res}] "
                                    + f"buffer:\n{self._buffer}")
                                return
                            if "event" in _res:
                                logger.debug(f"{_pre} {_res['event']}")
                                break
                        await asyncio.sleep(0)
                except Exception as e:
                    logger.exception(f"{_pre} inner error {repr(e)}")
                    self.status = "error"
                    return
                finally:
                    await asyncio.wait(self._tasks[pid])

    def print_hookup(self):
        msg = ""
        _now_str = datetime.now().strftime("%H:%M:%S")
        _fsize_str = f'[{naturalsize(self.filesize, format_=".2f") if hasattr(self, "filesize") else "NA"}]'

        def _print_downloading():
            _speed_str = []
            _progress_str = []
            _temps = [self.dl_cont[_file].copy() for _file in list(self._streams.values())]
            for _temp in _temps:
                if (_speed_meter := _temp.get("speed", "--")) and _speed_meter != "--":
                    _speed_str.append(f"{naturalsize(float(_speed_meter), binary=True)}ps")
                else:
                    _speed_str.append("--")
                if (_progress := _temp.get("progress", "--")) and _progress != "--":
                    _progress_str.append(f'{_progress}%')
                else:
                    _progress_str.append("--")
            _msg = (
                f'[Native] HOST[{self._host.split(".")[0]}] Video DL [{_speed_str[0]}] '
                + f'PR [{_progress_str[0]}] {_now_str}\n'
            )
            if len(_temps) > 1:
                _msg += (
                    f'   [Native] HOST[{self._host.split(".")[0]}] Audio DL [{_speed_str[1]}] '
                    + f'PR [{_progress_str[1]}]\n'
                )
            return _msg

        try:
            if self.status == "done":
                msg = f'[Native] HOST[{self._host.split(".")[0]}] Completed {_now_str}\n'
            elif self.status == "init":
                msg = f'[Native] HOST[{self._host.split(".")[0]}] Waiting '
                msg += f'{_fsize_str} {_now_str}\n'
            elif self.status == "error":
                msg = (
                    f'[Native] HOST[{self._host.split(".")[0]}] ERROR '
                    + f'{naturalsize(self.down_size, format_=".2f")} '
                    + f'{_fsize_str} {_now_str}\n'
                )
            elif self.status == "downloading":
                msg = _print_downloading()

            return msg

        except Exception as e:
            logger.exception(f"{self.premsg}[print hookup] error {repr(e)}")
