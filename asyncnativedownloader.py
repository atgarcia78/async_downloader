import logging
import asyncio
import re
import contextlib
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
from typing import cast
from threading import Lock
from pathlib import Path
from datetime import datetime

from yt_dlp.utils import (
    shell_quote
)

from utils import (
    naturalsize,
    SmoothETA,
    SpeedometerMA,
    try_get,
    traverse_obj,
    load_config_extractors,
    getter_basic_config_extr,
    limiter_non,
    get_host,
    async_lock,
    MySyncAsyncEvent,
    CONF_AUTO_PASRES
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


class AsyncNativeDownloader():

    _CLASSLOCK = asyncio.Lock()
    _CONFIG = load_config_extractors()

    def __init__(self, video_dict, vid_dl):

        try:
            self.background_tasks = set()
            self.info_dict = video_dict.copy()
            self.vid_dl = vid_dl
            self.ytdl = self.vid_dl.info_dl['ytdl']
            self.n_workers = self.vid_dl.info_dl['n_workers']
            self.download_path = self.info_dict['download_path']
            self.download_path.mkdir(parents=True, exist_ok=True)
            _filename = self.info_dict.get('_filename', self.info_dict.get('filename'))
            _formats = sorted(
                self.info_dict['requested_formats'],
                key=lambda x: (x.get('resolution', '') == "audio_only" or x.get('ext', '') == "m4a"))
            #  1 video, 2 audio
            self.filename = [Path(
                self.download_path,
                f'{_filename.stem}.f{fdict["format_id"]}.{fdict["ext"]}')
                for fdict in _formats]

            self._host = get_host(unquote(_formats[0]['url']))
            self.down_size = 0
            self.downsize_ant = 0
            self.dl_cont = [{'download_str': '--', 'speed_str': '--', 'eta_str': '--', 'progress_str': '--'}]

            self.error_message = ""

            self.ex_dl = ThreadPoolExecutor(thread_name_prefix='ex_saldl')

            self.special_extr = False

            def getter(x):
                value, key_text = getter_basic_config_extr(x, AsyncNativeDownloader._CONFIG) or (None, None)

                if value and key_text:
                    self.special_extr = True
                    limit = value['ratelimit'].ratelimit(key_text, delay=True)
                    maxplits = value['maxsplits']
                else:
                    limit = limiter_non.ratelimit('transp', delay=True)
                    maxplits = self.n_workers

                return (limit, maxplits)

            self._extractor = try_get(self.info_dict.get('extractor_key'), lambda x: x.lower())
            self.auto_pasres = False
            if self._extractor in CONF_AUTO_PASRES:
                self.auto_pasres = True

            self._limit, self._conn = getter(self._extractor)
            if self._conn < 16:
                with self.ytdl.params.setdefault('lock', Lock()):
                    self.ytdl.params.setdefault('sem', {})
                    self.sem = cast(type(Lock()), self.ytdl.params['sem'].setdefault(self._host, Lock()))
            else:
                self.sem = contextlib.nullcontext()

            self.premsg = f'[{self.info_dict["id"]}][{self.info_dict["title"]}]'
            self.ready_check = MySyncAsyncEvent("readycheck")

            self.status = 'init'

        except Exception as e:
            logger.exception(repr(e))
            raise

    def add_task(self, coro):
        _task = asyncio.create_task(coro)
        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    def _make_cmd(self) -> str:
        cmd = ['yt-dlp', '-P', str(self.download_path), '-o', '%(id)s_%(title)s.%(ext)s',
               self.info_dict['webpage_url'], '-v', '--allow-unplayable-formats', '-N', '50', '--downloader', 'native']
        return shell_quote(cmd)

    async def async_terminate(self, pid, msg=None):
        premsg = '[async_term]'
        if msg:
            premsg += f' {msg}'
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
        _res = {}

        if (_event := [
            _ev.name for _ev in (self.vid_dl.reset_event, self.vid_dl.stop_event, self.ready_check)
                if _ev.is_set()]):

            _res = {"event": _event}
            if 'readycheck' in _event:
                await asyncio.wait(self._tasks[pid])
                if self.status == "done":
                    await asyncio.sleep(0)
                    return {"status": "done"}
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
                if 'stop' in _event:
                    self.status = 'stop'
                await asyncio.sleep(0)

        return _res

    async def read_stream(self, proc: asyncio.subprocess.Process):

        _buffer = ""

        try:
            assert proc and proc.stderr

            if proc.returncode is not None:
                if (buffer := await proc.stderr.read()):  # type: ignore
                    _buffer = buffer.decode('utf-8')
                return _buffer

            while proc.returncode is None:
                try:
                    line = await proc.stderr.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    await asyncio.sleep(0)
                else:
                    if line:
                        _line = re.sub(r'\n', ' ', line.decode())
                        _buffer += _line
                        await asyncio.sleep(0)
                    else:
                        break

            return _buffer

        except Exception as e:
            logger.exception(f"{self.premsg}[read stream] {repr(e)}")
            raise
        finally:
            self.ready_check.set()

    async def fetch_async(self):

        self.status = "downloading"
        self._proc = {}
        self._tasks = {}
        self.speedometer = SpeedometerMA(initial_bytes=self.down_size)
        self.smooth_eta = SmoothETA()

        try:
            while True:
                async with async_lock(self.sem):

                    self.ready_check.clear()
                    pid = await self.async_start()
                    self.vid_dl.reset_event.clear()
                    self.vid_dl.pause_event.clear()
                    self.vid_dl.resume_event.clear()
                    try:
                        while True:
                            _res = await self.event_handle(pid)
                            if (_status := traverse_obj(_res, (('status', 'event'),))):
                                logger.debug(f"{self.premsg}[fetch_async] {_status}")
                                return
                            elif self.status in ("stop", "error"):
                                logger.debug(f"{self.premsg}[fetch_async] {self.status}")
                                return

                            await asyncio.sleep(0)
                    except Exception as e:
                        logger.exception(f"{self.premsg}[fetch_async] inner error {repr(e)}")
                        self.status = "error"
                        return
                    finally:
                        await asyncio.wait(self._tasks[pid])
        except Exception as e:
            logger.error(f"{self.premsg}[fetch_async] error {repr(e)}")
            self.status = "error"

    def print_hookup(self):

        msg = ""
        _now_str = datetime.now().strftime('%H:%M:%S')
        try:
            if self.status == "done":
                msg = f'[Native] HOST[{self._host.split(".")[0]}] Completed {_now_str}\n'
            elif self.status == "init":
                msg = f'[Native] HOST[{self._host.split(".")[0]}] Waiting '
                msg += f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}] {_now_str}\n'
            elif self.status == "error":
                msg = f'[Native] HOST[{self._host.split(".")[0]}] ERROR ' +\
                    f'{naturalsize(self.down_size, format_=".2f")} ' +\
                    f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}] {_now_str}\n'
            elif self.status == "downloading":
                msg = f'[Native] HOST[{self._host.split(".")[0]}] Downloading {_now_str}\n'

        except Exception as e:
            logger.exception(f"{self.premsg}[print hookup] error {repr(e)}")

        return msg
