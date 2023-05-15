import logging
import asyncio
import subprocess
import re
import contextlib
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
from typing import cast
from threading import Lock
from pathlib import Path
from datetime import timedelta

from yt_dlp.utils import (
    shell_quote,
    parse_filesize,
    smuggle_url
)

from utils import (
    naturalsize,
    ProxyYTDL,
    SmoothETA,
    SpeedometerMA,
    get_format_id,
    try_get,
    load_config_extractors,
    getter_basic_config_extr,
    limiter_non,
    get_host,
    async_lock,
    async_wait_for_any,
    MySyncAsyncEvent,
    CONF_AUTO_PASRES
)

logger = logging.getLogger("async_SALDL_DL")


class AsyncSALDLErrorFatal(Exception):
    """Error during info extraction."""

    def __init__(self, msg, exc_info=None):

        super().__init__(msg)

        self.exc_info = exc_info


class AsyncSALDLError(Exception):
    """Error during info extraction."""

    def __init__(self, msg, exc_info=None):

        super().__init__(msg)

        self.exc_info = exc_info


class AsyncSALDownloader():

    _CLASSALOCK = asyncio.Lock()
    _CONFIG = load_config_extractors()
    progress_pattern = re.compile(
                r'Size complete:\s+(?P<download_str>[^\s]+)\s+/\s+([^\s]+)\s*\((?P<progress_str>[^\)]+)\).*' +
                r'Rate:\s+([^\s]+)\s*\:\s*(?P<speed_str>[^\s]+)\s*Remaining:\s+([^\s]+)\s*\:\s*(?P<eta_str>[^\s]+)\s*Duration')

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
            self.filename = Path(
                self.download_path,
                f'{_filename.stem}.{self.info_dict["format_id"]}.{self.info_dict["ext"]}')

            self._host = get_host(unquote(self.info_dict.get('url')))
            self.down_size = 0
            self.downsize_ant = 0
            self.dl_cont = [{'download_str': '--', 'speed_str': '--', 'eta_str': '--', 'progress_str': '--'}]

            self.error_message = ""

            self.ex_dl = ThreadPoolExecutor(thread_name_prefix='ex_saldl')

            def getter(x):
                value, key_text = getter_basic_config_extr(x, AsyncSALDownloader._CONFIG) or (None, None)

                if value and key_text:
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
                    self.sem = cast(Lock, self.ytdl.params['sem'].setdefault(self._host, Lock()))
            else:
                self.sem = contextlib.nullcontext()

            self.filesize = self.info_dict.get('filesize') or self._get_filesize()

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
        cmd = ['saldl', self.info_dict['url'], '--resume', '-o', str(self.filename)]
        if self.info_dict.get('http_headers') is not None:
            for key, val in self.info_dict['http_headers'].items():
                cmd += ['-H', f'{key}: {val}']
        cmd += ['-c', str(self._conn), '-i', '0.05']
        return shell_quote(cmd)

    def _get_filesize(self) -> int:
        cmd = ['saldl', self.info_dict['url']]
        if self.info_dict.get('http_headers') is not None:
            for key, val in self.info_dict['http_headers'].items():
                cmd += ['-H', f'{key}: {val}']
        cmd += ['--get-info', 'file-size']
        with self.sem:
            with self._limit:
                filesize_str = subprocess.run(cmd, encoding='utf-8', capture_output=True).stdout.strip('\n')
        logger.debug(f'filesize: {filesize_str}')
        return int(filesize_str)

    async def update_uri(self):
        _init_url = self.info_dict.get('webpage_url')
        _init_url = smuggle_url(_init_url, {'indexdl': self.vid_dl.index})
        _ytdl_opts = self.ytdl.params.copy()
        async with ProxyYTDL(opts=_ytdl_opts, executor=self.ex_dl) as proxy_ytdl:

            proxy_info = get_format_id(
                proxy_ytdl.sanitize_info(
                    await proxy_ytdl.async_extract_info(_init_url)),
                self.info_dict['format_id'])

        if (_url := proxy_info.get("url")):
            self.info_dict['url'] = unquote(_url)
            self._host = get_host(unquote(_url))

    async def async_terminate(self, pid, msg=None):
        premsg = '[async_term]'
        if msg:
            premsg += f' {msg}'
        logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]{premsg} terminate proc {self._proc[pid]}")
        async with AsyncSALDownloader._CLASSALOCK:
            self._proc[pid].terminate()
            await asyncio.sleep(0)
        await asyncio.wait(self._tasks[pid])

    async def async_start(self, msg=None):
        async with AsyncSALDownloader._CLASSALOCK:
            async with self._limit:
                proc = await asyncio.create_subprocess_shell(
                    self._make_cmd(), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                await asyncio.sleep(0)
                self._proc[proc.pid] = proc
                self._tasks[proc.pid] = [self.add_task(self.read_stream(proc)), self.add_task(proc.wait())]
                return proc.pid

    async def event_handle(self, pid) -> dict:
        _res = {}
        if self.vid_dl.pause_event.is_set():
            await self.async_terminate(pid, "pause")
            await asyncio.sleep(0)
            _events = [self.vid_dl.resume_event, self.vid_dl.reset_event, self.vid_dl.stop_event]
            _print_ev = ' '.join([f"{_ev.name}[{_ev.is_set()}]" for _ev in _events])
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}][ev_handle] wait for event: {_print_ev}")
            _res = await async_wait_for_any(_events)

            await asyncio.sleep(0)
            _print_ev = ' '.join([f"{_ev.name}[{_ev.is_set()}]" for _ev in _events])
            logger.debug(
                f"[{self.info_dict['id']}][{self.info_dict['title']}][ev_handle] end wait for event: {_res}, status: {_print_ev}")
            if (event := _res.get("event")):
                if 'stop' in event:
                    self.status = 'stop'
                    await asyncio.sleep(0)
                    return {"event": ["stop"]}

                else:
                    await self.update_uri()
                    await asyncio.sleep(0)
                    return {"event": ["reset"]}

        elif (_event := [_ev.name for _ev in (self.vid_dl.reset_event, self.vid_dl.stop_event, self.ready_check)
                         if _ev.is_set()]):

            _res = {"event": _event}
            if 'readycheck' in _event:
                done, _ = await asyncio.wait(self._tasks[pid])
                failed_results = [repr(result.exception()) for result in done if result.exception()]
                if failed_results:
                    self.status = "error"
                    await asyncio.sleep(0)
                    return {"error": failed_results}
                if self.status == "done":
                    await asyncio.sleep(0)
                    return {"done": "done"}
                results = [result.result() for result in done]
                if results[1] == 0:
                    self.status = "done"
                    await asyncio.sleep(0)
                    return {"done": "done"}
                else:
                    if results[1] == 1 and "exists, quiting..." in results[0]:
                        self.status = "done"
                        await asyncio.sleep(0)
                        return {"done": "exists, quiting..."}
                    else:
                        self.status = "error"
                        await asyncio.sleep(0)
                        return {"error": f"returncode[{results[1]}] {results[0]}"}

            if any([_ in _event for _ in ('reset', 'stop')]):
                await self.async_terminate(pid, str(_event))
                await asyncio.sleep(0)
                if 'stop' in _event:
                    self.status = 'stop'
                    await asyncio.sleep(0)
                    return {"event": ["stop"]}

                elif 'reset' in _event:
                    await self.update_uri()
                    await asyncio.sleep(0)
                    return {"event": ["reset"]}
        return _res

    async def read_stream(self, proc: asyncio.subprocess.Process):

        _buffer_prog = ""
        _buffer_stderr = ""

        try:
            assert proc and proc.stderr

            if proc.returncode is not None:
                if (buffer := await proc.stderr.read()):  # type: ignore
                    _buffer_stderr = buffer.decode('utf-8')
                return _buffer_stderr

            while proc.returncode is None:
                try:
                    line = await proc.stderr.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    await asyncio.sleep(0)
                    continue
                if line:
                    _line = re.sub(r'\n', ' ', line.decode())
                    if not re.match(r'\d+', _line):
                        _buffer_prog += _line
                        _buffer_stderr += _line
                        if 'Download Finished' in _buffer_prog:
                            self.status = "done"
                            break
                        if (_prog_info := re.search(self.progress_pattern, _buffer_prog)):
                            _status = _prog_info.groupdict()
                            _dl_size = parse_filesize(_status['download_str'])
                            if _dl_size:
                                _speed_meter = self.speedometer(_dl_size)
                                _status.update(
                                    {'download_bytes': _dl_size, 'speed_meter': _speed_meter})
                                if _speed_meter and self.filesize:
                                    _est_time = (self.filesize - _dl_size) / _speed_meter
                                    _est_time_smooth = self.smooth_eta(_est_time)
                                    _status.update({'eta_smooth': _est_time_smooth})
                                self.down_size = _dl_size
                                self.vid_dl.info_dl['down_size'] += (self.down_size - self.downsize_ant)
                                self.downsize_ant = _dl_size
                            self.dl_cont.append(_status)
                            _buffer_prog = ""
                            if _status['progress_str'] == '100.00%':
                                self.status = "done"
                                break
                    await asyncio.sleep(0)
                else:
                    break
            return _buffer_stderr
        except Exception as e:
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][read stream] {repr(e)}")
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
                            if (done := (_res.get('done') or ("ok" if self.status == "done" else None))):
                                logger.debug(
                                    f"[{self.info_dict['id']}][{self.info_dict['title']}][fetch_async] DL completed {done}")
                                return
                            if (error := (_res.get('error') or ("error" if self.status == "error" else None))):
                                logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][fetch_async] {error}")
                                return
                            if any([self.status == "stop", 'stop' in _res.get("event", [''])]):
                                return
                            if 'reset' in _res.get("event", ['']):
                                break
                            await asyncio.sleep(0)
                    except Exception as e:
                        logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}] inner error {repr(e)}")
                        self.status = "error"
                        return
                    finally:
                        await asyncio.wait(self._tasks[pid])
        except Exception as e:
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}] error {repr(e)}")
            self.status = "error"

    def print_hookup(self):

        msg = ""
        try:
            if self.status == "done":
                msg = f'[SAL][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}] Completed\n'
            elif self.status == "init":
                msg = f'[SAL][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}] Waiting '
                msg += f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
            elif self.status == "error":
                msg = f'[SAL][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}] ERROR ' +\
                    f'{naturalsize(self.down_size, format_=".2f")} ' +\
                    f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
            elif self.status == "downloading":
                if not any(
                    [self.vid_dl.reset_event.is_set(), self.vid_dl.stop_event.is_set(), self.vid_dl.pause_event.is_set()]
                ) and self.dl_cont:

                    _temp = self.dl_cont[-1].copy()
                    if (_speed_meter := _temp.get('speed_meter')) and _speed_meter != '--':
                        _speed_str = f"{naturalsize(_speed_meter)}ps"
                    else:
                        _speed_str = '--'
                    if ((_eta_smooth := cast(float, _temp.get('eta_smooth'))) and _eta_smooth < 3600):

                        _eta_smooth_str = ":".join(
                            [_item.split(".")[0] for _item in f"{timedelta(seconds=_eta_smooth)}".split(":")[1:]])
                    else:
                        _eta_smooth_str = "--"
                    msg = f"[SAL][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}] DL[{_speed_str:>10}] "
                    msg += f"PR[{_temp.get('progress_str') or '--':>7}] ETA[{_eta_smooth_str:>6}]\n"
                else:
                    _substr = 'UNKNOWN'
                    if self.vid_dl.pause_event.is_set():
                        _substr = 'PAUSED'
                    elif self.vid_dl.reset_event.is_set():
                        _substr = 'RESET'
                    elif self.vid_dl.stop_event.is_set():
                        _substr = 'STOPPED'

                    msg = f"[SAL][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}] {_substr} "
                    msg += f"DL PR[{self.dl_cont[-1].get('progress_str', '--') if self.dl_cont else '--':>7}]\n"

        except Exception as e:
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][print hookup] error {repr(e)}")

        return msg
