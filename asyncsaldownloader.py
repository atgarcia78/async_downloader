import logging
import asyncio
import subprocess
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote

from pathlib import Path

from yt_dlp.utils import (
    shell_quote,
    parse_filesize,
    smuggle_url
)

from utils import (
    naturalsize,
    ProxyYTDL,
    get_format_id,
    try_get,
    CONFIG_EXTRACTORS,
    limiter_non
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

    _CONFIG = CONFIG_EXTRACTORS.copy()
    progress_pattern = re.compile(
                r'Size complete:\s+(?P<download_str>[^\s]+)\s+/\s+([^\s]+)\s*\((?P<progress_str>[^\)]+)\).*' +
                r'Rate:\s+([^\s]+)\s*\:\s*(?P<speed_str>[^\s]+)\s*Remaining:\s+([^\s]+)\s*\:\s*(?P<eta_str>[^\s]+)\s*Duration')

    def __init__(self, video_dict, vid_dl):

        try:

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

            self.filesize = self.info_dict.get('filesize') or self._get_filesize()
            self.down_size = 0
            self.downsize_ant = 0
            self.dl_cont = [{'download_str': '--', 'speed_str': '--', 'eta_str': '--', 'progress_str': '--'}]

            self.status = 'init'
            self.error_message = ""

            self.ex_dl = ThreadPoolExecutor(thread_name_prefix='ex_saldl')

            def getter(x):
                if not x:
                    value, key_text = ('', '')
                else:
                    value, key_text = try_get(
                        [(v, sk) for k, v in self._CONFIG.items()
                            for sk in k if sk == x], lambda y: y[0]
                    ) or ('', '')

                if value:
                    limit = value['ratelimit'].ratelimit(key_text, delay=True)
                    maxplits = value['maxsplits']
                else:
                    limit = limiter_non.ratelimit('transp', delay=True)
                    maxplits = self.n_workers

                return (limit, maxplits)

            self._extractor = try_get(self.info_dict.get('extractor_key'), lambda x: x.lower())
            self._limit, self._conn = getter(self._extractor)

        except Exception as e:
            logger.exception(repr(e))
            raise

    def _make_cmd(self) -> str:
        cmd = ['saldl', self.info_dict['url'], '--resume', '-o', str(self.filename)]
        if self.info_dict.get('http_headers') is not None:
            for key, val in self.info_dict['http_headers'].items():
                cmd += ['-H', f'{key}: {val}']
        cmd += ['-c', str(self._conn), '-i', '0.1']
        return shell_quote(cmd)

    def _get_filesize(self) -> int:
        cmd = ['saldl', self.info_dict['url']]
        if self.info_dict.get('http_headers') is not None:
            for key, val in self.info_dict['http_headers'].items():
                cmd += ['-H', f'{key}: {val}']
        cmd += ['--get-info', 'file-size']
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

    async def event_handle(self) -> dict:

        _res = {}
        if (_event := [_ev.name for _ev in (self.vid_dl.reset_event, self.vid_dl.stop_event) if _ev.is_set()]):
            _res = {"event": _event[0]}
        return _res

    async def read_stream(self, aproc: asyncio.subprocess.Process):

        _buffer_prog = ""
        _buffer_stderr = ""

        try:
            assert aproc.stderr

            if aproc.returncode is not None:
                if (buffer := await aproc.stderr.read()):
                    _buffer_stderr = buffer.decode('utf-8')
                return _buffer_stderr

            while aproc.returncode is None:
                try:
                    _res = await self.event_handle()
                    if _res.get("event") in ("stop", "reset"):
                        logger.info(
                            f"[{self.info_dict['id']}][{self.info_dict['title']}][read stream] terminate proc {aproc}")
                        aproc.terminate()
                        await asyncio.sleep(0)
                        break
                    line = await aproc.stderr.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    await asyncio.sleep(0)
                    continue
                if line:
                    _line = line.decode('utf-8').replace('\n', ' ')
                    if not re.search(r'^\d+', _line):
                        # logger.info(f"{_line}")
                        _buffer_prog += _line
                        _buffer_stderr += _line
                        # logger.info(_buffer_prog)
                        if 'Download Finished' in _buffer_prog:
                            self.status = "done"
                            break
                        if (_prog_info := re.search(self.progress_pattern, _buffer_prog)):
                            _status = _prog_info.groupdict()
                            _dl_size = parse_filesize(_status['download_str'])
                            if _dl_size:
                                _status.update({'download_bytes': _dl_size})
                                self.down_size = _dl_size
                                self.vid_dl.info_dl['down_size'] += (self.down_size - self.downsize_ant)
                                self.downsize_ant = _dl_size
                            # logger.debug(_status)
                            self.dl_cont.append(_status)
                            _buffer_prog = ""
                            if _status['progress_str'] == '100.00%':
                                self.status = "done"
                                break
                    await asyncio.sleep(0)
            return _buffer_stderr
        except Exception as e:
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][read stream] {repr(e)}")
            raise

    async def _async_call_downloader(self):

        try:

            cmd = self._make_cmd()
            _proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await asyncio.sleep(0)
            _coros = [self.read_stream(_proc), _proc.wait()]

            results = await asyncio.gather(*_coros, return_exceptions=True)
            failed_results = [repr(result) for result in results if isinstance(result, Exception)]

            if failed_results:
                logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}][async call dl] {failed_results}")
                self.status = "error"
                return

            if results[1] == 1:
                logger.info(f"[{self.info_dict['id']}][{self.info_dict['title']}][async call dl] error proc: {results[0]}")
                if "exists, quiting..." in results[0]:
                    self.status = "done"
                    return
                else:
                    self.status = "error"
                    return

        except Exception as e:
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][read stream] {repr(e)}")

    async def fetch_async(self):

        self.status = "downloading"

        try:
            while True:
                task_dl = asyncio.create_task(self._async_call_downloader())

                done, _ = await asyncio.wait([task_dl])
                if self.vid_dl.stop_event.is_set():
                    self.status = "stop"
                    return
                elif self.vid_dl.reset_event.is_set():
                    self.vid_dl.reset_event.clear()
                    await self.update_uri()
                    continue
                elif self.status == "done":
                    logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: DL completed")
                    return
                elif self.status == "error":
                    return
        except Exception as e:
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}] error {repr(e)}")
            self.status = "error"

    def print_hookup(self):

        msg = ""

        try:

            if self.status == "done":
                msg = f'[SAL][{self.info_dict["format_id"]}]: Completed\n'
            elif self.status == "init":
                msg = f'[SAL][{self.info_dict["format_id"]}]: Waiting '
                msg += f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
            elif self.status == "error":
                msg = f'[SAL][{self.info_dict["format_id"]}]: ERROR ' +\
                    f'{naturalsize(self.down_size, format_=".2f")} ' +\
                    f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
            elif self.status == "downloading":
                if (_speed := self.dl_cont[-1].get('speed_str')) and _speed != '--':
                    _speed_bytes = parse_filesize(_speed)
                    _speed_str = f"{naturalsize(_speed_bytes)}ps"
                else:
                    _speed_str = '--'
                msg = f"[SAL]: DL[{_speed_str:>10}] PR[{self.dl_cont[-1].get('progress_str') or '--':>7}]"
                msg += f" ETA[{self.dl_cont[-1].get('eta_str') or '--':>6}]\n"

        except Exception as e:
            logger.exception(f"[{self.info_dict['id']}][{self.info_dict['title']}][print hookup] error {repr(e)}")

        return msg
