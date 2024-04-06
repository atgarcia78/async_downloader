import asyncio
import contextlib
import logging
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from threading import Lock
from typing import cast
from urllib.parse import unquote

from utils import (
    InfoDL,
    LockType,
    async_lock,
    get_host,
    getter_basic_config_extr,
    limiter_non,
    load_config_extractors,
    myYTDL,
    naturalsize,
    sync_to_async,
    traverse_obj,
    try_call,
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
            self.info_dict = video_dict
            self._vid_dl = info_dl
            self.ytdl = ytdl
            self.n_workers = self.args.parts
            self.download_path = self.info_dict["download_path"]
            self.download_path.mkdir(parents=True, exist_ok=True)

            if not (_req_fmts := self.info_dict.get("requested_formats")):
                self._formats = [self.info_dict]
                self._streams = {self.info_dict['format_id']: 'video'}
            else:
                self._formats = sorted(
                    _req_fmts,
                    key=lambda x: (x.get("resolution", "") == "audio_only" or x.get("ext", "") == "m4a"))
                self._streams = {self._formats[0]['format_id']: 'video', self._formats[1]['format_id']: 'audio'}

            self._filename = Path(self.info_dict.get("filename"))
            if not self.drm:
                self.filename = Path(
                    self.download_path, f'{self._filename.stem}.{self.info_dict["ext"]}')
            else:
                self.filename = [
                    Path(self.download_path, f'{self._filename.stem}.f{fdict["format_id"]}.{fdict["ext"]}')
                    for fdict in self._formats]

            self._host = get_host(unquote(self._formats[0]["url"]))
            self.down_size = 0
            self.down_size_old = 0

            self.error_message = ""

            self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_natdl")

            self.sync_to_async = partial(
                sync_to_async, thread_sensitive=False, executor=self.ex_dl)

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

            self.status = "init"

            self._file = 'video'
            self.dl_cont = {
                'video': {"progress": "--", "downloaded": "--", "speed": "--"},
                'audio': {"progress": "--", "downloaded": "--", "speed": "--"}}

            if (_filesize := (
                    self.info_dict.get('filesize') or self.info_dict.get('filesize_approx') or
                    (self.info_dict.get('duration') or 0) * (self.info_dict.get('tbr') or 0) * 1000 / 8)):
                self.filesize = _filesize

        except Exception as e:
            logger.exception(repr(e))
            raise

    def fetch(self):

        def my_hook(d):
            try:
                if d['status'] == 'downloading':
                    if self._file == 'video' and try_call(lambda: list(self._streams.keys())[1] in d['filename']):
                        self._file = 'audio'
                        self.down_size_old = 0
                    self.down_size += (_inc := d['downloaded_bytes'] - self.down_size_old)
                    self._vid_dl.total_sizes["down_size"] += _inc
                    self.down_size_old = d['downloaded_bytes']
                    self.dl_cont[self._file] |= {
                        'downloaded': d['downloaded_bytes'],
                        'speed': d['_speed_str'],
                        'progress': d['_percent_str']
                    }
            except Exception as e:
                logger.exception(f"{self.premsg}[fetch] {repr(e)}")

        try:
            opts_upt = {
                'allow_unplayable_formats': True,
                'skip_download': False,
                'format': '+'.join(list(self._streams.keys())),
                'progress_hooks': [my_hook],
                'paths': {'home': str(self.download_path)},
                'outtmpl': {'default': f'{self._filename.stem}.%(ext)s'}}

            with myYTDL(params=(self.ytdl.params | opts_upt), silent=True) as pytdl:
                pytdl.params['http_headers'] |= traverse_obj(self.info_dict, ("formats", 0, "http_headers"))
                if _cookies := traverse_obj(self.info_dict, ("formats", 0, "cookies")):
                    pytdl._load_cookies(_cookies, autoscope=False)
                _info_dict = pytdl.sanitize_info(pytdl.process_ie_result(self.info_dict, download=True))
            self._vid_dl.info_dict |= _info_dict
        except Exception as e:
            logger.exception(f"{self.premsg}[fetch] {repr(e)}")
            self.status = "error"

    async def fetch_async(self):
        self.status = "downloading"
        _pre = f"{self.premsg}[fetch_async]"

        try:
            async with async_lock(self.sem):
                await self.sync_to_async(self.fetch)()

            self.status = "done"
        except Exception as e:
            logger.exception(f"{_pre} {repr(e)}")
            self.status = "error"

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
                    _speed_str.append(_speed_meter)
                else:
                    _speed_str.append("--")
                if (_progress := _temp.get("progress", "--")) and _progress != "--":
                    _progress_str.append(f'{_progress}')
                else:
                    _progress_str.append("--")
            _msg = (
                f'[Native] Video DL [{_speed_str[0]}] '
                + f'PR [{_progress_str[0]}] {_now_str}\n'
            )
            if len(_temps) > 1:
                _msg += (
                    f'   [Native] Audio DL [{_speed_str[1]}] '
                    + f'PR [{_progress_str[1]}]\n'
                )
            return _msg

        try:
            if self.status == "done":
                msg = f'[Native] Completed {_now_str}\n'
            elif self.status == "init":
                msg = f'[Native] Waiting {_fsize_str} {_now_str}\n'
            elif self.status == "error":
                msg = (
                    f'[Native] ERROR {naturalsize(self.down_size, format_=".2f")} '
                    + f'{_fsize_str} {_now_str}\n'
                )
            elif self.status == "downloading":
                msg = _print_downloading()

            return msg

        except Exception as e:
            logger.exception(f"{self.premsg}[print hookup] error {repr(e)}")
