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

from utils import (
    InfoDL,
    LockType,
    async_lock,
    get_format_id,
    get_host,
    getter_basic_config_extr,
    limiter_non,
    load_config_extractors,
    myYTDL,
    naturalsize,
    sync_to_async,
    try_get,
)

logger = logging.getLogger("asyncyoutubedl")


class AsyncYoutubeDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncYoutubeDLError(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncYoutubeDownloader:
    _CLASSLOCK = asyncio.Lock()
    _CONFIG = load_config_extractors()

    def __init__(
        self,
        args: Namespace,
        ytdl: myYTDL,
        video_dict: dict,
        info_dl: InfoDL,
        drm=False,
    ):
        self.args = args
        self.drm = drm
        self.info_dict = video_dict
        self._vid_dl = info_dl
        self.ytdl = ytdl
        self.n_workers = self.args.parts
        self.download_path = self.info_dict["download_path"]
        self.download_path.mkdir(parents=True, exist_ok=True)

        self._streams = [
            get_format_id(self.info_dict, _fmtid)
            for _fmtid in self.info_dict.get("format_id").split("+")
        ]

        self._filename = Path(self.info_dict.get("filename"))
        if not self.drm:
            self.filename = Path(
                self.download_path, f'{self._filename.stem}.{self.info_dict["ext"]}'
            )
        else:
            self.filename = [
                Path(
                    self.download_path,
                    f'{self._filename.stem}.f{fdict["format_id"]}.{fdict["ext"]}',
                )
                for fdict in self._streams
            ]

        self.down_size = 0
        self.down_size_old = 0
        self.error_message = ""

        self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_natdl")

        self.sync_to_async = partial(
            sync_to_async, thread_sensitive=False, executor=self.ex_dl
        )

        self.special_extr = False

        def getter(x):
            value, key_text = getter_basic_config_extr(
                x, AsyncYoutubeDownloader._CONFIG
            ) or (None, None)

            if value and key_text:
                self.special_extr = True
                limit = value["ratelimit"].ratelimit(key_text, delay=True)
                maxplits = value["maxsplits"]
            else:
                limit = limiter_non.ratelimit("transp", delay=True)
                maxplits = self.n_workers

            return (limit, maxplits)

        self._extractor = try_get(
            self.info_dict.get("extractor_key"), lambda x: x.lower()
        )
        self.auto_pasres = False

        self._limit, self._conn = getter(self._extractor)
        if self._conn < 16:
            with self.ytdl.params.setdefault("lock", Lock()):
                self.ytdl.params.setdefault("sem", {})
                self.sem = cast(
                    LockType,
                    self.ytdl.params["sem"].setdefault(
                        get_host(self._streams[0]["url"]), Lock()
                    ),
                )
        else:
            self.sem = contextlib.nullcontext()

        self.premsg = f'[{self.info_dict["id"]}][{self.info_dict["title"]}]'

        self.status = "init"
        self._file = "video"

        self.dl_cont = {
            "video": {"progress": "--", "downloaded": "--", "speed": "--"}
        } | (
            {"audio": {"progress": "--", "downloaded": "--", "speed": "--"}}
            if len(self._streams) > 1
            else {}
        )

        self._avg_size = bool(self.info_dict.get("filesize"))
        if _filesize := (
            self.info_dict.get("filesize")
            or self.info_dict.get("filesize_approx")
            or (self.info_dict.get("duration") or 0)
            * (self.info_dict.get("tbr") or 0)
            * 1000
            / 8
        ):
            self.filesize = _filesize

    def fetch(self):
        if len(self._streams) > 1:
            _fmtid2 = self._streams[1]["format_id"]
            def _new_str(x):
                return (self._file == "video" and _fmtid2 in x)
        else:
            def _new_str(x):
                return False

        def my_hook(d):
            if d["status"] == "downloading":
                if _new_str(d["filename"]):
                    self.dl_cont[self._file] |= {"speed": "--", "progress": "100%"}
                    self._file = "audio"
                    self.down_size_old = 0
                    self.filesize_offset = self.filesize
                self.down_size += (_inc := d["downloaded_bytes"] - self.down_size_old)
                self._vid_dl.total_sizes["down_size"] += _inc
                self.down_size_old = d["downloaded_bytes"]
                if not self._avg_size and (_filesize := d.get("total_bytes_estimate")):
                    if self._file == "video":
                        self.filesize = self._vid_dl.total_sizes["filesize"] = _filesize
                    else:
                        self.filesize = self._vid_dl.total_sizes["filesize"] = (
                            self.filesize_offset + _filesize
                        )

                self.dl_cont[self._file] |= {
                    "downloaded": d["downloaded_bytes"],
                    "speed": d["_speed_str"],
                    "progress": d["_percent_str"],
                }

        try:
            opts_upt = {
                "keepvideo": self.drm,
                "allow_unplayable_formats": self.drm,
                "concurrent_fragment_downloads": self.n_workers,
                "skip_download": False,
                "format": self.info_dict["format_id"],
                "progress_hooks": [my_hook],
                "paths": {"home": str(self.download_path)},
                "outtmpl": {"default": f"{self._filename.stem}.%(ext)s"},
            } | ({"postprocessors": []} if self.drm else {})

            with myYTDL(params=(self.ytdl.params | opts_upt), silent=True) as pytdl:
                pytdl.params["http_headers"] |= (
                    self._streams[0].get("http_headers") or {}
                )
                if _cookies_str := self._streams[0].get("cookies"):
                    pytdl._load_cookies(_cookies_str, autoscope=False)
                _info_dict = pytdl.sanitize_info(
                    pytdl.process_ie_result(
                        self.info_dict | ({"subtitles": {}} if self.drm else {}),
                        download=True,
                    )
                )

            _info_dict.pop("requested_subtitles", None)
            _info_dict.pop("subtitles", None)
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
            for _temp in self.dl_cont.values():
                if (_speed_meter := _temp.get("speed", "--")) and _speed_meter != "--":
                    _speed_str.append(_speed_meter)
                else:
                    _speed_str.append("--")
                if (_progress := _temp.get("progress", "--")) and _progress != "--":
                    _progress_str.append(f"{_progress}")
                else:
                    _progress_str.append("--")
            _msg = (
                f"[Youtube] Video DL [{_speed_str[0]}] "
                + f"PR [{_progress_str[0]}] {_now_str}\n"
            )
            if len(_speed_str) > 1:
                _msg += (
                    f"   [Youtube] Audio DL [{_speed_str[1]}] "
                    + f"PR [{_progress_str[1]}]\n"
                )
            return _msg

        try:
            if self.status == "done":
                msg = f"[Youtube] Completed {_now_str}\n"
            elif self.status == "init":
                msg = f"[Youtube] Waiting {_fsize_str} {_now_str}\n"
            elif self.status == "error":
                msg = (
                    f'[Youtube] ERROR {naturalsize(self.down_size, format_=".2f")} '
                    + f"{_fsize_str} {_now_str}\n"
                )
            elif self.status == "downloading":
                msg = _print_downloading()

            return msg

        except Exception as e:
            logger.exception(f"{self.premsg}[print hookup] error {repr(e)}")
