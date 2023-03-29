import asyncio
import contextlib
import copy
from datetime import (
    timedelta
)
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from shutil import rmtree
import aiofiles
import aiofiles.os as os
import httpx

from utils import (
    ProgressTimer,
    SmoothETA,
    SpeedometerMA,
    int_or_none,
    naturalsize,
    none_to_zero,
    try_get,
    traverse_obj,
    async_lock,
    limiter_non,
    dec_retry_error,
    CONFIG_EXTRACTORS,
    CONF_INTERVAL_GUI,
    get_domain,
    smuggle_url,
    my_dec_on_exception,
    get_format_id,
    sync_to_async,
    myYTDL,
    async_waitfortasks
)

from functools import partial

from threading import Lock

from urllib.parse import unquote

logger = logging.getLogger("async_http_DL")


class AsyncHTTPDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):

        super().__init__(msg)

        self.exc_info = exc_info


class AsyncHTTPDLError(Exception):
    def __init__(self, msg, exc_info=None):

        super().__init__(msg)

        self.exc_info = exc_info


retry = my_dec_on_exception(Exception, max_tries=3, raise_on_giveup=True,
                            interval=1)


class AsyncHTTPDownloader:

    _MIN_SIZE = 10485760  # 10MB
    _CHUNK_SIZE = 102400  # 100KB
    _MAX_RETRIES = 10
    _CONFIG = CONFIG_EXTRACTORS.copy()

    def __init__(self, video_dict, vid_dl):

        if not video_dict or not vid_dl:
            return
        self.background_tasks = set()
        self.info_dict = video_dict.copy()
        self.vid_dl = vid_dl

        self.video_url = self.info_dict["url"]

        self.uris = [unquote(self.video_url)]

        self.ytdl: myYTDL = self.vid_dl.info_dl["ytdl"]

        #  self.proxies = None

        self.verifycert = False
        self.timeout = httpx.Timeout(20, connect=20)

        self.limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)

        self.headers = self.info_dict.get("http_headers")

        self.init_client = httpx.Client(
            limits=self.limits,
            follow_redirects=True,
            timeout=self.timeout,
            verify=self.verifycert,
            headers=self.headers,
        )

        self.base_download_path = self.info_dict.get("download_path")
        if _filename := self.info_dict.get("_filename"):
            self.download_path = Path(
                self.base_download_path, self.info_dict["format_id"]
            )
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.filename = Path(
                self.base_download_path,
                _filename.stem
                + "."
                + self.info_dict["format_id"]
                + "."
                + self.info_dict["ext"],
            )
        else:
            _filename = self.info_dict.get("filename")
            self.download_path = Path(self.base_download_path, self.info_dict["format_id"])
            self.download_path.mkdir(parents=True, exist_ok=True)
            self.filename = Path(
                self.base_download_path,
                _filename.stem
                + "."
                + self.info_dict["format_id"]
                + "."
                + self.info_dict["ext"],
            )

        # self.filesize = none_to_zero(self.info_dict.get('filesize', 0))
        self.down_size = 0

        self.n_parts_dl = 0
        self.parts = []
        if self.filename.exists() and self.filename.stat().st_size > 0:
            self.status = "init_manipulating"
        else:
            self.status = "init"

        self.error_message = ""

        self.count = 0  # cuenta de los workers activos

        self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_httpdl")

        self.special_extr: bool = False

        self.n_parts: int = self.vid_dl.info_dl.get("n_workers", 16)

        self.premsg = ''.join([
                f'[{self.info_dict["id"]}]',
                f'[{self.info_dict["title"]}]',
                f'[{self.info_dict["format_id"]}]'])

        self.init()

    def init(self):

        try:
            def getter(x):
                if not x:
                    return (
                        limiter_non.ratelimit("transp", delay=True),
                        self.n_parts
                    )
                value, key_text = try_get(
                    [(v, sk)
                     for k, v in self._CONFIG.items()
                     for sk in k if sk == x],
                    lambda y: y[0]) or ("", "")
                if value:
                    self.special_extr = True
                    return (
                        value["ratelimit"].ratelimit(key_text, delay=True),
                        value["maxsplits"],
                    )
                else:

                    return (
                        limiter_non.ratelimit("transp", delay=True),
                        self.n_parts
                    )

            _extractor = self.info_dict.get("extractor_key").lower()
            self.auto_pasres = False
            _sem = False
            if _extractor and _extractor.lower() != "generic":
                self._decor, self.n_parts = getter(_extractor)
                if _extractor in ["doodstream", "vidoza"]:
                    self.auto_pasres = True
                if self.n_parts < 16:
                    _sem = True

            else:
                self._decor, self.n_parts = (
                    limiter_non.ratelimit("transp", delay=True),
                    self.n_parts,
                )

            self._NUM_WORKERS = self.n_parts

            self._host = get_domain(self.uris[0])

            if _sem:

                with self.ytdl.params["lock"]:
                    if not (
                        _temp := traverse_obj(
                            self.ytdl.params, ("sem", self._host))
                    ):
                        _temp = Lock()
                        self.ytdl.params["sem"].update({self._host: _temp})

                self.sem = _temp

            else:
                self.sem = contextlib.nullcontext()

            with self.sem:  # type: ignore

                _mult_ranges, _filesize = self.check_server()

                if not _mult_ranges:
                    logger.info(
                        f"{self.premsg}: server cant handle ranges"
                    )
                    self.n_parts = 1
                    self._NUM_WORKERS = 1

                if _filesize:
                    self.filesize = none_to_zero(_filesize)

                if self.filesize:

                    self.create_parts()
                    self.get_parts_to_dl()

                else:
                    raise AsyncHTTPDLErrorFatal("Can't get filesize")

        except (KeyboardInterrupt, Exception) as e:
            logger.error(
                f"{self.premsg}: {str(e)}"
            )
            self.init_client.close()
            self.status = "error"
            self.error_message = repr(e)
            raise

    def check_server(self):
        @dec_retry_error
        @self._decor
        def _check_server():
            try:
                res = self.init_client.head(self.uris[0], headers={"range": "bytes=0-"})
                logger.debug(f"{self.premsg}[check_server] {res} {res.request.headers} {res.headers}")
                res.raise_for_status()
                return (
                    res.headers.get("accept-ranges")
                    or res.headers.get("content-range"),
                    int_or_none(res.headers.get("content-length")),
                )

            except Exception as e:

                logger.exception(f"{self.premsg}[check_server] {repr(e)}")
                raise

        if (_ar := self.info_dict.get("accept_ranges")) and (
            _fs := self.info_dict.get("filesize")
        ):
            return (_ar, _fs)
        else:
            return _check_server() or (None, None)

    def upt_hsize(self, i, offset=None):

        @dec_retry_error
        @self._decor
        def _upt_hsize():
            try:

                if offset:
                    _start = self.parts[i]['offset'] + self.parts[i]['start']
                    self.parts[i]["headers"].append(
                        {
                            "range": f"bytes={_start}-{self.parts[i]['end']}"
                        }
                    )

                res = self.init_client.head(
                    self.parts[i]["url"], headers=self.parts[i]["headers"][-1]
                )
                logger.debug(
                    f"{self.premsg}[upt_hsize] {res} {res.request} {res.request.headers} {res.headers.get('content-length')}"
                )
                res.raise_for_status()
                headers_size = int_or_none(res.headers.get("content-length"))
                if not offset:
                    self.parts[i].update({"headersize": headers_size})
                else:
                    self.parts[i].update({"hsizeoffset": headers_size})

                logger.debug(f"{self.premsg}[upt_hsize] OK for part[{i}] \n{self.parts[i]}")

                return headers_size

            except Exception as e:
                logger.debug(f"{self.premsg}[upt_hsize] NOTOK for part[{i}] {repr(e)}")

        return _upt_hsize()

    def create_parts(self):

        if (
            _partsize := self.filesize // self.n_parts
        ) < self._MIN_SIZE:  # size of parts cant be less than _MIN_SIZE
            temp = self.filesize // self._MIN_SIZE + 1
            logger.info(
                f"{self.premsg}: size parts [{_partsize}] < {self._MIN_SIZE} -> change nparts [{self.n_parts} -> {temp}]"
            )
            self.n_parts = temp
            self._NUM_WORKERS = self.n_parts

        try:
            start_range = 0
            for i in range(1, self.n_parts + 1):

                if i == self.n_parts:
                    self.parts.append(
                        {
                            "part": i,
                            "offset": 0,
                            "start": start_range,
                            "end": "",
                            "headers": [{"range": f"bytes={start_range}-"}],
                            "downloaded": False,
                            "filepath":
                            Path(
                                self.download_path,
                                f"{self.filename.stem}_part_{i}_of_{self.n_parts}",
                            ),
                            "tempfilesize": (
                                _tfs := (
                                    self.filesize // self.n_parts
                                    + self.filesize % self.n_parts
                                )
                            ),
                            "headersize": None,
                            "hsizeoffset": None,
                            "size": -1,
                            "nchunks": (
                                (_nchunks := (_tfs // self._CHUNK_SIZE)) + 1)
                                if _tfs % self._CHUNK_SIZE
                                else _nchunks,  # type: ignore
                            "nchunks_dl": dict(),
                            "n_retries": 0,
                            "time2dlchunks": dict(),
                            "statistics": dict(),
                        }
                    )
                else:
                    end_range = start_range + (
                        self.filesize // self.n_parts - 1)
                    self.parts.append(
                        {
                            "part": i,
                            "offset": 0,
                            "start": start_range,
                            "end": end_range,
                            "headers": [
                                {"range": f"bytes={start_range}-{end_range}"}],
                            "downloaded": False,
                            "filepath": Path(
                                self.download_path,
                                f"{self.filename.stem}_part_{i}_of_{self.n_parts}",
                            ),
                            "tempfilesize": (
                                _tfs := (self.filesize // self.n_parts)),
                            "headersize": None,
                            "hsizeoffset": None,
                            "size": -1,
                            "nchunks": (
                                (_nchunks := (_tfs // self._CHUNK_SIZE)) + 1)
                            if (_tfs % self._CHUNK_SIZE)
                            else _nchunks,  # type: ignore
                            "nchunks_dl": dict(),
                            "n_retries": 0,
                            "time2dlchunks": dict(),
                            "statistics": dict(),
                        }
                    )

                    start_range = end_range + 1

            for i, part in enumerate(self.parts):
                part.update({"url": self.uris[i % len(self.uris)]})

            if self.n_parts == 1:
                self.parts[0].update({"headersize": self.filesize})
            else:
                with ThreadPoolExecutor(
                        thread_name_prefix="ex_httphsize") as ex:
                    # withself.ex_dl as ex:
                    fut = [ex.submit(self.upt_hsize, i)
                           for i in range(self.n_parts)]
                    assert fut
                    # done, pending = wait(fut, return_when=ALL_COMPLETED)

            _not_hsize = [_part for _part in self.parts
                          if not _part["headersize"]]
            if len(_not_hsize) > 0:
                logger.warning(f"{self.premsg}[create parts] not headersize in [{len(_not_hsize)}/{self.n_parts}]")

        except Exception as e:
            logger.debug(f"{self.premsg}[create parts] {repr(e)})")
            raise

    def get_parts_to_dl(self):

        self.parts_to_dl = []

        for i, part in enumerate(self.parts):
            _premsg = f"{self.premsg}[feed queue]:part[{part['part']}]"
            logger.debug(f"{_premsg}\n{part}")
            if not part["filepath"].exists():
                logger.debug(f"{_premsg} doesn't exits, lets DL")
                self.parts_to_dl.append(part["part"])
            else:
                partsize = part["filepath"].stat().st_size
                _headersize = part.get("headersize")
                if not _headersize:
                    _headersize = self.upt_hsize(i)

                if partsize == 0:
                    part["filepath"].unlink()
                    self.parts_to_dl.append(part["part"])
                    logger.debug(f"{_premsg} exits with size {partsize}. Re-download from scratch")

                elif _headersize:
                    if partsize > _headersize + 100:
                        part["filepath"].unlink()
                        self.parts_to_dl.append(part["part"])
                        logger.debug(f"{_premsg} exits size[{partsize}]. Re-download from scratch")

                    elif (_headersize - 100 <= partsize <= _headersize + 100):
                        # with a error margen of +-100bytes,
                        # file is fully downloaded
                        logger.debug(f"{_premsg} exits with size {partsize} and full downloaded")
                        self.down_size += partsize
                        part["downloaded"] = True
                        part["size"] = partsize
                        self.n_parts_dl += 1
                        continue
                    else:  # there's something previously downloaded
                        logger.debug(
                            f"{_premsg} exits size[{partsize}]" +
                            f" not full downloaded {_headersize}. Re-define header range to start from the dl size")
                        _old_part = part
                        part["offset"] = partsize
                        _hsizeoffset = self.upt_hsize(i, offset=True)
                        if _hsizeoffset:
                            part["hsizeoffset"] = _hsizeoffset
                            self.parts_to_dl.append(part["part"])
                            self.down_size += partsize
                            logger.debug(f"{self.premsg}[feed queue]\n{part}\n{self.parts[i]}")
                        else:
                            part = _old_part
                            part["filepath"].unlink()
                            self.parts_to_dl.append(part["part"])
                            logger.warning(f"{_premsg} no headersize. Re-download from scratch")

                else:

                    logger.warning(f"{_premsg} exits size [{partsize}] no headersize. Re-download")
                    part["filepath"].unlink()
                    self.parts_to_dl.append(part["part"])

        logger.debug(f"{self.premsg}[get_parts_to_dl] \n{list(self.parts_to_dl)}")

        if not self.parts_to_dl:
            self.status = "manipulating"

    async def rate_limit(self):

        @self._decor
        async def _rate_limit():
            await asyncio.sleep(0)

        await _rate_limit()

    async def upt_status(self):

        while not self.vid_dl.end_tasks.is_set():

            if self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):

                if self.down_size and not self.check_any_event_is_set():

                    _down_size = self.down_size
                    _speed_meter = self.speedometer(_down_size)
                    self.upt.update(
                        {"speed_meter": _speed_meter, "down_size": _down_size}
                    )

                    if _speed_meter and self.filesize:

                        _est_time = (self.filesize - _down_size) / _speed_meter
                        _est_time_smooth = self.smooth_eta(_est_time)
                        self.upt.update(
                            {"est_time": _est_time,
                             "est_time_smooth": _est_time_smooth}
                        )

            await asyncio.sleep(0)

    @retry
    def get_reset_info(self, _reset_url):
        _reset_info = self.ytdl.sanitize_info(
            self.ytdl.extract_info(_reset_url, download=False))
        if not _reset_info:
            raise AsyncHTTPDLError("no video info")
        return get_format_id(_reset_info, self.info_dict["format_id"])

    def resetdl(self):
        _wurl = self.info_dict.get("webpage_url")
        _webpage_url = (
            smuggle_url(_wurl, {"indexdl": self.vid_dl.index})
            if self.special_extr
            else _wurl
        )
        info_reset = self.get_reset_info(_webpage_url)
        logger.debug(f"{self.premsg}[reset] info reset {info_reset}")
        self.headers = info_reset.get("http_headers")
        self.video_url = info_reset.get("url")
        self.uris = [unquote(self.video_url)]  # type: ignore
        logger.debug(f"{self.premsg}[reset] uris {self.uris}")
        for i, part in enumerate(self.parts):
            part.update({"url": self.uris[i % len(self.uris)]})
        self.down_size = 0
        self.n_parts_dl = 0
        with self.sem:  # type: ignore
            self.get_parts_to_dl()

    def check_any_event_is_set(self):
        return any(
            [
                self.vid_dl.pause_event.is_set(),
                self.vid_dl.reset_event.is_set(),
                self.vid_dl.stop_event.is_set()
            ]

        )

    async def event_handle(self):

        _res = None
        if self.vid_dl.pause_event.is_set():

            _res = await async_waitfortasks(
                events=(self.vid_dl.resume_event, self.vid_dl.reset_event, self.vid_dl.stop_event),
                background_tasks=self.background_tasks)
            self.vid_dl.pause_event.clear()
            self.vid_dl.resume_event.clear()

            await asyncio.sleep(0)
        else:
            _event = [_ev.name for _ev in (self.vid_dl.reset_event, self.vid_dl.stop_event) if _ev.is_set()]
            if _event:
                _res = {"event": _event[0]}
                await asyncio.sleep(0)

        return _res

    async def fetch(self, i):

        client = httpx.AsyncClient(
                #  proxies=try_get(self.proxies, lambda x: x[i]),
                limits=self.limits,
                follow_redirects=True,
                timeout=self.timeout,
                verify=self.verifycert,
                headers=self.headers,
            )

        try:

            logger.debug(f"{self.premsg}[worker-{i}] launched")

            while True:

                _res = await async_waitfortasks(
                    self.parts_queue.get(),
                    events=(self.vid_dl.reset_event, self.vid_dl.stop_event),
                    background_tasks=self.background_tasks)
                if _res.get("event"):
                    return
                elif (_e := _res.get("exception")):
                    raise AsyncHTTPDLError(f'couldnt get frag from queue {repr(_e)}')
                else:
                    part = _res.get("result")
                    if part is None:
                        continue
                    elif isinstance(part, str) and part == "KILL":
                        logger.debug(f"{self.premsg}[worker-{i}] KILL")
                        return

                assert isinstance(part, int)

                _premsg = f"{self.premsg}[worker-{i}]:part[{part}]"

                logger.debug(f'{_premsg}\n{self.parts[part - 1]}')

                tempfilename = self.parts[part - 1]["filepath"]
                url = self.parts[part - 1]["url"]
                headers = self.parts[part - 1]["headers"][-1]

                while self.parts[part - 1]["n_retries"] < self._MAX_RETRIES:

                    try:
                        await self.rate_limit()
                        if (_res := await self.event_handle()):
                            if _res.get("event") in ("stop", "reset"):
                                return

                        async with (
                            aiofiles.open(tempfilename, mode="ab") as f,
                            client.stream("GET", url, headers=headers) as res
                        ):

                            logger.debug(
                                f"{_premsg}: [fetch] resp code {str(res.status_code)}:" +
                                f"rep {self.parts[part-1]['n_retries']}\n{res.request.headers}")

                            nth_key = str(
                                self.parts[part - 1]["n_retries"])
                            self.parts[part - 1]["nchunks_dl"].update(
                                {nth_key: 0})
                            self.parts[part - 1]["time2dlchunks"].update(
                                {nth_key: []}
                            )
                            self.parts[part - 1]["statistics"].update(
                                {nth_key: []})

                            if res.status_code >= 400:

                                raise AsyncHTTPDLError(f"error[{res.status_code}] part[{part}]")

                            else:
                                if (
                                    (len(self.parts[part - 1][
                                        "headers"]) == 1) and
                                    (not self.parts[part - 1][
                                        "headersize"])
                                ):
                                    self.parts[part - 1][
                                        "headersize"
                                    ] = int_or_none(
                                        res.headers.get("content-length")
                                    )

                                num_bytes_downloaded = res.num_bytes_downloaded

                                _started = time.monotonic()

                                async for chunk in res.aiter_bytes(
                                    chunk_size=self._CHUNK_SIZE
                                ):

                                    _timechunk = time.monotonic() - _started
                                    self.parts[part - 1]["time2dlchunks"][
                                        nth_key
                                    ].append(_timechunk)
                                    await f.write(chunk)

                                    async with self._ALOCK:
                                        self.down_size += (_iter_bytes := (
                                            res.num_bytes_downloaded -
                                            num_bytes_downloaded))
                                        if (_dif := self.down_size -
                                                self.filesize) > 0:
                                            self.filesize += _dif
                                        # self.first_data.set()

                                    async with self.vid_dl.alock:
                                        if _dif > 0:
                                            self.vid_dl.info_dl[
                                                "filesize"
                                            ] += _dif
                                        self.vid_dl.info_dl[
                                            "down_size"
                                        ] += _iter_bytes
                                    num_bytes_downloaded = res.num_bytes_downloaded

                                    self.parts[part - 1][
                                        "nchunks_dl"][nth_key] += 1

                                    if (_res := await self.event_handle()):
                                        if _res.get("event") in ("stop", "reset"):
                                            return

                                    _started = time.monotonic()

                        _tempfile_size = (await os.stat(tempfilename)).st_size
                        if (
                            self.parts[part - 1]["headersize"] - 100
                            <= _tempfile_size
                            <= self.parts[part - 1]["headersize"] + 100
                        ):
                            self.parts[part - 1]["downloaded"] = True
                            self.parts[part - 1]["size"] = _tempfile_size
                            async with self._ALOCK:
                                self.n_parts_dl += 1
                            logger.debug(
                                f"{_premsg} OK DL: total {self.n_parts_dl}\n{self.parts[part-1]}"
                            )
                            break

                        else:
                            logger.warning(
                                f"{_premsg}[fetch-stream] end of stream not completed:" +
                                f"{self.parts[part-1]['headersize'] - 100} <=  {_tempfile_size} <= " +
                                f"{self.parts[part-1]['headersize'] + 100}"
                            )

                            raise AsyncHTTPDLError(
                                f"{_premsg}[fetch-stream] end of stream not completed:" +
                                f"{self.parts[part-1]['headersize'] - 100} <=  {_tempfile_size} <= " +
                                f"{self.parts[part-1]['headersize'] + 100}"
                            )

                    except (
                        asyncio.CancelledError,
                        AsyncHTTPDLErrorFatal,
                        RuntimeError
                    ) as e:

                        logger.error(f"{_premsg}[fetch-res] error: {repr(e)}")
                        raise

                    except Exception as e:
                        logger.debug(f"{_premsg}[fetch-res] error: {repr(e)}")

                        if self.parts[part - 1]["n_retries"] < self._MAX_RETRIES:
                            self.parts[part - 1]["n_retries"] += 1
                            _tempfile_size = (await os.stat(
                                tempfilename)).st_size
                            self.parts[part - 1]["offset"] = _tempfile_size
                            if not self.upt_hsize(part - 1, offset=True):
                                raise AsyncHTTPDLErrorFatal(f"{_premsg}[fetch-res] Error to upt hsize part")

                            await asyncio.sleep(0)
                        else:
                            logger.warning(f"{_premsg}[fetch-res] error: maxnumrepeats")
                            raise AsyncHTTPDLErrorFatal(f"MaxNumRepeats part[{part}]")
                    finally:
                        await asyncio.sleep(0)

        finally:
            await client.aclose()
            logger.debug(
                f"{self.premsg}[worker-{i}] says bye"
            )
            self.count -= 1

    async def fetch_async(self):

        try:
            self._ALOCK = asyncio.Lock()
            # self.first_data = asyncio.Event()
            self.areset = sync_to_async(self.resetdl, executor=self.ex_dl)

            while True:

                self.count = self._NUM_WORKERS
                self.parts_queue = asyncio.Queue()
                for part in self.parts_to_dl:
                    self.parts_queue.put_nowait(part)
                for _ in range(self._NUM_WORKERS):
                    self.parts_queue.put_nowait("KILL")
                self.upt = {}
                self.vid_dl.reset_event.clear()
                self.vid_dl.pause_event.clear()
                self.vid_dl.resume_event.clear()
                self.vid_dl.end_tasks.clear()
                # self.first_data.clear()
                self.speedometer = SpeedometerMA(initial_bytes=self.down_size)
                self.progress_timer = ProgressTimer()
                self.smooth_eta = SmoothETA()
                self.status = "downloading"

                try:

                    upt_task = [asyncio.create_task(self.upt_status())]
                    self.background_tasks.add(upt_task[0])
                    upt_task[0].add_done_callback(self.background_tasks.discard)
                    async with async_lock(self.sem):  # type: ignore
                        self.tasks = [
                            asyncio.create_task(self.fetch(i))
                            for i in range(self._NUM_WORKERS)
                        ]
                        for _task in self.tasks:
                            self.background_tasks.add(_task)
                            _task.add_done_callback(self.background_tasks.discard)

                        done, _ = await asyncio.wait(self.tasks)
                        self.vid_dl.end_tasks.set()
                        await asyncio.wait(upt_task)

                    for d in done:
                        try:
                            d.result()
                        except Exception as e:
                            logger.exception(f"{self.premsg} {repr(e)}")

                    if not (await self.partsnotdl()):
                        logger.debug(f'{self.premsg}:Parts DL completed')
                        self.status = "init_manipulating"
                        return

                    elif self.vid_dl.stop_event.is_set():
                        return
                    elif self.vid_dl.reset_event.is_set():
                        await self.areset()
                        continue
                    else:
                        self.status = "error"
                        return

                except AsyncHTTPDLErrorFatal:
                    raise
                except Exception as e:
                    logger.exception(f"{self.premsg}[fetch_async] error {repr(e)}")
                finally:
                    await asyncio.sleep(0)

        except Exception as e:
            logger.exception(f"{self.premsg} {repr(e)}")
        finally:
            if self.init_client:
                self.init_client.close()

    async def partsnotdl(self):
        res = []
        for part in self.parts:
            if part["downloaded"] is False:
                res.append(part["part"])
        return res

    def sync_clean_when_error(self):
        for f in self.parts:
            if f["downloaded"] is False:
                if f["filepath"].exists():
                    f["filepath"].unlink()

    async def clean_when_error(self):
        for f in self.parts:
            if f["downloaded"] is False and await os.path.exists(f["filepath"]):
                await os.remove(f["filepath"])

    async def ensamble_file(self):

        logger.debug(f"{self.premsg}[ensamble_file] start ensambling {self.filename}")

        try:

            async with aiofiles.open(self.filename, mode="wb") as dest:
                for p in self.parts:
                    if p["filepath"].exists():
                        p["size"] = p["filepath"].stat().st_size
                        if p["headersize"] - 100 <= p["size"] <= p[
                                "headersize"] + 100:

                            async with aiofiles.open(p["filepath"], mode="rb") as source:
                                await dest.write(await source.read())
                        else:
                            raise AsyncHTTPDLError(f"{self.premsg}[ensamble_file] error when ensambling: {p}")
                    else:
                        raise AsyncHTTPDLError(f"{self.premsg}[ensamble_file] error when ensambling: {p}")

        except Exception as e:
            logger.debug(f"{self.premsg}[ensamble_file] error when ensambling parts {str(e)}")
            if await os.path.exists(self.filename):
                await os.remove(self.filename)
            self.status = "error"
            await self.clean_when_error()
            raise

        if await os.path.exists(self.filename):
            armtree = sync_to_async(
                    partial(rmtree, ignore_errors=True), executor=self.ex_dl)
            await armtree(self.download_path)
            self.status = "done"
            logger.debug(f"{self.premsg}[ensamble_file] file ensambled")
        else:
            self.status = "error"
            await self.clean_when_error()
            raise AsyncHTTPDLError(f"{self.premsg}[ensamble_file] error when ensambling parts")

    def format_parts(self):
        import math

        return f"{(int(math.log(self.n_parts, 10)) + 1)}d"

    def print_hookup(self):

        _filesize_str = naturalsize(self.filesize) if self.filesize else "--"
        if self.status == "done":
            return f"[HTTP][{self.info_dict['format_id']}]: Completed\n"
        elif self.status == "init":
            return f"[HTTP][{self.info_dict['format_id']}]: Waiting DL [{_filesize_str}][{self.n_parts_dl} of {self.n_parts}]\n"
        elif self.status == "error":
            return (
                f"[HTTP][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size)} " +
                f"[{_filesize_str}][{self.n_parts_dl} of {self.n_parts}]")
        elif self.status == "stop":
            return (
                f"[HTTP][{self.info_dict['format_id']}]: STOPPED {naturalsize(self.down_size)} " +
                f"[{_filesize_str}][{self.n_parts_dl} of {self.n_parts}]")

        elif self.status == "downloading":

            _eta_smooth_str = "--"
            _speed_meter_str = "--"

            _temp = copy.deepcopy(self.upt)

            if not self.check_any_event_is_set():

                if _speed_meter := _temp.get("speed_meter"):
                    _speed_meter_str = f"{naturalsize(_speed_meter)}ps"
                else:
                    _speed_meter_str = "--"

                if (
                    _est_time_smooth := _temp.get("est_time_smooth")
                ) and _est_time_smooth < 3600:

                    _eta_smooth_str = ":".join(
                        [
                            _item.split(".")[0]
                            for _item in
                            f"{timedelta(seconds=_est_time_smooth)}".split(
                                ":"
                            )[1:]
                        ]
                    )
                else:
                    _eta_smooth_str = "--"

            _temp_size = _temp.get("down_size", self.down_size)/self.filesize
            _progress_str = (
                f'{_temp_size*100:5.2f}%'
                if self.filesize
                else "-----"
            )
            return (
                f"[HTTP][{self.info_dict['format_id']}]:(WK[{self.count:2d}]) PARTS[{self.n_parts_dl:{self.format_parts()}}/" +
                f"{self.n_parts}] PR[{_progress_str}] DL[{_speed_meter_str}] ETA[{_eta_smooth_str}]")

        elif self.status == "init_manipulating":
            return f"[HTTP][{self.info_dict['format_id']}]: Waiting for Ensambling \n"
        elif self.status == "manipulating":
            if self.filename.exists():
                _size = self.filename.stat().st_size
            else:
                _size = 0
            return f"[HTTP][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size)} [{_filesize_str}]\n"
