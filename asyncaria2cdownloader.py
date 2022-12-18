import asyncio
import copy
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from threading import Lock
from urllib.parse import unquote, urlparse, urlunparse

import aria2p

from utils import (
    CONF_ARIA2C_EXTR_GROUP,
    CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED,
    CONF_ARIA2C_MIN_SIZE_SPLIT,
    CONF_ARIA2C_N_CHUNKS_CHECK_SPEED,
    CONF_ARIA2C_SPEED_PER_CONNECTION,
    CONF_ARIA2C_TIMEOUT_INIT,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    CONF_PROXIES_N_GR_VIDEO,
    CONFIG_EXTRACTORS,
    ProgressTimer,
    ProxyYTDL,
    SpeedometerMA,
    async_ex_in_executor,
    async_wait_time,
    get_domain,
    get_format_id,
    limiter_non,
    naturalsize,
    none_to_zero,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    try_get,
    unsmuggle_url,
)

logger = logging.getLogger("async_ARIA2C_DL")


class AsyncARIA2CDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncARIA2CDLError(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncARIA2CDownloader:
    _CONFIG = CONFIG_EXTRACTORS.copy()

    def __init__(self, port, enproxy, video_dict, vid_dl):

        self.info_dict = video_dict.copy()
        self.video_downloader = vid_dl
        self.enproxy = enproxy
        self.aria2_client = aria2p.API(aria2p.Client(port=port))

        self.ytdl = traverse_obj(self.video_downloader.info_dl, ("ytdl"))

        self.proxies = [i for i in range(CONF_PROXIES_MAX_N_GR_HOST)]

        self.video_url = self.info_dict.get("url")
        self.uris = [self.video_url]
        self._host = get_domain(self.video_url)

        self.headers = self.info_dict.get("http_headers")

        self.download_path = self.info_dict["download_path"]

        self.download_path.mkdir(parents=True, exist_ok=True)
        if _filename := self.info_dict.get("_filename"):

            self.filename = Path(
                self.download_path,
                _filename.stem
                + "."
                + self.info_dict["format_id"]
                + "."
                + "aria2."
                + self.info_dict["ext"],
            )
        else:
            _filename = self.info_dict.get("filename")
            self.filename = Path(
                self.download_path,
                _filename.stem
                + "."
                + self.info_dict["format_id"]
                + "."
                + "aria2."
                + self.info_dict["ext"],
            )

        self.filesize = none_to_zero((self.info_dict.get("filesize", 0)))
        self.down_size = 0

        self.status = "init"
        self.error_message = ""

        self.n_workers = self.video_downloader.info_dl["n_workers"]

        self.dl_cont = None

        self.count_init = 0

        self.last_progress_str = "--"

        self._ex_aria2dl = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")

        self.prep_init()

    def prep_init(self):
        def getter(x):

            value, key_text = try_get(
                [
                    (v, kt)
                    for k, v in self._CONFIG.items()
                    if any(x == (kt := _) for _ in k)
                ],
                lambda y: y[0],
            ) or ("", "")

            if value:
                self.special_extr = True
                return (
                    value["ratelimit"].ratelimit(key_text, delay=True),
                    value["maxsplits"],
                )

        _extractor = try_get(self.info_dict.get("extractor_key"), lambda x: x.lower())
        self.auto_pasres = False
        self.special_extr = False
        _sem = False
        self._mode = "simple"
        if _extractor and _extractor != "generic":
            self._decor, self._nsplits = getter(_extractor) or (
                limiter_non.ratelimit("transp", delay=True),
                self.n_workers,
            )
            if self._nsplits < 16 or _extractor in ["boyfriendtv"]:
                _sem = True
                if _extractor in CONF_ARIA2C_EXTR_GROUP:
                    self._mode = "group"

        else:
            self._decor, self._nsplits = (
                limiter_non.ratelimit("transp", delay=True),
                self.n_workers,
            )

        if self.enproxy == 0:
            _sem = False
            self._mode = "noproxy"

        self.n_workers = self._nsplits
        if self.filesize:
            _nsplits = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
            if _nsplits < self.n_workers:
                self.n_workers = _nsplits

        opts_dict = {
            "split": self.n_workers,
            "header": [f"{key}: {value}" for key, value in self.headers.items()],
            "dir": str(self.download_path),
            "out": self.filename.name,
            "uri-selector": "inorder",
            "min-split-size": CONF_ARIA2C_MIN_SIZE_SPLIT,
        }

        self.opts = self.aria2_client.get_global_options()
        for key, value in opts_dict.items():
            rc = self.opts.set(key, value)
            if not rc:
                logger.warning(
                    f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] options - couldnt set [{key}] to [{value}]"
                )

        with self.ytdl.params["lock"]:

            if not (_sem := traverse_obj(self.ytdl.params, ("sem", self._host))):
                _sem = Lock()
                self.ytdl.params["sem"].update({self._host: _sem})

        if _sem:
            if _extractor in ["doodstream"]:
                self.sem = None
                self.auto_pasres = True
            else:
                self.sem = _sem

        else:
            self.sem = None

        self.block_init = True

    async def init(self):

        try:
            if not self.sem:
                self._proxy = None
            else:

                while (
                    not self.video_downloader.stop_event.is_set()
                    and not self.video_downloader.reset_event.is_set()
                ):
                    async with self.video_downloader.master_hosts_alock:
                        if not self.video_downloader.hosts_dl.get(self._host):
                            self.video_downloader.hosts_dl.update(
                                {self._host: {"count": 1, "queue": asyncio.Queue()}}
                            )
                            for el in random.sample(self.proxies, len(self.proxies)):
                                self.video_downloader.hosts_dl[self._host][
                                    "queue"
                                ].put_nowait(el)
                            self._proxy = "get_one"
                            break

                        else:
                            if (
                                self.video_downloader.hosts_dl[self._host]["count"]
                                < CONF_PROXIES_MAX_N_GR_HOST
                            ):
                                self.video_downloader.hosts_dl[self._host]["count"] += 1
                                self._proxy = "get_one"
                                break
                            else:
                                self._proxy = "wait_for_one"

                    if self._proxy == "wait_for_one":
                        await asyncio.sleep(0)
                        continue

                # await asyncio.sleep(0)
                if (
                    self.video_downloader.stop_event.is_set()
                    or self.video_downloader.reset_event.is_set()
                ):
                    return

                done, pending = await asyncio.wait(
                    [
                        self.video_downloader.reset_event.wait(),
                        self.video_downloader.stop_event.wait(),
                        self.video_downloader.hosts_dl[self._host]["queue"].get(),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                try_get(list(pending), lambda x: (x[0].cancel(), x[1].cancel()))
                if (
                    self.video_downloader.stop_event.is_set()
                    or self.video_downloader.reset_event.is_set()
                ):
                    return

                self._index_proxy = try_get(list(done), lambda x: x[0].result())

                _webpage_url = (
                    smuggle_url(
                        self.info_dict.get("webpage_url"),
                        {"indexdl": self.video_downloader.index},
                    )
                    if self.special_extr
                    else self.info_dict.get("webpage_url")
                )

                if self._mode == "simple":

                    self._proxy = f"http://127.0.0.1:{CONF_PROXIES_BASE_PORT+self._index_proxy*100}"
                    self.opts.set("all-proxy", self._proxy)

                    try:
                        _ytdl_opts = self.ytdl.params.copy()

                        async with ProxyYTDL(
                            opts=_ytdl_opts,
                            proxy=self._proxy,
                            executor=self._ex_aria2dl,
                        ) as proxy_ytdl:
                            proxy_info = get_format_id(
                                proxy_ytdl.sanitize_info(
                                    await proxy_ytdl.async_extract_info(_webpage_url)
                                ),
                                self.info_dict["format_id"],
                            )

                        logger.debug(
                            f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] mode simple, proxy ip: {self._proxy} init uri: {proxy_info.get('url')}\n{proxy_info}"
                        )
                        self.video_url = proxy_info.get("url")
                        self.uris = [unquote(proxy_info.get("url"))]

                    except Exception as e:
                        logger.warning(
                            f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] mode simple, proxy ip: {self._proxy} init uri: {repr(e)}"
                        )
                        self.uris = [None]

                    self.opts.set("split", self.n_workers)
                    await asyncio.sleep(0)

                elif self._mode == "group":

                    self._proxy = f"http://127.0.0.1:{CONF_PROXIES_BASE_PORT+self._index_proxy*100+50}"
                    self.opts.set("all-proxy", self._proxy)

                    _uris = []

                    if self.filesize:
                        _n = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
                        _gr = _n // self._nsplits or 1
                        if _gr > CONF_PROXIES_N_GR_VIDEO:
                            _gr = CONF_PROXIES_N_GR_VIDEO
                    else:
                        _gr = CONF_PROXIES_N_GR_VIDEO

                    self.n_workers = _gr * self._nsplits
                    self.opts.set("split", self.n_workers)

                    async def get_uri(i):
                        try:
                            _ytdl_opts = self.ytdl.params.copy()
                            _proxy = f"http://127.0.0.1:{CONF_PROXIES_BASE_PORT+self._index_proxy*100+i}"
                            logger.debug(
                                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] proxy ip{i} {_proxy}"
                            )
                            if (
                                self.video_downloader.stop_event.is_set()
                                or self.video_downloader.reset_event.is_set()
                            ):
                                return

                            async with ProxyYTDL(
                                opts=_ytdl_opts, proxy=_proxy, executor=self._ex_aria2dl
                            ) as proxy_ytdl:
                                proxy_info = get_format_id(
                                    proxy_ytdl.sanitize_info(
                                        await proxy_ytdl.async_extract_info(
                                            _webpage_url
                                        )
                                    ),
                                    self.info_dict["format_id"],
                                )

                            logger.debug(
                                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] proxy ip{i} {_proxy} uri{i} {proxy_info.get('url')}"
                            )
                            _url = unquote(proxy_info.get("url"))
                            _url_as_dict = urlparse(_url)._asdict()
                            _url_as_dict[
                                "netloc"
                            ] = f"__routing={CONF_PROXIES_BASE_PORT+self._index_proxy*100+i}__.{_url_as_dict['netloc']}"
                            return urlunparse(list(_url_as_dict.values()))
                        except Exception as e:
                            _msg = f"host: {self._host} proxy[{i}]: {_proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
                            logger.debug(
                                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] init uris {repr(e)} - {_msg}"
                            )

                    _tasks_get_uri = {
                        asyncio.create_task(get_uri(i)): i for i in range(1, _gr + 1)
                    }
                    _all_tasks = {
                        "get_uri": asyncio.wait(
                            _tasks_get_uri, return_when=asyncio.ALL_COMPLETED
                        ),
                        "reset": asyncio.create_task(
                            self.video_downloader.reset_event.wait()
                        ),
                        "stop": asyncio.create_task(
                            self.video_downloader.stop_event.wait()
                        ),
                    }

                    done, pending = await asyncio.wait(
                        list(_all_tasks.values()), return_when=asyncio.FIRST_COMPLETED
                    )

                    if self.video_downloader.reset_event.is_set():

                        _all_tasks["stop"].cancel()
                        await asyncio.wait(pending)
                        return
                    if self.video_downloader.stop_event.is_set():

                        _all_tasks["reset"].cancel()
                        await asyncio.wait(pending)
                        return

                    try_get(list(pending), lambda x: (x[0].cancel(), x[1].cancel()))
                    for _task in _tasks_get_uri:
                        try:
                            if _uri := _task.result():
                                _uris.append(_uri)
                        except Exception as e:
                            logger.error(
                                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] host: {self._host} init uris proxy[{_tasks_get_uri[_task]}] {repr(e)}"
                            )

                    self.uris = _uris * self._nsplits

            logger.debug(
                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}] proxy {self._proxy} count_init: {self.count_init} uris:\n{self.uris}"
            )

            async with self._decor:
                self.dl_cont = await async_ex_in_executor(
                    self._ex_aria2dl, self.aria2_client.add_uris, self.uris, self.opts
                )

                self.async_update = sync_to_async(self.dl_cont.update, self._ex_aria2dl)
                self.async_pause = sync_to_async(
                    partial(self.aria2_client.pause, [self.dl_cont]), self._ex_aria2dl
                )
                self.async_resume = sync_to_async(
                    partial(self.aria2_client.resume, [self.dl_cont]), self._ex_aria2dl
                )
                self.async_remove = sync_to_async(
                    partial(self.aria2_client.remove, [self.dl_cont], clean=False),
                    self._ex_aria2dl,
                )

            _tstart = time.monotonic()

            while True:
                if not await async_wait_time(
                    CONF_INTERVAL_GUI / 2,
                    events=[
                        self.video_downloader.stop_event,
                        self.video_downloader.reset_event,
                    ],
                ):
                    return
                await self.async_update()
                if (
                    self.video_downloader.stop_event.is_set()
                    or self.video_downloader.reset_event.is_set()
                ):
                    return
                if self.dl_cont and (
                    self.dl_cont.total_length or self.dl_cont.status == "complete"
                ):
                    break
                if (self.dl_cont and self.dl_cont.status == "error") or (
                    time.monotonic() - _tstart > CONF_ARIA2C_TIMEOUT_INIT
                ):
                    if self.dl_cont and self.dl_cont.status == "error":
                        _msg_error = self.dl_cont.error_message
                    else:
                        _msg_error = "timeout"

                    raise AsyncARIA2CDLError(f"init error {_msg_error}")

                await asyncio.sleep(0)

            if (
                self.video_downloader.stop_event.is_set()
                or self.video_downloader.reset_event.is_set()
            ):
                return

            if self.dl_cont and self.dl_cont.status == "complete":
                self._speed.append((datetime.now(), "complete"))
                self.status = "done"

            elif self.dl_cont and self.dl_cont.total_length:
                if not self.filesize:
                    self.filesize = self.dl_cont.total_length
                    async with self.video_downloader.alock:
                        self.video_downloader.info_dl["filesize"] = self.filesize

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if self.sem:
                _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
            else:
                _msg = ""
            if self.dl_cont and self.dl_cont.status == "error":
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e)

            self.error_message = _msg_error
            self.count_init += 1

            if self.count_init < 3:
                logger.warning(
                    f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][init] {_msg} error: {_msg_error} count_init: {self.count_init}, will RESET"
                )
                if "estado=403" in _msg_error:
                    if not self.sem:

                        self.sem = True
                        self._index_proxy = None
                        async with self.video_downloader.master_hosts_alock:
                            if not self.video_downloader.hosts_dl.get(self._host):
                                self.video_downloader.hosts_dl.update(
                                    {self._host: {"count": 1, "queue": asyncio.Queue()}}
                                )
                                for el in random.sample(
                                    self.proxies, len(self.proxies)
                                ):
                                    self.video_downloader.hosts_dl[self._host][
                                        "queue"
                                    ].put_nowait(el)

                self.video_downloader.reset_event.set()
            else:
                self._speed.append((datetime.now(), "error"))
                self.status = "error"
                logger.error(
                    f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][init] {_msg} error: {_msg_error} count_init: {self.count_init}"
                )

    async def check_speed(self):
        def getter(x):
            if x <= 2:
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 0.2
            elif x <= (self.n_workers // 2):
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 1.25
            else:
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 1.75

        _speed = []

        try:
            while True:
                done, pending = await asyncio.wait(
                    [
                        _reset := asyncio.create_task(
                            self.video_downloader.reset_event.wait()
                        ),
                        _stop := asyncio.create_task(
                            self.video_downloader.stop_event.wait()
                        ),
                        _qspeed := asyncio.create_task(self._qspeed.get()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for _el in pending:
                    _el.cancel()
                if any([_ in done for _ in [_reset, _stop]]):
                    logger.info(
                        f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] event detected"
                    )
                    break
                _input_speed = try_get(list(done), lambda x: x[0].result())
                if _input_speed == ("KILL", "KILL"):
                    break

                if self.block_init:
                    continue

                _speed.append(_input_speed)

                if len(_speed) > CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED:

                    if any(
                        [
                            all(
                                [
                                    el[0] == 0
                                    for el in _speed[
                                        -CONF_ARIA2C_N_CHUNKS_CHECK_SPEED // 2 :
                                    ]
                                ]
                            ),
                            all(
                                [
                                    (_speed[i][1] == _speed[-1][1])
                                    and (_speed[i + 1][0] < _speed[i][0] * 0.9)
                                    for i in range(
                                        len(_speed) - CONF_ARIA2C_N_CHUNKS_CHECK_SPEED,
                                        len(_speed) - 1,
                                    )
                                ]
                            ),
                            all(
                                [
                                    el[1] == _speed[-1][1] and el[0] < getter(el[1])
                                    for el in _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED:]
                                ]
                            ),
                        ]
                    ):
                        self.video_downloader.reset_event.set()
                        _str_speed = ", ".join(
                            [
                                f'({el[2].strftime("%H:%M:")}{(el[2].second + (el[2].microsecond / 1000000)):06.3f}, {{"speed": {el[0]}, "connec": {el[1]}}})'
                                for el in _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED:]
                            ]
                        )
                        logger.info(
                            f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] speed reset: n_el_speed[{len(_speed)}]"
                        )
                        logger.debug(
                            f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] speed reset\n{_str_speed}"
                        )

                        break

                    else:
                        _speed = _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED // 2 :]

                await asyncio.wait(pending)
                await asyncio.sleep(0)

            await asyncio.wait(pending)
            await asyncio.sleep(0)

        except Exception as e:
            logger.exception(
                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] {str(e)}"
            )
        finally:
            logger.debug(
                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][check_speed] bye"
            )

    async def fetch(self):

        self.count_init = 0

        self.block_init = False
        try:

            while self.dl_cont and self.dl_cont.status in [
                "active",
                "paused",
                "waiting",
            ]:

                try:

                    if self.video_downloader.pause_event.is_set():
                        self._speed.append((datetime.now(), "pause"))
                        await self.async_pause()
                        await asyncio.sleep(0)
                        done, pending = await asyncio.wait(
                            [
                                asyncio.create_task(
                                    self.video_downloader.resume_event.wait()
                                ),
                                asyncio.create_task(
                                    self.video_downloader.reset_event.wait()
                                ),
                                asyncio.create_task(
                                    self.video_downloader.stop_event.wait()
                                ),
                            ],
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        for _el in pending:
                            _el.cancel()

                        async with self._decor:
                            await self.async_resume()
                        self._speed.append((datetime.now(), "resume"))
                        await asyncio.sleep(0)
                        self.video_downloader.pause_event.clear()
                        self.video_downloader.resume_event.clear()
                        self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2)
                        if any(
                            [
                                self.video_downloader.stop_event.is_set(),
                                self.video_downloader.reset_event.is_set(),
                            ]
                        ):

                            return

                    elif any(
                        [
                            self.video_downloader.stop_event.is_set(),
                            self.video_downloader.reset_event.is_set(),
                        ]
                    ):
                        self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2)
                        return

                    elif self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):
                        await self.async_update()
                        if self._qspeed:
                            self._qspeed.put_nowait(
                                (
                                    self.dl_cont.download_speed,
                                    self.dl_cont.connections,
                                    datetime.now(),
                                )
                            )
                        self._speed.append((datetime.now(), self.dl_cont))
                        _incsize = self.dl_cont.completed_length - self.down_size
                        self.down_size = self.dl_cont.completed_length

                        async with self.video_downloader.alock:
                            self.video_downloader.info_dl["down_size"] += _incsize

                    await asyncio.sleep(0)

                except BaseException as e:
                    if isinstance(e, KeyboardInterrupt):
                        raise

            if self.dl_cont and self.dl_cont.status == "complete":
                self._speed.append((datetime.now(), "complete"))
                self.status = "done"
                return
            elif self.dl_cont and self.dl_cont.status == "error":

                raise AsyncARIA2CDLError("error")

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if self.sem:
                _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
            else:
                _msg = ""
            if self.dl_cont and self.dl_cont.status == "error":
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e)
            logger.error(
                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch] {_msg}error: {_msg_error}"
            )
            self._speed.append((datetime.now(), "error"))
            self.status = "error"
            self.error_message = _msg_error

    async def fetch_async(self):

        self.progress_timer = ProgressTimer()
        self.status = "downloading"
        self._speed = []

        try:
            while True:

                try:

                    self._speed.append((datetime.now(), "init"))
                    await self.init()
                    if self.status == "done":
                        return
                    elif self.status == "error":
                        return
                    elif self.video_downloader.stop_event.is_set():
                        self._speed.append((datetime.now(), "stop"))
                        self.status = "stop"
                        return
                    elif self.video_downloader.reset_event.is_set():
                        self._speed.append((datetime.now(), "reset"))
                        self.video_downloader.reset_event.clear()
                        self.block_init = True
                        if self.dl_cont:
                            await self.async_remove()
                            continue
                    try:
                        self._qspeed = asyncio.Queue()
                        check_task = [asyncio.create_task(self.check_speed())]
                        self._speed.append((datetime.now(), "fetch"))
                        await self.fetch()
                        if self.status == "done":
                            return
                        elif self.status == "error":
                            return
                        elif self.video_downloader.stop_event.is_set():
                            self._speed.append((datetime.now(), "stop"))
                            self.status = "stop"
                            return
                        elif self.video_downloader.reset_event.is_set():
                            self._speed.append((datetime.now(), "reset"))
                            self.video_downloader.reset_event.clear()
                            self.block_init = True
                            if self.dl_cont:
                                await self.async_remove()
                                continue
                    finally:
                        self._qspeed.put_nowait(("KILL", "KILL"))
                        await asyncio.sleep(0)
                        await asyncio.wait(check_task)
                except BaseException as e:
                    if isinstance(e, KeyboardInterrupt):
                        raise
                    if self.sem:
                        _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
                    else:
                        _msg = ""
                    if self.dl_cont and self.dl_cont.status == "error":
                        _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
                    else:
                        _msg_error = repr(e)
                    logger.error(
                        f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch_async] {_msg} error: {_msg_error}"
                    )
                    self.status = "error"
                    self.error_message = _msg_error
                finally:

                    if self.sem:
                        async with self.video_downloader.master_hosts_alock:
                            self.video_downloader.hosts_dl[self._host]["count"] -= 1
                            if self._index_proxy:
                                self.video_downloader.hosts_dl[self._host][
                                    "queue"
                                ].put_nowait(self._index_proxy)
                        self._proxy = None

        except BaseException as e:
            logger.exception(
                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch_async] {repr(e)}"
            )
        finally:

            def _print_el(el):
                if isinstance(el, str):
                    return el
                else:
                    return {"status": el.status, "speed": el.download_speed}

            _str_speed = ", ".join(
                [
                    f'({el[0].strftime("%H:%M:")}{(el[0].second + (el[0].microsecond / 1000000)):06.3f}, {_print_el(el[1])})'
                    for el in self._speed
                ]
            )
            logger.debug(
                f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}][fetch_async] exiting [{len(self._speed)}]\n{_str_speed}"
            )

    def print_hookup(self):

        msg = ""

        if self.status == "done":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Completed\n"
        elif self.status == "init":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Waiting [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n"
        elif self.status == "error":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: ERROR {naturalsize(self.down_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n"
        elif self.status == "stop":
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: STOPPED {naturalsize(self.down_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f') if self.filesize else 'NA'}]\n"
        elif self.status == "downloading":

            if not any(
                [
                    self.block_init,
                    self.video_downloader.reset_event.is_set(),
                    self.video_downloader.stop_event.is_set(),
                    self.video_downloader.pause_event.is_set(),
                ]
            ):

                _temp = copy.deepcopy(self.dl_cont)

                _speed_str = (
                    f'{naturalsize(_temp.download_speed,binary=True,format_="6.2f")}ps'
                )
                _progress_str = f"{_temp.progress:.0f}%"
                self.last_progress_str = _progress_str
                _connections = _temp.connections
                _eta_str = _temp.eta_string()

                msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}] CONN[{_connections:2d}/{self.n_workers:2d}] DL[{_speed_str}] PR[{_progress_str}] ETA[{_eta_str}]\n"

            else:

                if self.block_init:
                    _substr = "INIT"
                elif self.video_downloader.pause_event.is_set():
                    _substr = "PAUSED"
                elif self.video_downloader.reset_event.is_set():
                    _substr = "RESET"
                elif self.video_downloader.stop_event.is_set():
                    _substr = "STOPPED"
                msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}] {_substr} DL PR[{self.last_progress_str}]\n"

        elif self.status == "manipulating":
            if self.filename.exists():
                _size = self.filename.stat().st_size
            else:
                _size = 0
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f')}]\n"

        return msg
