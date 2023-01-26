import asyncio
import contextlib
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
    CONF_AUTO_PASRES,
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
    async_lock,
    get_format_id,
    limiter_non,
    naturalsize,
    none_to_zero,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    try_get,
    myYTDL,
    async_waitfortasks
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
        self.ytdl: myYTDL = self.video_downloader.info_dl["ytdl"]

        self.proxies = [i for i in range(CONF_PROXIES_MAX_N_GR_HOST)]

        self.video_url = unquote(self.info_dict.get("url"))
        self.uris = [self.video_url]
        self._host = urlparse(self.video_url).netloc

        self.headers = self.info_dict.get("http_headers")

        self.download_path = self.info_dict["download_path"]

        self.download_path.mkdir(parents=True, exist_ok=True)

        _filename = self.info_dict.get(
            "_filename") or self.info_dict.get("filename")
        self.filename = Path(
            self.download_path,
            f"{_filename.stem}.{self.info_dict['format_id']}.\
                aria2.{self.info_dict['ext']}")

        self.filesize = none_to_zero((self.info_dict.get("filesize", 0)))
        self.down_size = 0

        self.status = "init"
        self.error_message = ""

        self.n_workers = self.video_downloader.info_dl["n_workers"]

        self.count_init = 0

        self.last_progress_str = "--"

        self.ex_dl = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")

        self.prep_init()

    def prep_init(self):

        def getter(x):
            if not x:
                value, key_text = ("", "")
            else:
                value, key_text = try_get(
                    [(v, sk) for k, v in self._CONFIG.items()
                        for sk in k if sk == x], lambda y: y[0]
                ) or ("", "")

            if value:
                self.special_extr = True
                limit = value["ratelimit"].ratelimit(key_text, delay=True)
                maxplits = value["maxsplits"]
            else:
                self.special_extr = False
                limit = limiter_non.ratelimit("transp", delay=True)
                maxplits = self.n_workers

            _sem = False
            if maxplits < 16:  # or x in ['']:
                _sem = True
                if x in CONF_ARIA2C_EXTR_GROUP:
                    self._mode = "group"

            if x in CONF_AUTO_PASRES:
                self.auto_pasres = True

            return (_sem, limit, maxplits)

        self._extractor = try_get(self.info_dict.get(
            "extractor_key"), lambda x: x.lower())
        self.auto_pasres = False
        self.special_extr = False
        self._mode = "simple"
        _sem, self._decor, self._nsplits = getter(self._extractor)

        if not self.enproxy:
            self._mode = "noproxy"

        self.n_workers = self._nsplits
        if self.filesize:
            _nsplits = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
            if _nsplits < self.n_workers:
                self.n_workers = _nsplits

        opts_dict = {
            "split": self.n_workers,
            "header": '\n'.join(
                [f"{key}: {value}"
                    for key, value in self.headers.items()]),
            "dir": str(self.download_path),
            "out": self.filename.name,
            "uri-selector": "inorder",
            "min-split-size": CONF_ARIA2C_MIN_SIZE_SPLIT,
        }

        self.premsg = f"[{self.info_dict['id']}][{self.info_dict['title']}][{self.info_dict['format_id']}]"
        self.opts = self.aria2_client.get_global_options()
        for key, value in opts_dict.items():
            if not isinstance(value, list):
                values = [value]
            else:
                values = value
            for value in values:
                rc = self.opts.set(key, value)
                if not rc:
                    logger.warning(
                        f"{self.premsg} couldnt set [{key}] to [{value}]"
                    )

        if (_sem and self._mode == "noproxy"):

            with self.ytdl.params.get("lock", contextlib.nullcontext()):

                if not (_temp := traverse_obj(self.ytdl.params,
                                              ("sem", self._host))):
                    _temp = Lock()
                    if not self.ytdl.params.get("sem"):
                        self.ytdl.params.update({"sem": {}})
                    self.ytdl.params["sem"].update({self._host: _temp})

            assert isinstance(_temp, Lock)
            self.sem = _temp

        else:
            self.sem = contextlib.nullcontext()

        self.block_init = True

    async def init(self):

        try:
            if self._mode == "noproxy":
                self._index_proxy = -1
                self._proxy = None
                self.uris = [
                    unquote(self.info_dict.get("url"))] * self.n_workers

            else:
                self._proxy = None

                async with self.video_downloader.master_hosts_alock:
                    if not self.video_downloader.hosts_dl.get(self._host):
                        queue_ = asyncio.Queue()
                        self.video_downloader.hosts_dl.update(
                            {self._host: {"count": 0,
                                          "queue": queue_}})

                        for el in random.sample(self.proxies,
                                                len(self.proxies)):
                            queue_.put_nowait(el)

                _res = await async_waitfortasks(
                    self.video_downloader.hosts_dl[self._host]["queue"].get(),
                    events=(self.video_downloader.reset_event,
                            self.video_downloader.stop_event))
                if _res.get("event"):
                    return
                elif (_e := _res.get("exception")):
                    raise AsyncARIA2CDLError(
                        f"couldnt get index proxy: {repr(_e)}")
                else:
                    self._index_proxy = _res.get("result", -1)
                    if self._index_proxy is None or\
                        not isinstance(self._index_proxy, int) or\
                            self._index_proxy == -1:
                        raise AsyncARIA2CDLError(
                            f"couldnt get index proxy: {self._index_proxy}")
                    self.video_downloader.hosts_dl[self._host]["count"] += 1

                _init_url = self.info_dict.get("webpage_url")
                if self.special_extr:
                    _init_url = smuggle_url(
                        _init_url, {"indexdl": self.video_downloader.index})

                if self._mode == "simple":

                    self._proxy = f"http://127.0.0.1:{CONF_PROXIES_BASE_PORT+self._index_proxy*100}"
                    self.opts.set("all-proxy", self._proxy)

                    try:
                        _ytdl_opts = self.ytdl.params.copy()

                        async with ProxyYTDL(
                            opts=_ytdl_opts,
                            proxy=self._proxy,
                            executor=self.ex_dl,
                        ) as proxy_ytdl:
                            proxy_info = get_format_id(
                                proxy_ytdl.sanitize_info(
                                    await proxy_ytdl.async_extract_info(_init_url)
                                ),
                                self.info_dict["format_id"],
                            )

                        _proxy_info_url = try_get(traverse_obj(proxy_info, ('url')), lambda x: unquote(x) if x else None)
                        logger.debug(
                            f"{self.premsg} mode simple, proxy ip: {self._proxy} init uri: {_proxy_info_url}\n{proxy_info}"
                        )
                        if not _proxy_info_url:
                            raise AsyncARIA2CDLError("couldnt get video url")
                        self.uris = [_proxy_info_url]

                    except Exception as e:
                        logger.warning(
                            f"{self.premsg} mode simple, proxy ip: {self._proxy} init uri: {repr(e)}"
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

                        assert isinstance(self._index_proxy, int)

                        _ytdl_opts = self.ytdl.params.copy()
                        _proxy_port = CONF_PROXIES_BASE_PORT + \
                            self._index_proxy*100+i
                        _proxy = f"http://127.0.0.1:{_proxy_port}"
                        logger.debug(
                            f"{self.premsg} proxy ip{i} {_proxy}"
                        )

                        try:

                            # if (
                            #     self.video_downloader.stop_event.is_set()
                            #     or self.video_downloader.reset_event.is_set()
                            # ):
                            #     return

                            async with ProxyYTDL(
                                opts=_ytdl_opts, proxy=_proxy, executor=self.ex_dl
                            ) as proxy_ytdl:
                                proxy_info = get_format_id(
                                    proxy_ytdl.sanitize_info(
                                        await proxy_ytdl.async_extract_info(_init_url)
                                    ),
                                    self.info_dict["format_id"],
                                )

                            _url = try_get(proxy_info['url'], lambda x: unquote(x) if x else None)

                            logger.debug(
                                f"{self.premsg} proxy ip{i} {_proxy} uri{i} {_url}"
                            )

                            if not _url:
                                raise AsyncARIA2CDLError(
                                    "couldnt get video url")

                            _url_as_dict = urlparse(_url)._asdict()
                            _url_as_dict["netloc"] = f"__routing={CONF_PROXIES_BASE_PORT+self._index_proxy*100+i}__.{_url_as_dict['netloc']}"
                            return urlunparse(list(_url_as_dict.values()))

                        except Exception as e:
                            _msg = f"host[{self._host}] proxy[({i}){_proxy}]: {str(e)}"
                            logger.debug(
                                f"{self.premsg}[{self.info_dict.get('original_url')}] ERROR init uris {_msg}"
                            )
                            raise
                        finally:
                            await asyncio.sleep(0)

                    _res = await async_waitfortasks({asyncio.create_task(get_uri(i)): i for i in range(1, _gr + 1)},
                                                    events=(self.video_downloader.reset_event, self.video_downloader.stop_event))
                    if _res.get("event"):
                        return
                    elif (_e := _res.get("exception")):
                        raise AsyncARIA2CDLError(
                            f"couldnt get uris: {repr(_e)}")
                    else:
                        _uris = _res.get("result", [])

                    assert _uris and isinstance(_uris, list)

                    self.uris = _uris * self._nsplits

            logger.debug(
                f"{self.premsg} proxy {self._proxy} count_init: {self.count_init} uris:\n{self.uris}"
            )

            async def add_uris() -> aria2p.Download:
                async with self._decor:
                    return await sync_to_async(self.aria2_client.add_uris, executor=self.ex_dl)(self.uris, options=self.opts)

            if (_dl := await add_uris()):
                self.dl_cont = _dl

            if hasattr(self, 'dl_cont'):
                self.async_update = sync_to_async(
                    self.dl_cont.update, executor=self.ex_dl)
                self.async_pause = sync_to_async(
                    partial(self.aria2_client.pause, [self.dl_cont]), executor=self.ex_dl
                )
                self.async_resume = sync_to_async(
                    partial(self.aria2_client.resume, [self.dl_cont]), executor=self.ex_dl
                )
                self.async_remove = sync_to_async(
                    partial(self.aria2_client.remove, [
                            self.dl_cont], clean=False),
                    executor=self.ex_dl,
                )

                self.async_restart = sync_to_async(
                    partial(self.aria2_client.remove, [
                            self.dl_cont], files=True, clean=True),
                    executor=self.ex_dl,
                )

                _tstart = time.monotonic()

                while True:
                    _res = await async_waitfortasks(timeout=CONF_INTERVAL_GUI / 2, events=(self.video_downloader.reset_event, self.video_downloader.stop_event))
                    if _res.get("event"):
                        return
                    await self.async_update()

                    if (
                        self.dl_cont.total_length or self.dl_cont.status == "complete"
                    ):
                        break
                    if (self.dl_cont.status == "error") or (
                        time.monotonic() - _tstart > CONF_ARIA2C_TIMEOUT_INIT
                    ):
                        if self.dl_cont.status == "error":
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

                if self.dl_cont.status == "complete":
                    self._speed.append((datetime.now(), "complete"))
                    self.status = "done"

                elif self.dl_cont.total_length:
                    if not self.filesize:
                        self.filesize = self.dl_cont.total_length
                        async with self.video_downloader.alock:
                            self.video_downloader.info_dl["filesize"] = self.filesize

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if self._mode != "noproxy":
                _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
            else:
                _msg = ""
            if hasattr(self, 'dl_cont') and self.dl_cont.status == "error":
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e)

            self.error_message = _msg_error
            self.count_init += 1

            if self.count_init < 3:
                if "estado=403" in _msg_error:
                    logger.warning(
                        f"{self.premsg}[init] {_msg} error: {_msg_error} count_init: {self.count_init}, will RESET"
                    )
                else:
                    logger.debug(
                        f"{self.premsg}[init] {_msg} error: {_msg_error} count_init: {self.count_init}, will RESET"
                    )

                await self.video_downloader.reset()
            else:
                self._speed.append((datetime.now(), "error"))
                self.status = "error"
                logger.error(
                    f"{self.premsg}[init] {_msg} error: {_msg_error} count_init: {self.count_init}"
                )

    async def check_speed(self):
        def getter(x):
            if x <= 2:
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 0.25
            elif x <= (self.n_workers // 2):
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 1.25
            else:
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 2.25

        def len_ap_list(_list, el):
            _list.append(el)
            return len(_list)

        _speed = []

        try:
            while True:
                _res = await async_waitfortasks(self._qspeed.get(), events=(self.video_downloader.reset_event, self.video_downloader.stop_event))
                if _res.get("event"):
                    break
                elif (_e := _res.get("exception")):
                    raise AsyncARIA2CDLError(
                        f"couldnt get index proxy: {repr(_e)}")
                elif not (_input_speed := _res.get("result")):
                    await asyncio.sleep(0)
                    continue
                elif isinstance(_input_speed, tuple) and isinstance(_input_speed[0], str) and _input_speed[0] == "KILL":
                    break
                elif self.block_init:
                    await asyncio.sleep(0)
                    continue
                elif len_ap_list(_speed, _input_speed) > CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED:

                    if any(
                            [
                                all([
                                    el[0] == 0 for el in _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED:]]),
                                all([self.n_workers > 1, all(
                                    [el[1] <= 1 for el in _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED:]])]),
                                all([el[1] == _speed[-1][1] and el[0] < getter(el[1])
                                    for el in _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED:]]),
                                # all([(_speed[i][1] == _speed[-1][1]) and (_speed[i + 1][0] < _speed[i][0] * 0.9) for i in range(len(_speed) - CONF_ARIA2C_N_CHUNKS_CHECK_SPEED, len(_speed) - 1)]),
                            ]):

                        _str_speed = ", ".join(
                            [
                                f'({el[2].strftime("%H:%M:")}{(el[2].second + (el[2].microsecond / 1000000)):06.3f}, {{"speed": {el[0]}, "connec": {el[1]}}})'
                                for el in _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED:]
                            ]
                        )
                        logger.info(
                            f"{self.premsg}[check_speed] speed reset: n_el_speed[{len(_speed)}]")
                        logger.debug(
                            f"{self.premsg}[check_speed] speed reset\n{_str_speed}")
                        await self.video_downloader.reset()
                        break

                    else:
                        # _speed = _speed[-CONF_ARIA2C_N_CHUNKS_CHECK_SPEED//2:]
                        _speed[0:CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED -
                               CONF_ARIA2C_N_CHUNKS_CHECK_SPEED+1] = ()
                        await asyncio.sleep(0)

                else:
                    await asyncio.sleep(0)

            await asyncio.sleep(0)

        except Exception as e:
            logger.exception(
                f"{self.premsg}[check_speed] {str(e)}")
        finally:
            logger.debug(
                f"{self.premsg}[check_speed] bye")

    async def fetch(self):

        self.count_init = 0
        self.block_init = False

        try:

            while (hasattr(self, 'dl_cont') and self.dl_cont.status in ["active", "paused", "waiting"]):

                try:

                    if self.video_downloader.pause_event.is_set():
                        self._speed.append((datetime.now(), "pause"))
                        await self.async_pause()
                        await asyncio.sleep(0)

                        _res = await async_waitfortasks(events=(self.video_downloader.resume_event, self.video_downloader.reset_event, self.video_downloader.stop_event))

                        async with self._decor:
                            await self.async_resume()
                        self._speed.append((datetime.now(), "resume"))
                        await asyncio.sleep(0)
                        self.video_downloader.pause_event.clear()
                        self.video_downloader.resume_event.clear()
                        self.progress_timer.reset()

                        if _res.get("event") in ("stop", "reset"):
                            return

                    elif any([self.video_downloader.stop_event.is_set(), self.video_downloader.reset_event.is_set()]):
                        self.progress_timer.reset()
                        return

                    elif self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):
                        await self.async_update()
                        if hasattr(self, '_qspeed'):
                            self._qspeed.put_nowait(
                                (
                                    self.dl_cont.download_speed,
                                    self.dl_cont.connections,
                                    datetime.now()
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

            if hasattr(self, 'dl_cont'):
                if self.dl_cont.status == "complete":
                    self._speed.append((datetime.now(), "complete"))
                    self.status = "done"
                    return

                if self.dl_cont.status == "error":
                    raise AsyncARIA2CDLError("error")

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if self._mode != "noproxy":
                _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
            else:
                _msg = ""
            if hasattr(self, 'dl_cont') and self.dl_cont.status == "error":
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e)
            logger.error(
                f"{self.premsg}[fetch] {_msg}error: {_msg_error}")
            self._speed.append((datetime.now(), "error"))
            self.status = "error"
            self.error_message = _msg_error

    async def fetch_async(self):

        self.progress_timer = ProgressTimer()
        self.status = "downloading"
        self._speed = []

        try:
            while True:
                async with async_lock(self.sem):
                    try:
                        self._speed.append((datetime.now(), "init"))
                        await self.init()
                        if self.status in ("done", "error"):
                            return
                        elif self.video_downloader.stop_event.is_set():
                            self._speed.append((datetime.now(), "stop"))
                            if self.status == "stop":
                                return
                            self.video_downloader.stop_event.clear()
                            self.block_init = True
                            if hasattr(self, 'dl_cont'):
                                await self.async_restart()
                                async with self.video_downloader.alock:
                                    self.video_downloader.info_dl["down_size"] -= self.down_size
                                self.down_size = 0
                                continue
                        elif self.video_downloader.reset_event.is_set():
                            self._speed.append((datetime.now(), "reset"))
                            self.video_downloader.reset_event.clear()
                            self.block_init = True
                            if hasattr(self, 'dl_cont'):
                                await self.async_remove()
                                continue

                        check_task = []
                        try:
                            self._qspeed = asyncio.Queue()
                            check_task = [
                                asyncio.create_task(self.check_speed())]
                            self._speed.append((datetime.now(), "fetch"))
                            await self.fetch()
                            if self.status in ("done", "error"):
                                return
                            elif self.video_downloader.stop_event.is_set():
                                self._speed.append((datetime.now(), "stop"))
                                if self.status == "stop":
                                    return
                                self.video_downloader.stop_event.clear()
                                self.block_init = True
                                if hasattr(self, 'dl_cont'):
                                    await self.async_restart()
                                    async with self.video_downloader.alock:
                                        self.video_downloader.info_dl["down_size"] -= self.down_size
                                    self.down_size = 0
                                    continue
                            elif self.video_downloader.reset_event.is_set():
                                self._speed.append((datetime.now(), "reset"))
                                self.video_downloader.reset_event.clear()
                                self.block_init = True
                                if hasattr(self, 'dl_cont'):
                                    await self.async_remove()
                                    continue
                        finally:
                            self._qspeed.put_nowait(
                                ("KILL", "KILL", datetime.now()))
                            await asyncio.sleep(0)
                            if check_task:
                                await asyncio.wait(check_task)
                    except BaseException as e:
                        if isinstance(e, KeyboardInterrupt):
                            raise
                        if self._mode != "noproxy":
                            _msg = f"host: {self._host} proxy: {self._proxy} count: {self.video_downloader.hosts_dl[self._host]['count']}"
                        else:
                            _msg = ""
                        if hasattr(self, 'dl_cont') and self.dl_cont.status == "error":
                            _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
                        else:
                            _msg_error = repr(e)
                        logger.error(
                            f"{self.premsg}[fetch_async] {_msg} error: {_msg_error}"
                        )
                        self.status = "error"
                        self.error_message = _msg_error
                    finally:
                        if self._mode != "noproxy" and hasattr(self, '_index_proxy') and self._index_proxy is not None and isinstance(self._index_proxy, int) and self._index_proxy >= 0:
                            async with self.video_downloader.master_hosts_alock:
                                self.video_downloader.hosts_dl[self._host]["count"] -= 1
                                self.video_downloader.hosts_dl[self._host]["queue"].put_nowait(
                                    self._index_proxy)
                            self._proxy = None

        except BaseException as e:
            logger.exception(
                f"{self.premsg}[fetch_async] {repr(e)}"
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
                f"{self.premsg}[fetch_async] exiting [{len(self._speed)}]\n{_str_speed}"
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
            ) and hasattr(self, 'dl_cont'):

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
                else:
                    _substr = "UNKNOWN"
                msg = f"[ARIA2C][{self.info_dict['format_id']}]: HOST[{self._host.split('.')[0]}] {_substr} DL PR[{self.last_progress_str}]\n"

        elif self.status == "manipulating":
            if self.filename.exists():
                _size = self.filename.stat().st_size
            else:
                _size = 0
            msg = f"[ARIA2C][{self.info_dict['format_id']}]: Ensambling {naturalsize(_size, format_='.2f')} [{naturalsize(self.filesize, format_='.2f')}]\n"

        return msg
