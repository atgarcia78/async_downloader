import asyncio
import contextlib
import copy
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Lock
from urllib.parse import unquote, urlparse, urlunparse
from typing import (
    cast,
    Union)

import aria2p
import requests

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
    async_waitfortasks,
    put_sequence,
    my_dec_on_exception
)

logger = logging.getLogger('async_ARIA2C_DL')


class AsyncARIA2CDLErrorFatal(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


class AsyncARIA2CDLError(Exception):
    def __init__(self, msg, exc_info=None):
        super().__init__(msg)
        self.exc_info = exc_info


retry = my_dec_on_exception(
    AsyncARIA2CDLErrorFatal, max_time=60, raise_on_giveup=False, interval=5)

kill_item = object()


class AsyncARIA2CDownloader:

    _CONFIG = CONFIG_EXTRACTORS.copy()
    _CLASSALOCK = asyncio.Lock()
    _LOCK = Lock()
    _INIT = False
    aria2_client: aria2p.API

    def __init__(self, port, enproxy, video_dict, vid_dl):

        self.background_tasks = set()
        self.info_dict = video_dict.copy()
        self.vid_dl = vid_dl
        self.enproxy = enproxy

        with AsyncARIA2CDownloader._LOCK:
            if not AsyncARIA2CDownloader._INIT:
                AsyncARIA2CDownloader.aria2_client = aria2p.API(aria2p.Client(port=port, timeout=2))
                AsyncARIA2CDownloader._INIT = True

        self.ytdl: myYTDL = self.vid_dl.info_dl['ytdl']

        self.video_url = unquote(self.info_dict.get('url'))
        self.uris = [self.video_url]
        self._host = urlparse(self.video_url).netloc

        self.headers = self.info_dict.get('http_headers')

        self.download_path = self.info_dict['download_path']

        self.download_path.mkdir(parents=True, exist_ok=True)

        _filename = self.info_dict.get('_filename', self.info_dict.get('filename'))
        self.filename = Path(
            self.download_path,
            f'{_filename.stem}.{self.info_dict["format_id"]}.aria2c.{self.info_dict["ext"]}')

        self.filesize = none_to_zero((self.info_dict.get('filesize', 0)))
        self.down_size = 0

        self.status = 'init'
        self.error_message = ''

        self.n_workers = self.vid_dl.info_dl['n_workers']

        self.count_init = 0
        self.count_repeats = 0

        self.last_progress_str = '--'

        self.ex_dl = ThreadPoolExecutor(thread_name_prefix='ex_aria2dl')

        self.premsg = (
            f'[{self.info_dict["id"]}][{self.info_dict["title"]}][{self.info_dict["format_id"]}]')

        self.prep_init()

    def prep_init(self):

        def getter(x):
            if not x:
                value, key_text = ('', '')
            else:
                value, key_text = try_get(
                    [(v, sk) for k, v in self._CONFIG.items()
                        for sk in k if sk == x], lambda y: y[0]
                ) or ('', '')

            if value:
                self.special_extr = True
                limit = value['ratelimit'].ratelimit(key_text, delay=True)
                maxplits = value['maxsplits']
            else:
                self.special_extr = False
                limit = limiter_non.ratelimit('transp', delay=True)
                maxplits = self.n_workers

            _sem = False
            if maxplits <= 16:  # or x in ['']:
                _sem = True
                if x in CONF_ARIA2C_EXTR_GROUP:
                    self._mode = 'group'

            if x in CONF_AUTO_PASRES:
                self.auto_pasres = True
                self._min_check_speed = CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED // 2

            return (_sem, limit, maxplits)

        self._extractor = try_get(self.info_dict.get('extractor_key'), lambda x: x.lower())
        self.auto_pasres = False
        self.special_extr = False
        self._mode = 'simple'
        self._min_check_speed = CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED
        self._n_check_speed = CONF_ARIA2C_N_CHUNKS_CHECK_SPEED
        _sem, self._decor, self._nsplits = getter(self._extractor)

        # self.sync_to_async = lambda x: sync_to_async(x, thread_sensitive=False, executor=self.ex_dl)

        if not self.enproxy:
            self._mode = 'noproxy'

        self.n_workers = self._nsplits
        if self.filesize:
            _nsplits = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
            if _nsplits < self.n_workers:
                self.n_workers = _nsplits

        opts_dict = {
            'split': self.n_workers,
            'header': '\n'.join(
                [f'{key}: {value}'
                    for key, value in self.headers.items()]),
            'dir': str(self.download_path),
            'out': self.filename.name,
            'uri-selector': 'inorder',
            'min-split-size': CONF_ARIA2C_MIN_SIZE_SPLIT,
        }

        self.config_client(opts_dict)

        if _sem and self._mode == 'noproxy':

            with self.ytdl.params.setdefault('lock', Lock()):
                self.ytdl.params.setdefault('sem', {})
                self.sem = cast(Lock, self.ytdl.params['sem'].setdefault(self._host, Lock()))
        else:
            self.sem = contextlib.nullcontext()

        self.block_init = True

    def config_client(self, opts_dict):
        self.opts = AsyncARIA2CDownloader.aria2_client.get_global_options()
        for key, value in opts_dict.items():
            if not isinstance(value, list):
                values = [value]
            else:
                values = value
            for value in values:
                rc = self.opts.set(key, value)
                if not rc:
                    logger.warning(f'{self.premsg} couldnt set [{key}] to [{value}]')

    async def _acall(self, func, /, *args, **kwargs):
        try:
            return await sync_to_async(func, thread_sensitive=False, executor=self.ex_dl)(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            logger.warning(f'{self.premsg}[acall][{func}] error: {type(e)}')
            if 'add_uris' in func.__name__:
                raise AsyncARIA2CDLErrorFatal('add uris fails')
            _res = await self.reset_aria2c()
            if _res:
                await self.vid_dl.reset()
                return {'reset': 'ok'}
            else:
                return {'error': AsyncARIA2CDLErrorFatal('reset failed')}
        except aria2p.ClientException as e:
            logger.warning(f'{self.premsg}[acall][{func}] error: {type(e)}')
            await self.vid_dl.reset()
            return {'reset': 'ok'}
        except Exception as e:
            logger.exception(f'{self.premsg}[acall][{func}] error: {repr(e)}')
            return {'error': e}

    async def reset_aria2c(self):

        async with AsyncARIA2CDownloader._CLASSALOCK:
            try:
                AsyncARIA2CDownloader.aria2_client.get_stats()
                logger.info(f'{self.premsg}[reset_aria2c] test conn ok')
                return True
            except Exception:
                logger.info(f'{self.premsg}[reset_aria2c] test conn no ok, lets reset aria2c')
                res, _port = await self.vid_dl.info_dl['nwsetup'].reset_aria2c()
                logger.info(f'{self.premsg}[reset_aria2c] {_port} {res}')
                AsyncARIA2CDownloader.aria2_client.client.port = _port
                try:
                    AsyncARIA2CDownloader.aria2_client.get_stats()
                    logger.info(f'{self.premsg}[reset_aria2c] after reset, test conn ok')
                    return True
                except Exception:
                    logger.info(f'{self.premsg}[reset_aria2c]  test conn no ok')

    async def async_pause(self, list_dl: list[Union[None, aria2p.Download]]):
        if list_dl:
            await self._acall(AsyncARIA2CDownloader.aria2_client.pause, list_dl)

    async def async_resume(self, list_dl):
        if list_dl:
            async with self._decor:
                await self._acall(AsyncARIA2CDownloader.aria2_client.resume, list_dl)

    async def async_remove(self, list_dl, clean=False):
        if list_dl:
            await self._acall(AsyncARIA2CDownloader.aria2_client.remove, list_dl, clean=clean)

    async def async_restart(self, list_dl, clean=False):
        if list_dl:
            await self._acall(AsyncARIA2CDownloader.aria2_client.remove, list_dl, files=True, clean=clean)

    async def add_uris(self, uris: list[str]) -> Union[aria2p.Download, None]:
        async with self._decor:
            return await self._acall(AsyncARIA2CDownloader.aria2_client.add_uris, uris, options=self.opts)  # type: ignore

    async def aupdate(self, dl_cont):
        if dl_cont:
            await self._acall(dl_cont.update)

    @retry
    async def init(self):

        try:
            if self._mode == 'noproxy':
                self._index_proxy = -1
                self._proxy = None
                self.uris = [unquote(self.info_dict.get('url'))] * self.n_workers

            else:
                self._proxy = None

                async with self.vid_dl.master_hosts_alock:
                    if not self.vid_dl.hosts_dl.get(self._host):
                        _seq = random.sample(range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST)
                        queue_ = put_sequence(asyncio.Queue(), _seq)
                        self.vid_dl.hosts_dl.update({self._host: {'count': 0, 'queue': queue_}})

                _res = await async_waitfortasks(
                        self.vid_dl.hosts_dl[self._host]['queue'].get(),
                        events=(self.vid_dl.reset_event, self.vid_dl.stop_event),
                        background_tasks=self.background_tasks)

                if _res.get('event'):
                    return
                elif (_e := _res.get('exception')):
                    raise AsyncARIA2CDLError(f'couldnt get index proxy: {repr(_e)}')
                else:
                    self._index_proxy = _res.get('result', -1)
                    if (self._index_proxy is None or
                        not isinstance(self._index_proxy, int) or
                            self._index_proxy == -1):

                        raise AsyncARIA2CDLError(f'couldnt get index proxy: {self._index_proxy}')

                    self.vid_dl.hosts_dl[self._host]['count'] += 1

                _init_url = self.info_dict.get('webpage_url')
                if self.special_extr:
                    _init_url = smuggle_url(_init_url, {'indexdl': self.vid_dl.index})

                if self._mode == 'simple':
                    _proxy_port = CONF_PROXIES_BASE_PORT+self._index_proxy*100
                    self._proxy = f'http://127.0.0.1:{_proxy_port}'
                    self.opts.set('all-proxy', self._proxy)

                    try:
                        _ytdl_opts = self.ytdl.params.copy()

                        async with ProxyYTDL(opts=_ytdl_opts, proxy=self._proxy, executor=self.ex_dl) as proxy_ytdl:

                            proxy_info = get_format_id(
                                proxy_ytdl.sanitize_info(
                                    await proxy_ytdl.async_extract_info(_init_url)),
                                self.info_dict['format_id'])

                        _proxy_info_url = try_get(
                            traverse_obj(proxy_info, ('url')), lambda x: unquote(x) if x else None)
                        logger.debug(
                            f'{self.premsg} mode simple, proxy ip: ' +
                            f'{self._proxy} init uri: {_proxy_info_url}\n{proxy_info}')
                        if not _proxy_info_url:
                            raise AsyncARIA2CDLError('couldnt get video url')
                        self.uris = [_proxy_info_url]

                    except Exception as e:
                        logger.warning(
                            f'{self.premsg} mode simple, proxy ip: ' +
                            f'{self._proxy} init uri: {repr(e)}')
                        self.uris = [None]

                    self.opts.set("split", self.n_workers)
                    await asyncio.sleep(0)

                elif self._mode == 'group':
                    _port = CONF_PROXIES_BASE_PORT+self._index_proxy*100+50
                    self._proxy = f'http://127.0.0.1:{_port}'
                    self.opts.set('all-proxy', self._proxy)

                    _uris = []

                    if self.filesize:
                        _n = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
                        _gr = _n // self._nsplits or 1
                        if _gr > CONF_PROXIES_N_GR_VIDEO:
                            _gr = CONF_PROXIES_N_GR_VIDEO
                    else:
                        _gr = CONF_PROXIES_N_GR_VIDEO

                    self.n_workers = _gr * self._nsplits
                    self.opts.set('split', self.n_workers)

                    async def get_uri(i):

                        assert isinstance(self._index_proxy, int)

                        _ytdl_opts = self.ytdl.params.copy()
                        _proxy_port = CONF_PROXIES_BASE_PORT + self._index_proxy*100+i
                        _proxy = f'http://127.0.0.1:{_proxy_port}'
                        logger.debug(f'{self.premsg} proxy ip{i} {_proxy}')

                        try:

                            async with ProxyYTDL(opts=_ytdl_opts, proxy=_proxy, executor=self.ex_dl) as proxy_ytdl:

                                proxy_info = get_format_id(
                                    proxy_ytdl.sanitize_info(await proxy_ytdl.async_extract_info(_init_url)),
                                    self.info_dict['format_id'])

                            _url = try_get(proxy_info['url'], lambda x: unquote(x) if x else None)

                            logger.debug(f"{self.premsg} ip{i}{_proxy} uri{i} {_url}")

                            if not _url:
                                raise AsyncARIA2CDLError('couldnt get video url')

                            _url_as_dict = urlparse(_url)._asdict()
                            _temp = f"__routing={_proxy_port}__.{_url_as_dict['netloc']}"
                            _url_as_dict["netloc"] = _temp
                            return urlunparse(list(_url_as_dict.values()))

                        except Exception as e:
                            _msg = f"hst[{self._host}]pr[{i}:{_proxy}]{str(e)}"
                            logger.debug(
                                f"{self.premsg}[{self.info_dict.get('original_url')}] ERROR init uris {_msg}")
                            raise
                        finally:
                            await asyncio.sleep(0)

                    _tasks = {self.add_task(get_uri(i)): i for i in range(1, _gr + 1)}

                    _res = await async_waitfortasks(
                        _tasks, events=(self.vid_dl.reset_event, self.vid_dl.stop_event),
                        background_tasks=self.background_tasks)

                    if _res.get('event'):
                        return
                    elif (_e := _res.get('exception')):
                        raise AsyncARIA2CDLError(f'couldnt get uris: {repr(_e)}')
                    else:
                        _uris = _res.get('result', [])

                    assert _uris and isinstance(_uris, list)

                    self.uris = _uris * self._nsplits

            logger.debug(
                f'{self.premsg} proxy {self._proxy} count_init: ' +
                f'{self.count_init} uris:\n{self.uris}')

            self.uris = cast(list[str], self.uris)
            self.dl_cont = None

            if (_dl := await self.add_uris(self.uris)):

                self.dl_cont = _dl
                _tstart = time.monotonic()

                while True:

                    if (event := traverse_obj(await self.event_handle(), 'event')):
                        if event in ("stop", "reset"):
                            return

                    elif self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):

                        if (resupt := await self.aupdate(self.dl_cont)):
                            if 'error' in resupt:
                                raise AsyncARIA2CDLError('init error: error update dl_cont')
                            elif 'reset' in resupt:
                                self.vid_dl.reset_event.clear()
                                raise AsyncARIA2CDLError('init reset')

                        if self.dl_cont.total_length or self.dl_cont.status == "complete":
                            break

                        if self.dl_cont.status == "error" or (time.monotonic() - _tstart > CONF_ARIA2C_TIMEOUT_INIT):
                            if self.dl_cont.status == "error":
                                _msg_error = self.dl_cont.error_message
                            else:
                                _msg_error = "timeout"

                            raise AsyncARIA2CDLError(f"init error {_msg_error}")

                    await asyncio.sleep(0)

                if self.dl_cont.status == "complete":
                    self._speed.append((datetime.now(), "complete"))
                    self.status = "done"

                if self.dl_cont.total_length:
                    if not self.filesize:
                        self.filesize = self.dl_cont.total_length
                        async with self.vid_dl.alock:
                            self.vid_dl.info_dl["filesize"] = self.filesize

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if self._mode != "noproxy":
                _msg = f'host: {self._host} proxy: {self._proxy} count: ' +\
                       f'{self.vid_dl.hosts_dl[self._host]["count"]}'
            else:
                _msg = ""
            if self.dl_cont and self.dl_cont.status == "error":
                _msg_error = f"{repr(e)} - {self.dl_cont.error_message}"
            else:
                _msg_error = repr(e)

            self.error_message = _msg_error
            self.count_init += 1

            if self.count_init < 3:
                if "estado=403" in _msg_error:
                    logger.warning(
                        f'{self.premsg}[init] {_msg} error: {_msg_error} count_init: {self.count_init}, will RESET')
                else:
                    logger.debug(
                        f'{self.premsg}[init] {_msg} error: {_msg_error} count_init: {self.count_init}, will RESET')

                self._speed.append((datetime.now(), 'resetinit'))

                await self.async_remove([self.dl_cont])
                await asyncio.sleep(0)

                raise AsyncARIA2CDLErrorFatal('repeat init')

            else:
                self._speed.append((datetime.now(), "error"))
                self.status = "error"
                logger.error(
                    f'{self.premsg}[init] {_msg} error: {_msg_error} count_init max: {self.count_init}')

    async def check_speed(self):
        def getter(x):
            if x <= 2:
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 0.25
            elif x <= (self.n_workers // 2):
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 1.25
            else:
                return x * CONF_ARIA2C_SPEED_PER_CONNECTION * 5

        def len_ap_list(_list, el):
            _list.append(el)
            return len(_list)

        def perc_below(_list):
            total = len(_list)
            _below = sum([1 for el in _list if el[0] < getter(el[1])])
            return int((_below/total)*100)

        _speed = []

        _index = self._n_check_speed
        _min_check = self._min_check_speed

        try:
            while True:
                _res = await async_waitfortasks(
                    self._qspeed.get(), events=(self.vid_dl.reset_event, self.vid_dl.stop_event),
                    background_tasks=self.background_tasks)

                if _res.get('event'):
                    logger.debug(f'{self.premsg}[check_speed] event set {_res.get("event")}')
                    break
                elif (_e := _res.get('exception')):
                    logger.debug(f'{self.premsg}[check_speed] exception from wait {repr(_e)}')
                    raise AsyncARIA2CDLError(f'couldnt get index proxy: {repr(_e)}')
                elif not (_input_speed := _res.get('result')):
                    await asyncio.sleep(0)
                    continue
                # elif isinstance(_input_speed, tuple) and isinstance(
                #         _input_speed[0], str) and _input_speed[0] == 'KILL':
                elif _input_speed == kill_item:
                    logger.debug(f'{self.premsg}[check_speed] KIKLL from queue')
                    break
                elif self.block_init:
                    await asyncio.sleep(0)
                    continue
                elif len_ap_list(_speed, _input_speed) > _min_check:

                    if any(
                            [
                                all([el[0] == 0 for el in _speed[-_index:]]),
                                all([self.n_workers > 1, all([el[1] <= 1 for el in _speed[-_index:]])]),
                                # all([el[0] < getter(el[1]) for el in _speed[-_index:]])
                                perc_below(_speed[-_index:]) >= 50
                            ]):

                        def _print_el(el):
                            if isinstance(el, str):
                                return el
                            else:
                                return f"['speed': {el[0]}, 'connec': {el[1]}]"

                        def _strdate(el):
                            _secs = el[2].second + el[2].microsecond / 1000000
                            return f'{el[2].strftime("%H:%M:")}{_secs:06.3f}'

                        _str_speed = ', '.join([f'({_strdate(el)}, {_print_el(el)})' for el in _speed[-_index:]])

                        logger.debug(
                            f'{self.premsg}[check_speed] speed reset: n_el_speed[{len(_speed)}]')
                        logger.debug(
                            f'{self.premsg}[check_speed] speed reset\n{_str_speed}')

                        await self.vid_dl.reset()

                        break

                    else:
                        _speed[0:_min_check - _index//2 + 1] = ()
                        await asyncio.sleep(0)

                else:
                    await asyncio.sleep(0)

            await asyncio.sleep(0)

        except Exception as e:
            logger.exception(
                f'{self.premsg}[check_speed] {str(e)}')
        finally:
            logger.debug(
                f'{self.premsg}[check_speed] bye')

    def add_task(self, coro):
        _task = asyncio.create_task(coro)
        self.background_tasks.add(_task)
        _task.add_done_callback(self.background_tasks.discard)
        return _task

    async def event_handle(self, status=None):

        _res = {}
        if self.vid_dl.pause_event.is_set():
            self._speed.append((datetime.now(), "pause"))
            await self.async_pause([self.dl_cont])
            await asyncio.sleep(0)
            _res = await async_waitfortasks(
                events=(self.vid_dl.resume_event, self.vid_dl.reset_event, self.vid_dl.stop_event),
                background_tasks=self.background_tasks)
            await self.async_resume([self.dl_cont])
            self.vid_dl.pause_event.clear()
            self.vid_dl.resume_event.clear()
            self._speed.append((datetime.now(), "resume"))
            await asyncio.sleep(0)
            self.progress_timer.reset()
        else:
            _event = [_ev.name for _ev in (self.vid_dl.reset_event, self.vid_dl.stop_event) if _ev.is_set()]
            if _event:
                _res = {"event": _event[0]}
                await asyncio.sleep(0)
                self.progress_timer.reset()

        if status and (event := _res.get("event")):
            if event == 'stop':
                self._speed.append((datetime.now(), 'stop'))
                if self.status == 'stop':
                    return {"event": "exit"}
                self.vid_dl.stop_event.clear()
                await self.async_restart([self.dl_cont])
                async with self.vid_dl.alock:
                    self.vid_dl.info_dl['down_size'] -= self.down_size
                self.down_size = 0

            elif event == 'reset':
                self._speed.append((datetime.now(), 'reset'))
                self.vid_dl.reset_event.clear()
                await self.async_remove([self.dl_cont])

        return _res

    async def fetch(self):

        self.count_init = 0
        self.block_init = False

        try:
            assert self.dl_cont
            while self.dl_cont.status in ['active', 'paused', 'waiting']:

                if (event := traverse_obj(await self.event_handle(), 'event')):
                    if event in ("stop", "reset"):
                        return

                elif self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):

                    if (_res := await self.aupdate(self.dl_cont)):
                        if 'error' in _res:
                            raise AsyncARIA2CDLError('fetch error: error update dl_cont')
                        elif 'reset' in _res:
                            return

                    self._qspeed.put_nowait(
                        (self.dl_cont.download_speed, self.dl_cont.connections, datetime.now()))

                    _incsize = self.dl_cont.completed_length - self.down_size
                    self.down_size = self.dl_cont.completed_length
                    async with self.vid_dl.alock:
                        self.vid_dl.info_dl['down_size'] += _incsize

                await asyncio.sleep(0)

            if self.dl_cont:
                if self.dl_cont.status == 'complete':
                    self._speed.append((datetime.now(), 'complete'))
                    self.status = 'done'

                if self.dl_cont.status == 'error':
                    raise AsyncARIA2CDLError('fetch error')
            else:
                raise AsyncARIA2CDLError('fetch error')

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            _msg = ''
            if self._mode != 'noproxy':
                _msg = f'host: {self._host} proxy: {self._proxy} count: ' +\
                       f'{self.vid_dl.hosts_dl[self._host]["count"]}'
            _msg_error = repr(e)
            if self.dl_cont and self.dl_cont.status == 'error':
                _msg_error = f'{repr(e)} - {self.dl_cont.error_message}'

            logger.error(f'{self.premsg}[fetch] {_msg} error: {_msg_error}')
            self._speed.append((datetime.now(), 'error'))
            self.status = 'error'
            self.error_message = _msg_error

    async def fetch_async(self):

        self.progress_timer = ProgressTimer()
        self.status = 'downloading'
        self._speed = []

        try:

            while True:
                async with async_lock(self.sem):
                    try:
                        self._speed.append((datetime.now(), 'init'))
                        init_task = self.add_task(self.init())
                        await asyncio.wait([init_task])
                        if self.status in ('done', 'error'):
                            return
                        if (event := traverse_obj(await self.event_handle(status='downloading'), 'event')):
                            if event == 'exit':
                                return
                            elif event in ('stop', 'reset'):
                                self.block_init = True
                                continue

                        self._qspeed = asyncio.Queue()
                        check_task = self.add_task(self.check_speed())
                        self._speed.append((datetime.now(), 'fetch'))

                        try:

                            fetch_task = self.add_task(self.fetch())
                            await asyncio.wait([fetch_task])
                            await asyncio.sleep(0)
                            if self.status in ('done', 'error'):
                                return
                            if (event := traverse_obj(await self.event_handle(status='downloading'), 'event')):
                                if event == 'exit':
                                    return
                                elif event in ('stop', 'reset'):
                                    self.block_init = True
                                    continue
                        finally:
                            self._qspeed.put_nowait(kill_item)
                            await asyncio.sleep(0)
                            await asyncio.wait([check_task])

                    except BaseException as e:
                        if isinstance(e, KeyboardInterrupt):
                            raise

                        _msg = ''
                        if self._mode != 'noproxy':
                            _msg = f'host: {self._host} proxy: {self._proxy} ' +\
                                   f'count: {self.vid_dl.hosts_dl[self._host]["count"]}'
                        _msg_error = repr(e)
                        if self.dl_cont and self.dl_cont.status == 'error':
                            _msg_error = f'{repr(e)} - {self.dl_cont.error_message}'

                        logger.error(f'{self.premsg}[fetch_async] {_msg} error: {_msg_error}')
                        self.status = 'error'
                        self.error_message = _msg_error

                    finally:
                        if all([
                                self._mode != 'noproxy', hasattr(self, '_index_proxy'),
                                self._index_proxy, isinstance(self._index_proxy, int),
                                self._index_proxy >= 0]):  # type: ignore

                            async with self.vid_dl.master_hosts_alock:
                                self.vid_dl.hosts_dl[self._host]['count'] -= 1
                                self.vid_dl.hosts_dl[self._host][
                                    'queue'].put_nowait(
                                    self._index_proxy)
                            self._proxy = None

        except BaseException as e:
            logger.exception(f'{self.premsg}[fetch_async] {repr(e)}')
        finally:
            #  await self.async_remove([self.dl_cont], clean=True)

            def _print_el(el):
                if isinstance(el[1], str):
                    return el[1]
                else:
                    return f"['status': {el[1].status}, 'speed': {el[1].download_speed}]"

            def _strdate(el):
                _secs = el[0].second + el[0].microsecond / 1000000
                return f'{el[0].strftime("%H:%M:")}{_secs:06.3f}'

            _str_speed = ', '.join([f'({_strdate(el)}, {_print_el(el)})' for el in self._speed])

            logger.debug(f'{self.premsg}[fetch_async] exiting [{len(self._speed)}]\n{_str_speed}')

    def print_hookup(self):
        msg = ''

        if self.status == 'done':
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: Completed\n'
        elif self.status == 'init':
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: Waiting ' +\
                  f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
        elif self.status == 'error':
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: ERROR ' +\
                  f'{naturalsize(self.down_size, format_=".2f")} ' +\
                  f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
        elif self.status == 'stop':
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: STOPPED ' +\
                  f'{naturalsize(self.down_size, format_=".2f")} ' +\
                  f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
        elif self.status == 'downloading':
            if not any(
                [
                    self.block_init,
                    self.vid_dl.reset_event.is_set(),
                    self.vid_dl.stop_event.is_set(),
                    self.vid_dl.pause_event.is_set(),
                ]
            ) and self.dl_cont:

                _temp = copy.deepcopy(self.dl_cont)

                _speed_temp = naturalsize(
                    _temp.download_speed,
                    binary=True,
                    format_="6.2f")
                _speed_str = f'{_speed_temp}ps'
                _progress_str = f"{_temp.progress:.0f}%"
                self.last_progress_str = _progress_str
                _connections = _temp.connections
                _eta_str = _temp.eta_string()

                msg = f'[ARIA2C][{self.info_dict["format_id"]}]: HOST' +\
                      f'[{self._host.split(".")[0]}] ' +\
                      f'CONN[{_connections:2d}/{self.n_workers:2d}] ' +\
                      f'DL[{_speed_str}] PR[{_progress_str}] ETA[{_eta_str}]\n'

            else:

                if self.block_init:
                    _substr = 'INIT'
                elif self.vid_dl.pause_event.is_set():
                    _substr = 'PAUSED'
                elif self.vid_dl.reset_event.is_set():
                    _substr = 'RESET'
                elif self.vid_dl.stop_event.is_set():
                    _substr = 'STOPPED'
                else:
                    _substr = 'UNKNOWN'

                msg = f'[ARIA2C][{self.info_dict["format_id"]}]: ' +\
                      f'HOST[{self._host.split(".")[0]}]' +\
                      f'{_substr} DL PR[{self.last_progress_str}]\n'

        elif self.status == 'manipulating':
            if self.filename.exists():
                _size = self.filename.stat().st_size
            else:
                _size = 0
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: Ensambling ' +\
                  f'{naturalsize(_size, format_=".2f")}' +\
                  f'[{naturalsize(self.filesize, format_=".2f")}]\n'

        return msg
