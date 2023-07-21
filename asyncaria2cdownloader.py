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
from urllib.parse import (
    unquote,
    urlparse,
    urlunparse
)
from typing import (
    cast,
    Union
)
import aria2p
import requests
from utils import (
    CONF_AUTO_PASRES,
    CONF_ARIA2C_EXTR_GROUP,
    CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED,
    CONF_ARIA2C_MIN_SIZE_SPLIT,
    CONF_ARIA2C_N_CHUNKS_CHECK_SPEED,
    CONF_ARIA2C_SPEED_PER_CONNECTION,
    CONF_INTERVAL_GUI,
    CONF_PROXIES_BASE_PORT,
    CONF_PROXIES_MAX_N_GR_HOST,
    CONF_PROXIES_N_GR_VIDEO,
    load_config_extractors,
    getter_basic_config_extr,
    ProgressTimer,
    ProxyYTDL,
    async_lock,
    get_format_id,
    limiter_non,
    naturalsize,
    smuggle_url,
    sync_to_async,
    traverse_obj,
    try_get,
    myYTDL,
    async_waitfortasks,
    async_wait_for_any,
    put_sequence,
    my_dec_on_exception,
    get_host,
    variadic,
    LockType,
    Token

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

kill_token = Token("kill")


class AsyncARIA2CDownloader:

    _CONFIG = load_config_extractors()
    _CLASSALOCK = asyncio.Lock()
    _LOCK = Lock()
    _INIT = False
    aria2_API: aria2p.API

    def __init__(self, port, enproxy, video_dict, vid_dl):

        self.background_tasks = set()
        self.info_dict = video_dict.copy()
        self.vid_dl = vid_dl
        self.enproxy = enproxy

        with AsyncARIA2CDownloader._LOCK:
            if not AsyncARIA2CDownloader._INIT:
                AsyncARIA2CDownloader.aria2_API = aria2p.API(aria2p.Client(port=port, timeout=2))
                AsyncARIA2CDownloader._INIT = True

        self.ytdl: myYTDL = self.vid_dl.info_dl['ytdl']

        video_url = unquote(self.info_dict.get('url'))
        self.uris = cast(list[str], [video_url])
        self._extractor = try_get(self.info_dict.get('extractor_key'), lambda x: x.lower())
        self._host = get_host(video_url, shorten=(self._extractor == 'vgembed'))

        self.headers = self.info_dict.get('http_headers', {})

        self.download_path = self.info_dict['download_path']

        self.download_path.mkdir(parents=True, exist_ok=True)

        _filename = self.info_dict.get('_filename', self.info_dict.get('filename'))
        self.filename = Path(
            self.download_path,
            f'{_filename.stem}.{self.info_dict["format_id"]}.aria2c.{self.info_dict["ext"]}')

        self.dl_cont = None

        self.status = 'init'
        self.block_init = True
        self.error_message = ''

        self.n_workers = self.vid_dl.info_dl['n_workers']

        self.last_progress_str = '--'

        self.ex_dl = ThreadPoolExecutor(thread_name_prefix='ex_aria2dl')

        self.premsg = (
            f'[{self.info_dict["id"]}][{self.info_dict["title"]}][{self.info_dict["format_id"]}]')

        def getter(x):
            value, key_text = getter_basic_config_extr(x, AsyncARIA2CDownloader._CONFIG) or (None, None)
            if value and key_text:
                self.special_extr = True
                limit = value['ratelimit'].ratelimit(key_text, delay=True)
                maxplits = value['maxsplits']
            else:
                self.special_extr = False
                limit = limiter_non.ratelimit('transp', delay=True)
                maxplits = self.n_workers

            _sem = False
            if maxplits < 16:  # or x in ['']:
                _sem = True
                if x in CONF_ARIA2C_EXTR_GROUP:
                    self._mode = 'group'

            if x in CONF_AUTO_PASRES:
                self.auto_pasres = True
                self._min_check_speed = CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED // 2

            return (_sem, limit, maxplits)

        self.auto_pasres = False
        self.special_extr = False
        self._min_check_speed = CONF_ARIA2C_MIN_N_CHUNKS_DOWNLOADED_TO_CHECK_SPEED
        self._n_check_speed = CONF_ARIA2C_N_CHUNKS_CHECK_SPEED
        self._mode = 'simple'
        _sem, self._decor, self._nsplits = getter(self._extractor)

        if not self.enproxy:
            self._mode = 'noproxy'
        else:
            with self.vid_dl.master_hosts_lock:
                if not self.vid_dl.hosts_dl.get(self._host):
                    _seq = random.sample(range(CONF_PROXIES_MAX_N_GR_HOST), CONF_PROXIES_MAX_N_GR_HOST)
                    queue_ = put_sequence(asyncio.Queue(), _seq)
                    self.vid_dl.hosts_dl.update({self._host: {'count': 0, 'queue': queue_}})

        if _sem and self._mode == 'noproxy':

            with self.ytdl.params.setdefault('lock', Lock()):
                self.ytdl.params.setdefault('sem', {})
                self.sem = cast(LockType, self.ytdl.params['sem'].setdefault(self._host, Lock()))
        else:
            self.sem = contextlib.nullcontext()

        self.n_workers = self._nsplits
        self.down_size = 0
        self.filesize = self.info_dict.get('filesize')
        if not self.filesize:
            if _res := self._get_filesize(self.uris):
                self.filesize, self.down_size = _res[0], _res[1]
        if self.filesize:
            _nsplits = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
            if _nsplits < self.n_workers:
                self.n_workers = _nsplits

        _headers = self.headers.copy()

        opts_dict = {
            'split': self.n_workers,
            'header': '\n'.join(
                [f'{key}: {value}'
                    for key, value in _headers.items()]),
            'dir': str(self.download_path),
            'out': self.filename.name,
            'uri-selector': 'inorder',
            'min-split-size': CONF_ARIA2C_MIN_SIZE_SPLIT,
            'dry-run': 'false'
        }

        self.config_client(opts_dict)

    def config_client(self, opts_dict):
        self.opts = AsyncARIA2CDownloader.aria2_API.get_global_options()
        for key, value in opts_dict.items():
            if not isinstance(value, list):
                values = [value]
            else:
                values = value
            for el in values:
                rc = self.opts.set(key, el)
                if not rc:
                    logger.warning(f'{self.premsg} couldnt set [{key}] to [{value}]')

    def _get_filesize(self, uris) -> Union[tuple, None]:
        try:
            opts_dict = {
                'header': '\n'.join(
                    [f'{key}: {value}'
                        for key, value in self.headers.items()]),
                'dry-run': 'true',
                'dir': str(self.download_path),
                'out': self.filename.name
            }
            opts = AsyncARIA2CDownloader.aria2_API.get_global_options()
            for key, value in opts_dict.items():
                if not isinstance(value, list):
                    values = [value]
                else:
                    values = value
                for el in values:
                    opts.set(key, el)

            with self.sem:
                with self._decor:
                    _res = AsyncARIA2CDownloader.aria2_API.add_uris(uris, options=opts)
                filesize = None
                downsize = None
                start = time.monotonic()
                while (time.monotonic() - start < 5):
                    time.sleep(1)
                    _res.update()
                    if filesize := int(_res.total_length):
                        downsize = int(_res.completed_length)
                        break
                AsyncARIA2CDownloader.aria2_API.remove([_res])
                logger.debug(f'{self.premsg}[get_filesize] {filesize} {downsize}')
                if filesize:
                    return (filesize, downsize)
        except Exception as e:
            logger.exception(f'{self.premsg}[get_filesize] error: {repr(e)}')

    async def _acall(self, func, /, *args, **kwargs):
        '''
        common async executor of aria2 client requests to the server rpc
        '''
        try:
            return await sync_to_async(func, thread_sensitive=False, executor=self.ex_dl)(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            logger.warning(f'{self.premsg}[acall][{func}] error: {type(e)}')
            if 'add_uris' in func.__name__:
                raise AsyncARIA2CDLErrorFatal('add uris fails')
            if await self.reset_aria2c():
                return {'reset': 'ok'}
            else:
                return {'error': AsyncARIA2CDLErrorFatal('reset failed')}
        except aria2p.ClientException as e:
            logger.warning(f'{self.premsg}[acall][{func}] error: {type(e)}')
            if 'add_uris' in func.__name__:
                raise AsyncARIA2CDLErrorFatal('add uris fails')
            return {'reset': 'ok'}
        except Exception as e:
            logger.exception(f'{self.premsg}[acall][{func}] error: {repr(e)}')
            return {'error': e}

    async def reset_aria2c(self):

        async with AsyncARIA2CDownloader._CLASSALOCK:
            try:
                AsyncARIA2CDownloader.aria2_API.get_stats()
                logger.info(f'{self.premsg}[reset_aria2c] test conn ok')
                return True
            except Exception:
                logger.info(f'{self.premsg}[reset_aria2c] test conn no ok, lets reset aria2c')
                res, _port = await self.vid_dl.info_dl['nwsetup'].reset_aria2c()
                logger.info(f'{self.premsg}[reset_aria2c] {_port} {res}')
                AsyncARIA2CDownloader.aria2_API.client.port = _port
                try:
                    AsyncARIA2CDownloader.aria2_API.get_stats()
                    logger.info(f'{self.premsg}[reset_aria2c] after reset, test conn ok')
                    return True
                except Exception:
                    logger.info(f'{self.premsg}[reset_aria2c]  test conn no ok')

    async def async_pause(self, list_dl: list[Union[None, aria2p.Download]]):
        if list_dl:
            await self._acall(AsyncARIA2CDownloader.aria2_API.pause, list_dl)

    async def async_resume(self, list_dl: list[Union[None, aria2p.Download]]):
        if list_dl:
            async with self._decor:
                await self._acall(AsyncARIA2CDownloader.aria2_API.resume, list_dl)

    async def async_remove(self, list_dl: list[Union[None, aria2p.Download]]):
        if list_dl:
            await self._acall(AsyncARIA2CDownloader.aria2_API.remove, list_dl, clean=False)

    async def async_restart(self, list_dl: list[Union[None, aria2p.Download]]):
        if list_dl:
            await self._acall(AsyncARIA2CDownloader.aria2_API.remove, list_dl, files=True, clean=True)

    async def add_uris(self, uris: list[str]) -> Union[aria2p.Download, None]:
        async with self._decor:
            return await self._acall(AsyncARIA2CDownloader.aria2_API.add_uris, uris, options=self.opts)  # type: ignore

    async def aupdate(self, dl_cont):
        if dl_cont:
            return await self._acall(dl_cont.update)

    def uptpremsg(self):
        _upt = f"{self.premsg} host: {self._host}"
        if self._mode != "noproxy":
            _upt += f"proxy: {self._proxy} count: {self.vid_dl.hosts_dl[self._host]['count']}"
        return _upt

    async def init(self):

        dl_cont = None
        self._speed.append((datetime.now(), 'init'))
        try:

            if (dl_cont := await self.add_uris(self.uris)):

                if (resupt := await self.aupdate(dl_cont)):
                    if any([_ in resupt for _ in ('error', 'reset')]):
                        raise AsyncARIA2CDLError('init: error update dl_cont')

            return dl_cont

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            if dl_cont and dl_cont.status == "error":
                _msg_error = f"{repr(e)} - {dl_cont.error_message}"
            else:
                _msg_error = repr(e)
            self.error_message = _msg_error
            logger.exception(f"{self.uptpremsg()}[init] error: {_msg_error}")
            self.status = "error"

    async def update_uri(self):
        _init_url = self.info_dict.get('webpage_url')
        if self.special_extr:
            _init_url = smuggle_url(_init_url, {'indexdl': self.vid_dl.index})

        if self._mode == 'noproxy':

            if self.n_rounds > 1:

                async with ProxyYTDL(opts=self.ytdl.params.copy(), executor=self.ex_dl) as proxy_ytdl:
                    proxy_info = get_format_id(
                        proxy_ytdl.sanitize_info(
                            await proxy_ytdl.async_extract_info(_init_url)),
                        self.info_dict['format_id'])
                if (_url := proxy_info.get("url")):
                    video_url = unquote(_url)
                    self.uris = [video_url]
                    if (_host := get_host(video_url, shorten=(self._extractor == 'vgembed'))) != self._host:
                        self._host = _host
                        if isinstance(self.sem, LockType):
                            async with async_lock(self.ytdl.params['lock']):
                                self.sem = cast(LockType, self.ytdl.params['sem'].setdefault(self._host, Lock()))
                else:
                    raise AsyncARIA2CDLError('couldnt get video url')

        else:
            _res = await async_waitfortasks(
                self.vid_dl.hosts_dl[self._host]['queue'].get(),
                events=(self.vid_dl.reset_event, self.vid_dl.stop_event),
                background_tasks=self.background_tasks)

            if 'event' in _res:
                return
            elif (_temp := _res.get('exception')) or (_temp := _res.get('result')) is None:
                raise AsyncARIA2CDLError(f'couldnt get index proxy: {repr(_temp)}')
            else:
                self._index_proxy = cast(int, _temp)
                async with self.vid_dl.master_hosts_alock():
                    self.vid_dl.hosts_dl[self._host]['count'] += 1

            _uris: list[str] = []

            if self._mode == 'simple':
                _proxy_port = CONF_PROXIES_BASE_PORT + self._index_proxy * 100
                self._proxy = f'http://127.0.0.1:{_proxy_port}'
                self.opts.set('all-proxy', self._proxy)

                try:
                    async with ProxyYTDL(opts=self.ytdl.params.copy(), proxy=self._proxy, executor=self.ex_dl) as proxy_ytdl:

                        proxy_info = get_format_id(
                            proxy_ytdl.sanitize_info(
                                await proxy_ytdl.async_extract_info(_init_url)),
                            self.info_dict['format_id'])

                    _proxy_info_url = cast(str, try_get(
                        traverse_obj(proxy_info, ('url')), lambda x: unquote(x) if x else None))
                    logger.debug(f"{self.uptpremsg()} [update_uri]{_proxy_info_url}\n{proxy_info}")
                    if not _proxy_info_url:
                        raise AsyncARIA2CDLError('couldnt get video url')

                    _uris.append(_proxy_info_url)

                except Exception as e:
                    logger.warning(f"{self.uptpremsg()} [update_uri] {repr(e)}")
                    raise AsyncARIA2CDLError(f'couldnt get uris: {repr(e)}')

                self.opts.set("split", self.n_workers)
                # await asyncio.sleep(0)

            elif self._mode == 'group':
                _port = CONF_PROXIES_BASE_PORT + self._index_proxy * 100 + 50
                self._proxy = f'http://127.0.0.1:{_port}'
                self.opts.set('all-proxy', self._proxy)

                if self.filesize:
                    _n = self.filesize // CONF_ARIA2C_MIN_SIZE_SPLIT or 1
                    _gr = _n // self._nsplits or 1
                    if _gr > CONF_PROXIES_N_GR_VIDEO:
                        _gr = CONF_PROXIES_N_GR_VIDEO
                else:
                    _gr = CONF_PROXIES_N_GR_VIDEO

                self.n_workers = _gr * self._nsplits
                self.opts.set('split', self.n_workers)

                logger.info(
                    f'{self.premsg} enproxy {self.enproxy} mode {self._mode} proxy {self._proxy} _gr {_gr} n_workers {self.n_workers} ')

                async def get_uri(i):

                    assert isinstance(self._index_proxy, int)

                    _ytdl_opts = self.ytdl.params.copy()
                    _proxy_port = CONF_PROXIES_BASE_PORT + self._index_proxy * 100 + i
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
                        _url_as_dict["netloc"] = _temp.replace('__.www', '__.')
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

                logger.debug(f'{self.premsg} {_res}')

                if _res.get('event'):
                    return
                elif (_temp := _res.get('exception')) or not (_temp := _res.get('result')):
                    raise AsyncARIA2CDLError(f'couldnt get uris: {repr(_temp)}')
                else:
                    _uris.extend(cast(list[str], variadic(_temp)))

                #  self.uris = _uris * self._nsplits
            self.uris = _uris

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

        def perc_below(_list, _max):
            total = len(_list)
            _below = sum([1 for el in _list if el[0] < getter(el[1])])
            _perc = int((_below / total) * 100)
            if _perc > _max:
                return _perc

        _speed = []

        _index = self._n_check_speed
        _min_check = self._min_check_speed

        try:
            while True:
                _res = await async_waitfortasks(
                    self._qspeed.get(),
                    events=self.vid_dl.stop_event,
                    background_tasks=self.background_tasks)
                if 'event' in _res:
                    logger.debug(f'{self.premsg}[check_speed] stop event')
                    return
                elif (_temp := _res.get('exception')):
                    raise AsyncARIA2CDLError(f'error when get from qspeed: {repr(_temp)}')
                else:
                    _input_speed = _res.get('result')

                if _input_speed == kill_token:
                    logger.debug(f'{self.premsg}[check_speed] {kill_token} from queue')
                    return
                elif any([_input_speed is None, self.block_init, self.check_any_event_is_set()]):
                    continue
                elif len_ap_list(_speed, _input_speed) > _min_check:

                    _res_dl0 = False
                    _res_ncon = False
                    _res_perc = False

                    if any(
                            [
                                _res_dl0 := all([el[0] == 0 for el in _speed[-_index:]]),
                                _res_ncon := all([self.n_workers > 1, all([el[1] < self.n_workers - 1 for el in _speed[-_index:]])]),
                                #  _res_perc := perc_below(_speed[-_index:], 80)
                            ]):

                        def _print_el(item: tuple) -> str:
                            _secs = item[2].second + item[2].microsecond / 1000000
                            return f"({item[2].strftime('%H:%M:')}{_secs:06.3f}, ['speed': {item[0]}, 'connec': {item[1]}])"

                        _str_speed = ', '.join([_print_el(el) for el in _speed[-_index:]])

                        # logger.debug(f'{self.premsg}[check_speed] speed reset: n_el_speed[{len(_speed)}]')
                        logger.info(f'{self.premsg}[check_speed] speed reset: n_el_speed[{len(_speed)}] dl0[{_res_dl0}] ncon[{_res_ncon}] perc[{_res_perc}]')  # \n{_str_speed}')
                        logger.debug(f'{self.premsg}[check_speed]\n{_str_speed}')

                        await self.vid_dl.reset()
                        _speed = []
                        await asyncio.sleep(0)

                    else:
                        _speed[0:_min_check - _index // 2 + 1] = ()
                        await asyncio.sleep(0)

                else:
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

    def check_any_event_is_set(self):
        return any([self.vid_dl.pause_event.is_set(), self.vid_dl.reset_event.is_set(),
                    self.vid_dl.stop_event.is_set()])

    async def event_handle(self):

        _res = {}
        if self.vid_dl.pause_event.is_set():

            self._speed.append((datetime.now(), "pause"))
            await self.async_pause([self.dl_cont])
            await asyncio.sleep(0)
            _res = await async_wait_for_any(
                [self.vid_dl.resume_event, self.vid_dl.reset_event, self.vid_dl.stop_event])
            await self.async_resume([self.dl_cont])
            self.vid_dl.pause_event.clear()
            self.vid_dl.resume_event.clear()
            if "resume" in _res["event"]:
                _res["event"].remove("resume")
                if not _res["event"]:
                    _res = {}
            self._speed.append((datetime.now(), "resume"))
            await asyncio.sleep(0)
            self.progress_timer.reset()
        else:
            if (_event := [_ev.name for _ev in (self.vid_dl.reset_event, self.vid_dl.stop_event)
                           if _ev.is_set()]):

                _res = {"event": _event}
                self.progress_timer.reset()

        if _event := _res.get("event"):
            if "stop" in _event:
                self.status = "stop"
            if "reset" in _event:
                self.vid_dl.reset_event.clear()

            await asyncio.sleep(0)
            await self.async_remove([self.dl_cont])
            await asyncio.sleep(0)

        return _res

    async def error_handle(self, error):
        if error == '471':
            await asyncio.sleep(30)
        await self.async_remove([self.dl_cont])
        await asyncio.sleep(0)

    async def fetch(self):

        self._speed.append((datetime.now(), 'fetch'))
        try:
            self.dl_cont = await self.init()
            self.block_init = False
            if self.status in ('done', 'error', 'stop'):
                return

            if not self.dl_cont:
                raise AsyncARIA2CDLErrorFatal('could get dl_cont')

            while True:

                if self.dl_cont.status not in ['active', 'paused', 'waiting']:
                    break

                if self.progress_timer.has_elapsed(seconds=CONF_INTERVAL_GUI / 2):
                    if (traverse_obj(await self.event_handle(), 'event')):
                        return

                    _result = await self.aupdate(self.dl_cont) or {}
                    if 'error' in _result:
                        raise AsyncARIA2CDLError('fetch error: error update dl_cont')
                    elif 'reset' in _result:
                        await self.vid_dl.reset()
                    else:
                        self._qspeed.put_nowait(
                            (self.dl_cont.download_speed, self.dl_cont.connections, datetime.now()))

                        _incsize = self.dl_cont.completed_length - self.down_size
                        self.down_size = self.dl_cont.completed_length
                        async with self.vid_dl.alock:
                            self.vid_dl.info_dl['down_size'] += _incsize

                else:
                    await asyncio.sleep(0)

            if self.dl_cont.status == 'complete':
                self._speed.append((datetime.now(), "complete"))
                self.status = 'done'

            elif self.dl_cont.status == 'error':
                if 'status=471' in cast(str, self.dl_cont.error_message):
                    await self.error_handle('471')

                else:
                    raise AsyncARIA2CDLError('fetch error')

        except BaseException as e:
            _msg_error = repr(e)
            if self.dl_cont and self.dl_cont.status == 'error':
                _msg_error += f' - {self.dl_cont.error_message}'

            logger.error(f'{self.uptpremsg()}[fetch] error: {_msg_error}')
            self._speed.append((datetime.now(), 'error'))
            self.status = 'error'
            self.error_message = _msg_error
            if isinstance(e, KeyboardInterrupt):
                raise

    async def fetch_async(self):

        self.progress_timer = ProgressTimer()
        self.status = 'downloading'
        self._speed = []
        self.n_rounds = 0

        self._qspeed = asyncio.Queue()
        check_task = self.add_task(self.check_speed())

        try:
            while True:
                self.n_rounds += 1
                self.dl_cont = None
                self.block_init = True
                self._index_proxy = None
                self._proxy = None
                try:
                    await self.update_uri()
                    logger.debug(f'{self.uptpremsg()} uris:\n{self.uris}')
                    async with async_lock(self.sem):
                        await self.fetch()
                    if self.status in ('done', 'error', 'stop'):
                        return

                except BaseException as e:
                    _msg_error = repr(e)
                    if self.dl_cont and self.dl_cont.status == 'error':
                        _msg_error += f" - {self.dl_cont.error_message}"

                    logger.error(f"{self.uptpremsg()}[fetch_async] error: {_msg_error}")
                    self.status = 'error'
                    self.error_message = _msg_error
                    if isinstance(e, KeyboardInterrupt):
                        raise
                    return

                finally:
                    if all([self._mode != 'noproxy', getattr(self, '_index_proxy', None) is not None]):

                        async with self.vid_dl.master_hosts_alock():
                            self.vid_dl.hosts_dl[self._host]['count'] -= 1
                            self.vid_dl.hosts_dl[self._host]['queue'].put_nowait(self._index_proxy)
                    await asyncio.sleep(0)

        except BaseException as e:
            logger.exception(f'{self.premsg}[fetch_async] {repr(e)}')
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:
            self._qspeed.put_nowait(kill_token)
            await asyncio.wait([check_task])

            def _print_el(el: tuple):

                _secs = el[0].second + el[0].microsecond / 1000000
                _str0 = f'{el[0].strftime("%H:%M:")}{_secs:06.3f}'
                if isinstance(el[1], (str, list, tuple)):
                    _str1 = el[1]
                else:
                    _str1 = f"['status': {el[1].status}, 'speed': {el[1].download_speed}]"

                return f'({_str0}, {_str1})'

            # _str_speed = ', '.join([_print_el(el) for el in self._speed])

            logger.debug(f'{self.premsg}[fetch_async] exiting')  # [{len(self._speed)}]\n{_str_speed}')

    def print_hookup(self):
        msg = ''

        if self.status == 'done':
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}] Completed\n'
        elif self.status == 'init':
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}] Waiting ' +\
                  f'{naturalsize(self.down_size, format_=".2f")} ' +\
                  f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
        elif self.status == 'error':
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}] ERROR ' +\
                  f'{naturalsize(self.down_size, format_=".2f")} ' +\
                  f'[{naturalsize(self.filesize, format_=".2f") if self.filesize else "NA"}]\n'
        elif self.status == 'stop':
            msg = f'[ARIA2C][{self.info_dict["format_id"]}]: HOST[{self._host.split(".")[0]}] STOPPED ' +\
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
                    binary=True)
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
                      f'HOST[{self._host.split(".")[0]}] ' +\
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
