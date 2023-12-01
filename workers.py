import asyncio
import logging
from collections import deque

from utils import MySyncAsyncEvent, print_tasks, try_call


class Workers:
    def __init__(self, asyncdl, name, max_workers):
        self.asyncdl = asyncdl
        self.logger = logging.getLogger(name)
        self._max = max_workers
        self.running = deque()
        self.waiting = deque()
        self.tasks = {}
        self.info_dl = {}
        self.alock = asyncio.Lock()
        self.exit = MySyncAsyncEvent(f"{name}exit")

    @property
    def max_workers(self):
        return self._max

    @max_workers.setter
    def max_workers(self, value):
        self._max = value

    async def add_task(self, **kwargs):

        task_index = kwargs.get('task_index')
        sortwaiting = kwargs.get('sortwaiting')
        _pre = lambda x: f"[add_task][{x}]"

        if len(self.running) < self.max_workers:
            if not (task_index := (task_index or try_call(
                    lambda: self.waiting.popleft()))):
                self.logger.debug(f"{_pre(task_index)} empty waiting list")
            else:
                self.running.append(task_index)
                self.tasks |= {self.asyncdl.add_task(
                    self._task(task_index)): task_index}
                self.logger.debug(f"{_pre(task_index)} task ok {print_tasks(self.tasks)}")
        elif task_index:
            self.waiting.append(task_index)
            if sortwaiting:
                self.waiting = deque(sorted(self.waiting))
            self.logger.debug(f"{_pre()} task to waiting list")
        else:
            self.logger.debug(
                f"{_pre()} running full, no task added from waiting")

    async def remove_task(self, task_index):
        async with self.alock:
            self.running.remove(task_index)
            await self.add_task()

    async def has_tasks(self):
        async with self.alock:
            return bool(self.waiting or self.running)


class WorkersRun(Workers):
    def __init__(self, asyncdl):
        super().__init__(asyncdl, "WorkersRun", asyncdl.workers)

    async def add_worker(self):
        async with self.alock:
            self.max_workers += 1
            await self.add_task()

    async def del_worker(self):
        async with self.alock:
            if self.max_workers > 0:
                self.max_workers -= 1

    async def check_to_stop(self):
        self.logger.debug(
            f"[check_to_stop] running[{len(self.running)}] " +
            f"waiting[{len(self.waiting)}]")
        if not await self.has_tasks():
            self.logger.debug("[check_to_stop] set exit")
            self.exit.set()
            self.asyncdl.end_dl.set()

    async def move_to_waiting_top(self, dl_index):
        async with self.alock:
            dl = self.info_dl[dl_index]["dl"]
            dl_status = dl.info_dl["status"]
            if dl_index in self.waiting:
                if self.waiting.index(dl_index) > 0:
                    self.waiting.remove(dl_index)
                    self.waiting.appendleft(dl_index)
                    self.logger.debug(
                        f"[move_to_waiting_top] {list(self.waiting)}")
            elif dl_index not in self.running and dl_status in ("stop", "error"):
                await dl.reinit()
                await self.add_task(task_index=dl_index)

    async def add_dl(self, dl, url_key):
        _pre = f"[{dl.info_dict['id']}][{dl.info_dict['title']}][{url_key}]:[add_dl]"
        if dl.index in self.info_dl:
            self.logger.warning(
                f"{_pre} dl with index[{dl.index}] already processed")
            return
        self.info_dl |= {dl.index: {"url": url_key, "dl": dl}}
        self.logger.debug(
            f"{_pre} running[{len(self.running)}] waiting[{len(self.waiting)}]")
        async with self.alock:
            await self.add_task(task_index=dl.index, sortwaiting=True)

    async def _task(self, dl_index):
        url_key, dl = self.info_dl[dl_index]["url"], self.info_dl[dl_index]["dl"]
        _pre = f"[{dl.info_dict['id']}][{dl.info_dict['title']}][{url_key}]:[_task]"

        try:
            if dl.info_dl["status"] not in ("init_manipulating", "done"):
                self.logger.debug(f"{_pre} DL init OK, ready to DL")
                if dl.info_dl.get("auto_pasres"):
                    self.asyncdl.list_pasres.add(dl.index)
                    self.logger.debug(
                        f"{_pre} added dl[{dl.index}] " +
                        f"to auto_pasres{list(self.asyncdl.list_pasres)}")
                await dl.run_dl()
            else:
                self.logger.debug(f"{_pre} DL init OK, video parts DL OK")

            await self.asyncdl.run_callback(dl, url_key)

        except Exception as e:
            self.logger.exception(f"{_pre} error {str(e)}")
        finally:
            self.logger.debug(f"{_pre} end task worker run")
            await self.remove_task(dl_index)
            if self.asyncdl.WorkersInit.exit.is_set():
                self.logger.debug(f"{_pre} WorkersInit.exit is set")
                if not await self.has_tasks():
                    self.logger.debug(f"{_pre} pending running or waiting")
                    self.exit.set()
                    self.asyncdl.end_dl.set()
                    self.logger.debug(f"{_pre} end_dl set")
                else:
                    self.logger.debug(f"{_pre} no waiting no running")
            else:
                self.logger.debug(f"{_pre} WorkersInit.exit not set")
                if await self.has_tasks():
                    self.logger.debug(f"{_pre} pending running or waiting")
                    return
                self.logger.debug(
                    f"{_pre} no running no waiting, " +
                    "so lets wait for WorkersInit.exit")
                await self.asyncdl.WorkersInit.exit.async_wait()
                if not await self.has_tasks():
                    self.logger.debug(
                        f"{_pre} WorkersInit.exit is set after waiting, " +
                        "no running no waiting, lets set exit")
                    self.exit.set()
                    self.asyncdl.end_dl.set()
                    self.logger.info(f"{_pre} end_dl set")
                else:
                    self.logger.debug(
                        f"{_pre} WorkersInit.exit is set after waiting, " +
                        "pending running or waiting, so lets exit")


class WorkersInit(Workers):
    def __init__(self, asyncdl):
        super().__init__(asyncdl, "WorkersInit", asyncdl.init_nworkers)

    async def add_init(self, url_key):
        _pre = f"[add_init]:[{url_key}]"
        self.logger.debug(
            f"{_pre} running[{len(self.running)}] waiting[{len(self.waiting)}]")
        if url_key in self.waiting or url_key in list(self.tasks.values()):
            self.logger.warning(f"{_pre} already processed")
            return
        async with self.alock:
            await self.add_task(task_index=url_key)

    async def _task(self, url_key):
        _pre = f"[_task]:[{url_key}]"

        try:
            if url_key == "KILL":
                await self.remove_task(url_key)
                while await self.has_tasks():
                    await asyncio.sleep(0)
                self.logger.debug(f"{_pre} end tasks worker init: exit")
                self.asyncdl.t3.stop()
                self.exit.set()
                await self.asyncdl.WorkersRun.check_to_stop()
            else:
                try:
                    await self.asyncdl.init_callback(url_key)
                finally:
                    await self.remove_task(url_key)

        except Exception as e:
            self.logger.exception(f"{_pre} error {str(e)}")
