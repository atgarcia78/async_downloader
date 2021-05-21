
import asyncio
from ppadb.client_async import ClientAsync as AdbClient
import shlex
from pathlib import Path
import logging
from asynclogger import AsyncLogger


class AsyncADBFile():
    
    def __init__(self, forig, fdest, nworkers):
        
        self.forig = f"/sdcard/videos/{forig}"
        self.fdest = fdest if isinstance(fdest, Path) else Path(fdest)
        self.nworkers = nworkers
        self.logger = logging.getLogger("asyncADBFile")
        self.alogger = AsyncLogger(self.logger)
        

    async def worker(self, i, dev, _queue):
        
        await self.alogger.debug(f"[{i}] - worker start")
        
        while True:
            f = await _queue.get()
            await self.alogger.debug(f"[{i}]{f[0]} - init")
            if f[0] == "KILL": break
            await asyncio.sleep(0)
            _fpath = Path(self.fdest, f[0])
            await dev.pull(f"{self.forig}/{f[0]}", f"{str(_fpath)}")
            
            await self.alogger.debug(f"[{i}][{f[0]}] - dl - exists[{_fpath.exists()}]")
            if (_fpath.exists() ):
                    _fsize = _fpath.stat().st_size                    
                    if _fsize == int(f[1]):                        
                        res = await dev.shell(f"rm '{self.forig}/{f[0]}'")
                        await self.alogger.debug(f"[{i}][{f[0]}] - rm - {res}")
                        
                    
            self.nvideos -= 1
            await self.alogger.debug(f"[{i}][{f[0]}] - end - quedan {self.nvideos}")
            await asyncio.sleep(0)

    async def run(self):
        
        client = AdbClient(host="127.0.0.1", port=5037)
        devices = await client.devices()
        dev = devices[0]
        _queue = asyncio.Queue()
        res = await dev.shell(f"ls -l {self.forig}")
        await self.alogger.debug(res)
        list_res = res.splitlines()        
        self.nvideos = len(list_res)
        for f in list_res[1:]:
            _data = shlex.split(f)
            await self.alogger.info(f"{_data[7]}:{_data[4]}")
            _queue.put_nowait((_data[7], _data[4]))
        for i in range(self.nworkers):
            _queue.put_nowait(("KILL", "KILL"))
        _tasks = [asyncio.create_task(self.worker(i, dev, _queue)) for i in range(self.nworkers)]
        await asyncio.wait(_tasks)
        
        asyncio.get_running_loop().stop()
    
    

