
import asyncio
from ppadb.client_async import ClientAsync as AdbClient


class AdbAsyncClient():
    
           

    async def worker(self, i, dev, _queue):
        
        while True:
            f = await _queue.get()
            print(f"[{i}] - {f} - init")
            if f == "KILL": break
            await asyncio.sleep(0)
            await dev.pull(f"/sdcard/videos/{f}", f"/Volumes/DatosToni/videos/Varios/{f}")
            self.nvideos -= 1
            print(f"[{i}] - {f} - end - quedan {self.nvideos}")
            await asyncio.sleep(0)

    async def main(self):
        
        client = AdbClient(host="127.0.0.1", port=5037)
        devices = await client.devices()
        dev = devices[0]
        _queue = asyncio.Queue()
        res = await dev.shell(f"ls /sdcard/videos")
        list_res = res.splitlines()
        print(list_res)
        self.nvideos = len(list_res)
        for f in list_res:
            _queue.put_nowait(f)
        for i in range(6):
            _queue.put_nowait("KILL")
        _tasks = [asyncio.create_task(self.worker(i, dev, _queue)) for i in range(6)]
        await asyncio.wait(_tasks)
    
    
asdbcl = AdbAsyncClient()
asyncio.run(asdbcl.main())
