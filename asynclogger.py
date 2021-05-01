import functools
import asyncio
from concurrent.futures import ThreadPoolExecutor

class AsyncLogger():
    
    def __init__(self, logger):
        
        self.logger = logger        
        self.ex = ThreadPoolExecutor(max_workers=1)
        
    async def debug(self, *args, **kwargs):        
        
        loop = asyncio.get_running_loop()
        
        
        blocking_task = [loop.run_in_executor(self.ex, functools.partial(self.logger.debug, *args, **kwargs))]
        await asyncio.wait(blocking_task)
        
    async def info(self, *args, **kwargs):

        loop = asyncio.get_running_loop()
        
        blocking_task = [loop.run_in_executor(self.ex, functools.partial(self.logger.info, *args, **kwargs))]
        await asyncio.wait(blocking_task)
        
    async def warning(self, *args, **kwargs):

        loop = asyncio.get_running_loop()
        
        blocking_task = [loop.run_in_executor(self.ex, functools.partial(self.logger.warning, *args, **kwargs))]
        await asyncio.wait(blocking_task)
        
    async def error(self, *args, **kwargs):

        loop = asyncio.get_running_loop()
        
        blocking_task = [loop.run_in_executor(self.ex, functools.partial(self.logger.error, *args, **kwargs))]
        await asyncio.wait(blocking_task)
        
        
        
        
    