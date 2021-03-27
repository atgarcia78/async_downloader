from datetime import datetime
import aiofiles
import asyncio
import aiorun
from pathlib import Path
from asyncio_pool import AioPool
import uvloop
import logging
from common_utils import (
    naturalsize
)

class AsyncFile(object):
    
    _CHUNK_SIZE = 1024*5
    
    def __init__(self, file_orig, file_dest, n_parts):
        
        self.logger = logging.getLogger("async_file")
        
        if isinstance(file_orig, Path):
            self.file_orig = file_orig
        else:
            self.file_orig = Path(file_orig)
        if isinstance(file_dest, Path):
            self.file_dest = file_dest
        else:
            self.file_dest = Path(file_dest)
            
        self.n_parts = n_parts
        
        self.size = self.file_orig.stat().st_size
        
        self.parts = []        
        
        self._get_parts()
        
        self.progress = 0
        
        #self.logger.info(f"{self.file_orig.name}:{self.size}:{self.parts}")
        
        self.status = 'init'
        
        
    def _get_parts(self):
        
        start_range = 0
        
        for i in range(self.n_parts):
            if i == self.n_parts-1:
                self.parts.append({'part': i, 'start' : start_range, 'end': self.size, 'size': self.size // self.n_parts + self.size % self.n_parts})                
            else:
                end_range = start_range + (self.size//self.n_parts)
                self.parts.append({'part': i , 'start' : start_range, 'end': end_range,
                            'size': self.size //self.n_parts})
                start_range = end_range + 1
                
    def _feed_queue(self):
        
        for part in self.parts:
            self.queue_parts.put_nowait(part)
            
        for _ in range(self.n_parts):
            self.queue_parts.put_nowait("KILL")
            
            
        self.logger.info(f"{self.file_orig.name}:queue {self.queue_parts._queue}")
                             
    
    async def _worker_reader(self):        
        
        while not self.queue_parts.empty():
            
            part = await self.queue_parts.get()
            
            self.logger.info(f"{self.file_orig.name}:read part {part}")
            
            if part == "KILL":
                break
            
            if part == "KILLANDCLEAN":
                for _ in range(self.n_parts): self.queue_chunks.put_nowait(("KILL", "KILL"))
                break
        
            
            self.forig_obj.seek(part['start'])
            chunk = await self.forig_obj.read(part['size'])
            self.queue_chunks.put_nowait((part, chunk))
            self.logger.info(f"{self.forig_obj.name}: {part['part']}:read {part['start']} size {chunk.__sizeof__()}")
            asyncio.sleep(0)
            
            
        self.logger.info(f"{self.file_orig.name}: reader says bye")
            
    
    # async def _worker_writer(self):
        
    #     #self.logger.info(f"{self.file_orig.name}: writer init")
    #     while True:
                    
    #         if self.queue_chunks.empty():
    #              await asyncio.sleep(0.1)
    #              continue 
    #         else: break
    
    #     while not self.queue_chunks.empty():   
        
    #         #self.logger.info(f"{self.file_orig.name}: chunks queue size {self.queue_chunks.qsize()}")
    #         (part, chunk) = await self.queue_chunks.get()
    
    #         #self.logger.info(f"{self.file_orig.name}:write part {part}")
            
    #         if part == "KILL":
    #             break
        

    #         #self.logger.info(f"{part['part']}: write {part['start']} size {chunk.__sizeof__()}")
    #         self.fdest_obj.seek(part['start'])
    #         await self.fdest_obj.write(chunk)   
    #         self.progress += chunk.__sizeof__()         
    #         self.logger.info(f"{self.file_orig.name}:{part['part']}:write progress [{naturalsize(self.progress)}]")
            
        
    #     self.logger.info(f"{self.file_orig.name}: writer says bye")
    
    
    
    async def _worker(self):
        
        async with aiofiles.open(str(self.file_orig), 'rb') as forig_obj, aiofiles.open(str(self.file_dest), 'wb') as fdest_obj:
            
            while True:
            
                part = await self.queue_parts.get()
                self.logger.info(f"{self.file_orig.name}:read part {part}")
                
                if part == "KILL":
                    break 
            
                # async with self.forig_obj:                
                #     async with self.fdest_obj:                
                forig_obj.seek(part['start'])
                fdest_obj.seek(part['start'])
                # n_iters = part['size'] // self._CHUNK_SIZE
                # partsize = 0
                # for n in range(n_iters+1):
                #     if n == n_iters-1:
                #         _chunk_size = self._CHUNK_SIZE + part['size'] % self._CHUNK_SIZE
                #     else:                                        
                #         _chunk_size = self._CHUNK_SIZE
                                        
                #     chunk_data = await forig_obj.read(_chunk_size)
    
                #     chsize = await fdest_obj.write(chunk_data)
                    
                #     partsize += chsize
                #     self.progress += chsize
                chunk_data = await forig_obj.read(part['size'])
                chsize = await fdest_obj.write(chunk_data)
                self.progress += chsize
                
                self.logger.info(f"{self.file_orig.name}:done part {part['part']} size est {part['size']} size real {chsize}")
            
            self.logger.info(f"{self.file_orig.name}: _worker bye bye")
    
    
    async def executor(self, pool):
        
       
        self.logger.info(f"[executor] {self.file_orig.name} {self.parts}")
        
        self.queue_parts = asyncio.Queue()
        self._feed_queue()
        #self.queue_chunks = asyncio.Queue()
        
        
        # async with aiofiles.open(str(self.file_orig), 'rb') as self.forig_obj:
        #     async with aiofiles.open(str(self.file_dest), 'wb') as self.fdest_obj:
                
        
        
        
        
        self.status = 'running'
        try:
            #async with AioPool() as pool:
            async with pool:
                    
                futures = [pool.spawn_n(self._worker()) for _ in range(self.n_parts)]

                
                done, pending = await asyncio.wait(futures,return_when=asyncio.ALL_COMPLETED)
                
                self.logger.info(f"{self.file_orig.name} Done : {len(done)} Pending: {len(pending)}")
                
                    
                
                if pending:
                    try:
                        await pool.cancel(pending)
                    except Exception as e:
                        pass
                    await asyncio.gather(*pending, return_exceptions=True)
                
                results = []
                for task in done:                            
                    try:
                        res = task.result()
                        self.logger.info(res)
                        if not res: results.append("OK")                                
                    except Exception as e:
                        self.logger.info(f"{e}")
                            
        
                self.logger.info(f"{self.file_orig.name}: results: {results}")
                if len(results) == len(done):                        
                    self.status = 'done'
                else: self.status = 'error'
                
                self.logger.info(f"{self.file_orig.name}: status {self.status}")
                
                    
        except Exception as e:
            self.logger.info(f"{self.file_orig.name} {e}")
            self.status = 'error'
        

                                    