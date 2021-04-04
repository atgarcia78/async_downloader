import aiofiles
import asyncio
from pathlib import Path
import logging
from common_utils import (
    naturalsize
)

class AsyncFile(object):
    
    _CHUNK_SIZE = 1048576*100
    
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
            
        for _ in range(self.n_parts-1):
            self.queue_parts.put_nowait("KILL")
        
        self.queue_parts.put_nowait("KILLANDFILL")    
            
        self.logger.debug(f"{self.file_orig.name}:queue {self.queue_parts._queue}")
                             
    
    async def _worker_reader(self, i):        
        
        self.logger.info(f"[{self.file_orig.name}][reader{i}] init")
        
        
        #while self.queue_parts.empty():
        
        
        
        while True:
        
            await asyncio.sleep(1)
            part = await self.queue_parts.get()
    
                
            #self.logger.info(f"[{self.file_orig.name}][reader{i}] part {part}")
            
            if part == "KILL":
                break
            
            if part == "KILLANDFILL":
                for _ in range(self.n_parts):
                    self.queue_chunks.put_nowait(("KILL", None))
                    await asyncio.sleep(1)
                break
                    
            
            self.forig_obj.seek(part['start'])
            chunk = await self.forig_obj.read(part['size'])
            self.queue_chunks.put_nowait((part, chunk))
            #await asyncio.sleep(1)
            #self.logger.info(f"[{self.file_orig.name}][reader{i}] {part['part']}:requests to read from {part['start']} {part['size']},  {chunk.__sizeof__()} read")
                    
        
    #    self.logger.info(f"[{self.file_orig.name}][reader{i}]reader says bye")
            
    
    async def _worker_writer(self, i):
        
        self.logger.info(f"[{self.file_orig.name}][writer{i}] init")
        
        
        #while self.queue_chunks.empty():
        #    await asyncio.sleep(1)
        
        while True: 
            
            #self.logger.info(f"{self.file_orig.name}: chunks queue size {self.queue_chunks.qsize()}")
            await asyncio.sleep(1)
            (part, chunk) = await self.queue_chunks.get()
 
            #self.logger.info(f"{self.file_orig.name}:write part {part}")
            
            if part == "KILL":
                break
        
            
            self.fdest_obj.seek(part['start'])
            
            
            # n_iters = part['size'] // self._CHUNK_SIZE
            # ant_index = 0
            # self.logger.info(f"{self.file_orig.name}: part {part['part']} size {part['size']} n_iters {n_iters}")
            # for n in range (n_iters+1):
            #     #if n == n_iters - 1: _chunk_data_size = part['size'] // self._CHUNK_SIZE + part['size'] % self._CHUNK_SIZE
            #     #else: _chunk_data_size = self._CHUNK_SIZE
            #     start_index = ant_index
            #     end_index = start_index + self._CHUNK_SIZE
            #     chsize = await self.fdest_obj.write(chunk[start_index:end_index])
            #     self.progress += chsize
            #     ant_index = end_index + 1
            #     self.logger.info(f"{self.file_orig.name}: part {part['part']} n {n} sindx {start_index} eindx {end_index} chsize {chsize}") 
            # chsize = await self.fdest_obj.write(chunk[ant_index:ant_index+part['size']%self._CHUNK_SIZE])
            # self.progress += chsize             
            
            
            chsize = await self.fdest_obj.write(chunk)
            self.progress += chsize
            
            
            #self.logger.info(f"[{self.file_orig.name}]{part['part']}: {part['size']} {chsize} {chunk.__sizeof__()}")
            
       # self.logger.info(f"[{self.file_orig.name}][writer{i}] writer says bye")
    
    
       
    
    async def executor(self):
        
       
        self.logger.info(f"[executor] {self.file_orig.name}")
        self.logger.debug(f"[executor] {self.file_orig.name} {self.parts}")
        
        self.queue_parts = asyncio.Queue()
        self._feed_queue()
        self.queue_chunks = asyncio.Queue()

        self.status = 'running'
        await asyncio.sleep(1)
        
        async with aiofiles.open(self.file_orig, 'rb') as self.forig_obj, aiofiles.open(self.file_dest, 'wb') as self.fdest_obj:
        
            readers_tasks = [asyncio.create_task(self._worker_reader(i)) for i in range(self.n_parts)]
            writers_tasks = [asyncio.create_task(self._worker_writer(i)) for i in range(self.n_parts)]
            self.logger.info(f"[executor] {self.file_orig.name} tasks readers&writers in the loop")
            
            done, pending = await asyncio.wait(readers_tasks + writers_tasks, return_when=asyncio.ALL_COMPLETED)
            
        if (dest_size := self.file_dest.stat().st_size) == self.size: 
            self.status = 'done'
            self.file_orig.unlink()
        else: 
            self.status = 'error'
            self.file_dest.unlink()
            
        self.logger.info(f"[{self.file_orig.name}] origfile size {self.size} destfile size {dest_size} {self.status}")
        return
            
    def print_hookup(self):        
            
        if self.status == "done":
            return (f"[{self.file_orig.name}]: DONE size dest[{naturalsize(self.file_dest.stat().st_size)}] orig[{naturalsize(self.size)}]\n")
        elif self.status == "init":
            return (f"[{self.file_orig.name}]: Waiting to enter in the pool size orig[{naturalsize(self.size)}\n")            
        elif self.status == "error":
            return (f"[{self.file_orig.name}]: ERROR progress [{naturalsize(self.progress)}] of [{naturalsize(self.size)}]\n")
        else:            
            return (f"[{self.file_orig.name}]: Progress {naturalsize(self.progress)} of [{naturalsize(self.size)}]\n")
        
               
       
        

                                    