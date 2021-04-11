import aiofiles 
import asyncio
from pathlib import Path
import logging
from common_utils import (
    naturalsize
)

class AsyncFile(object):
    
    _CHUNK_SIZE = 1048576*10
    
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
                
    async def _move_part(self, i):
        
        
        offset_init = int(self.parts[i]['start'])
        nbytes = int(self.parts[i]['size'])
        part_end = offset_init + nbytes
        progress_part = 0
        offset = offset_init
        self.logger.info(f"[move_part][{i}] init {offset_init} size {nbytes}")              
        while True:
            
            if offset >= part_end:
                break
            if (offset + self._CHUNK_SIZE) > part_end:
                _nbytes = part_end - offset
            else: _nbytes = self._CHUNK_SIZE
            self.logger.info(f"[move_part][{i}] read _nbytes {_nbytes}")
            
            async with self.lock:                
                self.aforig.seek(offset)  
                      
                chunk = await self.aforig.read(_nbytes)
            
                  
                _offset = await self.aforig.tell()
                
                await asyncio.sleep(0.1)    
           
                self.afdest.seek(offset)
            
                written = await self.afdest.write(chunk)
                
            self.progress += written
            progress_part += written
            offset = _offset
            self.logger.info(f"[move_part][{i}] new offset {offset}  {part_end}")
            
        
        self.logger.info(f"[move_part][{i}] bye")
        # #self.logger.debug(f"[move_part][{i}] pos orig {self.aforig.tell()}")
        # chunk = await self.aforig.read(nbytes)
        # await asyncio.sleep(0.1)
        # self.afdest.seek(offset)
        # #self.logger.debug(f"[move_part][{i}] pos dest {self.afdest.tell()}")
        # self.progress += await self.afdest.write(chunk)
    
    
       
    
    async def executor(self):
        
       
        self.logger.info(f"[executor] {self.file_orig.name}")
        self.logger.debug(f"[executor] {self.file_orig.name} {self.parts}")
        
        self.lock = asyncio.Lock()       

        self.status = 'running'
        await asyncio.sleep(0.1)
        
        
        async with aiofiles.open(self.file_orig, 'rb') as self.aforig, aiofiles.open(self.file_dest, 'wb') as self.afdest:
        
            
            self.move_tasks = [asyncio.create_task(self._move_part(i)) for i in range(self.n_parts)]
            
            self.logger.info(f"[executor] {self.file_orig.name} tasks readers&writers in the loop")
            
            done, pending = await asyncio.wait(self.move_tasks, return_when=asyncio.ALL_COMPLETED)
            
        if (dest_size := self.file_dest.stat().st_size) == self.size: 
            self.status = 'done'
            #self.file_orig.unlink()
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
            return (f"[{self.file_orig.name}]: Progress {naturalsize(self.progress)} of [{(self.size)}]\n")
        
               
       
        

                                    