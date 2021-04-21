import aiofiles 
from concurrent.futures import (
    ThreadPoolExecutor,
    wait)
import asyncio
import threading
from pathlib import Path
import logging
from common_utils import (
    naturalsize
)

class AsyncFile(object):
    
    _CHUNK_SIZE = 1048576
    
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
        
        self.lock = threading.Lock()
        
        self.afdest = open(self.file_dest, "wb") 
        
        
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
                
    def _move_part(self, i):
        
        
        offset = int(self.parts[i]['start'])
        nbytes = int(self.parts[i]['size'])
        self.logger.info(f"[move_part][{i}] init size {nbytes}")              
        
        
        with open(self.file_orig, 'rb') as aforig:
            aforig.seek(offset)
            #self.logger.debug(f"[move_part][{i}] pos orig {self.aforig.tell()}")
            chunk = aforig.read(nbytes)
        
        
        
        with self.lock:
             
        
            self.afdest.seek(offset)
            #await afdest.truncate()
            #pos = await afdest.seek(offset)
            #self.logger.debug(f"[move_part][{i}] pos dest {self.afdest.tell()}")
            written = self.afdest.write(chunk)
            self.progress += written
            #apos = await afdest.tell()
        
        #self.logger.info(f"[{self.file_orig.stem}][move_part][{i}] init size {nbytes} pos {pos} apos {apos} written {written}")
       #await asyncio.sleep(0.1)
    
       
    
    def executor(self):
        
       
        self.logger.info(f"[executor] {self.file_orig.name}")
        self.logger.info(f"[executor] {self.file_orig.name} {self.parts}")
        
       
        #self.lock = asyncio.Lock()

        self.status = 'running'
        #await asyncio.sleep(0.1)
        
        
        
        #async with aiofiles.open(self.file_dest, "wb") as self.afdest:
        with ThreadPoolExecutor(thread_name_prefix=f"[{self.file_orig.stem}]", max_workers=self.n_parts) as ex:
            self.move_fut = [ex.submit(self._move_part, i) for i in range(self.n_parts)]    
        #self.move_tasks = [asyncio.to_thread(self._move_part, i) for i in range(self.n_parts)]
            wait(self.move_fut)
            #self.move_tasks = [asyncio.create_task(self._move_part(i)) for i in range(self.n_parts)]
            
        #self.logger.info(f"[executor] {self.file_orig.name} tasks readers&writers in  loop")
            
        #await asyncio.gather(*self.move_tasks, asyncio.sleep(1))
        
        self.afdest.close()
                  
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
        
               
       
        

                                    