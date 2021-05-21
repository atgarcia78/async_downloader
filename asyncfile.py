import aiofiles 
from concurrent.futures import (
    ThreadPoolExecutor,
    wait,
    ALL_COMPLETED)
import asyncio
import threading
from pathlib import Path
import logging
from common_utils import (
    naturalsize
)
import time
from queue import Queue

from send2trash import send2trash

class AsyncFile(object):
    
    _CHUNK_SIZE = 1048576 * 300
    
    def __init__(self, file_orig, file_dest, max_workers):
        
        self.logger = logging.getLogger("async_file")        
        self.status = 'init'
        if isinstance(file_orig, Path):
            self.file_orig = file_orig
        else:
            self.file_orig = Path(file_orig)
        if isinstance(file_dest, Path):
            self.file_dest = file_dest
        else:
            self.file_dest = Path(file_dest)            
        self.max_workers = max_workers        
        self.size = self.file_orig.stat().st_size        
        self.parts = []   
        
        self._get_parts()
        
        self.progress = 0
        

        
    def _get_parts(self):
        
        self.n_parts = self.size // self._CHUNK_SIZE
        
        start_range = 0
        
        if self.n_parts == 0:
            self.parts.append({'part': 1, 'start' : start_range, 'end': self.size, 'size': self.size}) 
            self.n_parts = 1
            
        else:
            
            for i in range(self.n_parts):
                if i == self.n_parts-1:
                    self.parts.append({'part': i, 'start' : start_range, 'end': self.size, 'size': self._CHUNK_SIZE + self.size % self._CHUNK_SIZE})                
                else:
                    end_range = start_range + self._CHUNK_SIZE
                    self.parts.append({'part': i , 'start' : start_range, 'end': end_range,
                                'size': self._CHUNK_SIZE})
                    start_range = end_range + 1
                
    def _read_worker(self, i):
        
        while True:
            part = self.parts_queue.get()
            if part == "KILL":
                self.count += 1
                self.logger.info(f"[{self.file_orig.name}]:part [{part}]:count [{self.count}]")                
                break
            elif part =="KILLANDCLEAN":
                self.logger.info(f"[{self.file_orig.name}]:part [{part}]:count [{self.count}]")
                while not (self.count == self._max_n_readers - 1):
                    time.sleep(1)
                self.chunk_queue.put((-1, "KILL"))
                self.logger.info(f"[{self.file_orig.name}]: bye READER [{i}]")
                break
            else:
                offset = part['start']
                nbytes = part['size']
                self.logger.info(f"[{self.file_orig.name}]:[move_part][{part['part']}] offset {offset} init size {nbytes}") 
                with open(self.file_orig, "rb") as aforig:
                    aforig.seek(offset)
                    chunk = aforig.read(nbytes) 
                self.chunk_queue.put((offset, chunk)) 
        
                
        
    
    
    def _write_worker(self, afdest):
    
        while self.chunk_queue.qsize() == 0:
            time.sleep(0.1)
            
        while True:
            offset, chunk = self.chunk_queue.get()
            self.logger.info(f"[{self.file_orig.name}]:WRITER:offset[{offset}]")
            if offset == -1:
                self.logger.info(f"[{self.file_orig.name}]: bye WRITER")
                break
            else:
                afdest.seek(offset)
                written = afdest.write(chunk)
                afdest.flush()
                self.progress += written
                
       
    
    def executor(self):
        
       
        self.status = 'running'
        self.logger.info(f"[executor]{self.file_orig} \n {self.parts} ")
        
        self.chunk_queue = Queue()
        self.parts_queue = Queue()
        
        for part in self.parts:
            self.parts_queue.put(part)
        
        self._max_n_readers = min(self.max_workers-1, self.n_parts) 
        self.logger.info(f"._max_n_readers:{self._max_n_readers}")   
        
        for i in range(self._max_n_readers-1):
            self.parts_queue.put("KILL")
            
        self.parts_queue.put("KILLANDCLEAN")
        
        self.count = 0
        
        try:
        
            with open(self.file_dest, "wb") as afdest:
                
            #async with aiofiles.open(self.file_dest, "wb") as self.afdest:
                with ThreadPoolExecutor(thread_name_prefix=f"[{self.file_orig.stem}]", max_workers=self._max_n_readers+1) as ex:
                    
                    move_fut = [ex.submit(self._read_worker, i) for i in range(self._max_n_readers)]
                    writer_fut = [ex.submit(self._write_worker(afdest))]
                    

                    
            if (dest_size := self.file_dest.stat().st_size) == self.size: 
                self.status = 'done'
                try:
                    send2trash(str(self.file_orig))
                except Exception as e:
                    self.logger.error(f"[{self.file_orig.name}] error to trash: {str(e)}", exc_info=True)
            else: 
                self.status = 'error'
                self.file_dest.unlink()
                
            self.logger.info(f"[{self.file_orig.name}] origfile size {self.size} destfile size {dest_size} {self.status}")
        except Exception as e:
            self.logger.error(f"[{self.file_orig.name}] error: {str(e)}", exc_info=True)
            
            
    def print_hookup(self):        
            
        if self.status == "done":
            return (f"[{self.file_orig.name}]: DONE size dest[{naturalsize(self.file_dest.stat().st_size)}] orig[{naturalsize(self.size)}]")
        elif self.status == "init":
            return (f"[{self.file_orig.name}]: Waiting to enter in the pool size orig[{naturalsize(self.size)}]")            
        elif self.status == "error":
            return (f"[{self.file_orig.name}]: ERROR progress [{naturalsize(self.progress)}] of [{naturalsize(self.size)}]")
        else:            
            return (f"[{self.file_orig.name}]: Progress {naturalsize(self.progress)} of [{naturalsize(self.size)}]")
        
               
       
        

                                    