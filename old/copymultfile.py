# coding: utf-8        
from datetime import datetime
import aiofiles
import asyncio
import aiorun
from pathlib import Path
from asyncio_pool import AioPool
import uvloop
import logging
from common_utils import init_logging


async def worker_reader(forig, part_queue, chunk_queue):  
    
    logger = logging.getLogger("worker_reader")
    
    #logger.info(part)
    while not part_queue.empty():
        
        part = await part_queue.get()
        if part == "KILL":
            break
        
        if part == "KILLANDCLEAN":
            for _ in range(32): chunk_queue.put_nowait(("KILL", "KILL"))
            break
    
        #async with rwlock.reader_lock:
        #logger.info(f"{part}:read")
        pos =  forig.seek(part['start'])
        #logger.info(f"{part['part']}:read {pos}")
        chunk = await forig.read(part['size'])
        logger.info(f"{forig.name}: {part['part']}:read {part['start']} size {chunk.__sizeof__()}")
        await chunk_queue.put((part, chunk))
            
        
    logger.info(f"{forig.name}: reader says bye")
              
async def worker_writer(fdest, chunk_queue):        
        
    logger = logging.getLogger("worker_writer")
    
    while True:
        
        if chunk_queue.empty():
            await asyncio.sleep(0.1)
            continue 
        else: break
    
    while not chunk_queue.empty():   
        
        (part, chunk) = await chunk_queue.get()
    
        if part == "KILL":
            break
    
        #async with rwlock.writer_lock:
        #logger.info(f"{part['part']}: write {part['start']} size {chunk.__sizeof__()}")
        pos = fdest.seek(part['start'])
        logger.info(f"{fdest.name}:{part['part']}:write {pos}")
        await fdest.write(chunk)            
        logger.info(f"{fdest.name}:{part['part']}:write {chunk.__sizeof__()}")
        
    logger.info(f"{fdest.name}: writer says bye")
    
async def async_ex():
    
    logger = logging.getLogger("async_ex")
    vid_orig = [file for file in Path(Path.home(), "testing/20210313/temp").iterdir()]
    vid_dest = [Path(f"/Volumes/Pandaext4/videos/ONLYFANS/", file.name) for file in Path(Path.home(), "testing/20210313/temp").iterdir()]
    queue_files = asyncio.Queue()
    
    for vid1, vid2 in zip(vid_orig, vid_dest):
        await queue_files.put((vid1, vid2))
        
    
    
    logger.info(queue_files._queue)
    
    try:
        async with AioPool(size=5) as pool:
            
            futures = [pool.spawn_n(async_main(queue_files)) for _ in range(5)]
                        
            done, pending = await asyncio.wait(futures, return_when=asyncio.ALL_COMPLETED)
            
            logger.info(f"Done : {len(done)} Pemnding: {len(pending)}")    
            
            if pending:
                try:
                    await pool.cancel(pending)
                except Exception as e:
                    pass
                await asyncio.gather(*pending, return_exceptions=True)
            
            if done:
                for d in done:
                    try:                        
                        #d.result()
                        e = d.exception()  
                        if e: logger.info(f"aiopool {e}")                            
                    except Exception as e:
                        logger.info(f"aiopool {e}")
            
            
    except Exception as e:
        logger.info(e)
        
    asyncio.get_running_loop().stop()
            

async def async_main(queue_files):        
        
    logger = logging.getLogger("async_file")

    while not queue_files.empty():
    
        (vid, vid2) = await queue_files.get()
        
        size = vid.stat().st_size
        start_range = 0
        parts = []
        n_parts = 32
        
        queue_part = asyncio.Queue()
        chunk_queue = asyncio.Queue()
        
            
        for i in range(n_parts):
            if i == n_parts-1:
                parts.append({'part': i, 'start' : start_range, 'end': size, 'size': size // n_parts + size % n_parts})                
            else:
                end_range = start_range + (size//n_parts)
                parts.append({'part': i , 'start' : start_range, 'end': end_range,
                            'size': size // n_parts})
                start_range = end_range + 1
                
        logger.info(f"{vid.name} {len(parts)}")        
        
        tsize = 0
        for part in parts:
            await queue_part.put(part)
            tsize += part['size']
            
        logger.info(f"{vid.name} size {size} suma parts {tsize} {size == tsize}")
            
        for _ in range(n_parts):
            await queue_part.put("KILL")        
        await queue_part.put("KILLANDCLEAN")
                
        
        async with aiofiles.open(vid, 'rb') as forig:
            async with aiofiles.open(vid2, 'wb') as fdest:
        
        
                try:
                    async with AioPool(size=2*n_parts) as pool:
                        
                        futures = [pool.spawn_n(worker_reader(forig, queue_part, chunk_queue)) for _ in range(n_parts)]
                        futures2 = [pool.spawn_n(worker_writer(fdest, chunk_queue)) for _ in range(n_parts)]

                        
                        done, pending = await asyncio.wait(futures+futures2, return_when=asyncio.ALL_COMPLETED)
                        
                        logger.info(f"{vid.name} Done : {len(done)} Pemnding: {len(pending)}")    
                        
                        if pending:
                            try:
                                await pool.cancel(pending)
                            except Exception as e:
                                pass
                            await asyncio.gather(*pending, return_exceptions=True)
                        
                        if done:
                            for d in done:
                                try:                        
                                    #d.result()
                                    e = d.exception()  
                                    if e: logger.info(f"{vid.name} aiopool {e}")                            
                                except Exception as e:
                                    logger.info(f"{vid.name} aiopool {e}")
                
                
                except Exception as e:
                    logger.info(f"{vid.name} {e}")
        
      
    
    
 
def main():
    
    logger = logging.getLogger("main")
    
    time1 = datetime.now()
    try:
        aiorun.run(async_ex(), use_uvloop=True) 
        
    except Exception as e:
        logger.info(f"aiorun {e}")   
    
    logger.info(datetime.now() - time1)


if __name__ == "__main__":
    
    init_logging()
    main()
