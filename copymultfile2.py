# coding: utf-8        
from datetime import datetime
import aiofiles
import asyncio
import aiorun
from pathlib import Path
from asyncio_pool import AioPool
import uvloop
import logging
from common_utils import (
    init_logging,
    init_tk_afiles,
    naturalsize
)
from asyncfile import AsyncFile
import tkinter as tk


async def run_tk(root, text0, text1, interval, afiles, pool):
    
    logger = logging.getLogger("run_tk")
    
  
    try:
        
        while True:
            root.update()
            res = [file.status for file in afiles]
            logger.info(res)
            if ("init" in res and not "running" in res): 
                pass
            elif (not "init" in res and not "running" in res):                
                break
            else:
                text0.delete(1.0, tk.END)
                for file in afiles:
         
                    text0.insert(tk.END, f"[{file.file_orig.name}] {file.status} {naturalsize(file.progress)} [{naturalsize(file.size)}]\n")
                
                
                text1.delete(1.0, tk.END)
                text1.insert(tk.END, f"Active corutines: {pool.n_active}")
            
                
            await asyncio.sleep(interval)
                  
            
    except tk.TclError as e:
        if "application has been destroyed" not in e.args[0]:
            raise
    
    logger.info("BYE BYE TK")
    
    root.quit()
    

    
async def async_main(list_files, root, text0, text1):
    
    logger = logging.getLogger("async_main")
    

    logger.info([afile.file_orig.name for afile in list_files])
    
    try:

        async with AioPool() as pool:
            
            
            #fut1 = pool.spawn_n(run_tk(root, text0, text1, 0.25, list_files, pool))
            futures = [pool.spawn_n(file.executor(pool)) for file in list_files]
            #futures.append(fut1)            
            #logger.info(futures)#futures.append(fut1)
                        
            done, pending = await asyncio.wait(futures, return_when=asyncio.ALL_COMPLETED)
            
            logger.info(f"Done : {len(done)} Pending: {len(pending)}")    
            
            if pending:
                try:
                    await pool.cancel(pending)
                except Exception as e:
                    pass
                await asyncio.gather(*pending, return_exceptions=True)
            
            for task in done:
                try:
                    res = task.result()
                    logger.info(res)
                except Exception as e:
                    logger.info(f"{e}")    
            
            
    except Exception as e:
        logger.info(e)
        
    asyncio.get_running_loop().stop()
            

    
    
 
def main():
    
    logger = logging.getLogger("main")
    
    time1 = datetime.now()
    
    vid_orig = [file for file in Path(Path.home(), "testing/20210312/temp").iterdir() if file.is_file()]
    vid_dest = [Path(f"/Volumes/Pandaext4/videos/20210312", file.name) for file in vid_orig]
    
    logger.info(vid_orig)
    logger.info(vid_dest)
    # vid_orig = [file for file in Path(f"/Volumes/Pandaext4/videos/ONLYFANS").iterdir()]
    # vid_dest = [Path(Path.home(), "testing/20210313/temp")]

    list_files = [AsyncFile(vid1, vid2, 16) for vid1, vid2 in zip(vid_orig, vid_dest)]
    
    #logger.info([afile.file_orig.name for afile in list_files])
    
    try:
        (root, text0, text1) = init_tk_afiles(len(list_files))
        #root.update()
        for file in list_files:
            text0.insert(tk.END, f"[{file.file_orig.name}] Progress {naturalsize(file.progress)} [{naturalsize(file.size)}]\n")
        
        aiorun.run(async_main(list_files, root, text0, text1), stop_on_unhandled_errors=True, use_uvloop=True) 
        
    except Exception as e:
        logger.info(f"aiorun {e}", exc_info=True)   
    
    logger.info(datetime.now() - time1)


if __name__ == "__main__":
    
    init_logging()
    main()
