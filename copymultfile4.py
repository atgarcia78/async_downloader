# coding: utf-8        
from datetime import datetime
import aiofiles
import asyncio
import aiorun
from pathlib import Path
import uvloop
import logging
from common_utils import (
    init_logging,
    init_tk_afiles,
    naturalsize
)
import tkinter as tk
from asyncfile_v3 import AsyncFile
from asyncio_pool import AioPool
import argparse
    
async def run_tk(afiles, root, text, interval):
    
    logger = logging.getLogger("run_tk")
    
    
  
    try:
        
        while True:
            root.update()
            res = [file.status for file in afiles]
            #logger.info(res)
            if ("init" in res and not "running" in res): 
                pass
            elif (not "init" in res and not "running" in res):                
                break
            else:
                text.delete(1.0, tk.END)
                for file in afiles:
                                       
                    text.insert(tk.END, f"[{file.file_orig.name}] {file.status} {naturalsize(file.progress)} [{naturalsize(file.size)}]\n")                
                
                            
            await asyncio.sleep(interval)
                  
            
    except tk.TclError as e:
        if "application has been destroyed" not in e.args[0]:
            raise
    
    logger.info("BYE BYE TK")
    
    
async def async_main(list_files, root, text):
    
    logger = logging.getLogger("async_main")
    

    logger.info([afile.file_orig.name for afile in list_files])
    
    try:
        
        async with AioPool(size=24) as pool:
            
            fut2 = [pool.spawn_n(run_tk(list_files, root, text, 0.25))]
            futures = [pool.spawn_n(file.executor()) for file in list_files]
            

        #tasks = [asyncio.create_task(file.executor()) for file in list_files]
        #task_tk = [asyncio.create_task(run_tk(list_files, root, text, 0.25))]
            
        #await asyncio.wait(tasks + task_tk)                        

            
            
    except Exception as e:
        logger.info(e)
        
    asyncio.get_running_loop().stop()
            


def main():
    
    logger = logging.getLogger("main")
    
    time1 = datetime.now()
    
    parser = argparse.ArgumentParser(description="Async move files")
    parser.add_argument("--orig", help="orig folder", default="", type=str)
    parser.add_argument("--dest", help="dest folder", default="", type=str)
    
    args = parser.parse_args()
    
    vid_orig = [file for file in Path(Path.home(), args.orig).iterdir() if file.is_file() and not file.name.startswith(".")]
    vid_dest = [Path("/Volumes/Pandaext4/videos", args.dest, file.name) for file in vid_orig]
    
    logger.info(vid_orig)
    logger.info(vid_dest)
    # vid_orig = [file for file in Path(f"/Volumes/Pandaext4/videos/ONLYFANS").iterdir()]
    # vid_dest = [Path(Path.home(), "testing/20210313/temp")]

    list_files = [AsyncFile(vid1, vid2, 64) for vid1, vid2 in zip(vid_orig, vid_dest)]
    
    #logger.info([afile.file_orig.name for afile in list_files])
    
    try:
        root, text = init_tk_afiles(len(list_files))  
        aiorun.run(async_main(list_files, root, text), stop_on_unhandled_errors=True, use_uvloop=True) 
        
    except Exception as e:
        logger.info(f"aiorun {e}", exc_info=True)   
    
    logger.info(datetime.now() - time1)


if __name__ == "__main__":
    
    init_logging()
    main()
