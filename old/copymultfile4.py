# coding: utf-8        
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
import aiofiles
import asyncio
import aiorun
from pathlib import Path
import uvloop
import logging
from common_utils import (
    init_logging, init_tk,
    init_tk_afiles,
    naturalsize
)
import tkinter as tk
from asyncfile import AsyncFile
from asyncio_pool import AioPool
import argparse
import time
from threading import Thread
    
async def run_tk(afiles, root, text0, text1, text2, interval):
    
    logger = logging.getLogger("run_tk")    
    
    logger.info("INIT TK")
    
    try:
        count = 0
        while True:
            root.update()
            res = set([file.status for file in afiles])
            #logger.debug(res)
            if ("init" in res and not "running" in res): 
                pass
            elif (not "init" in res and not "running" in res):                
                break
            else:
                text0.delete(1.0, tk.END)
                text1.delete(1.0, tk.END)
                text2.delete(1.0, tk.END)    
                for file in afiles:
                    mens = file.print_hookup()                    
                    if file.status in ["init"]:
                        text0.insert(tk.END, mens)
                    if file.status in ["running"]:                        
                        text1.insert(tk.END, mens)
                    if file.status in ["done", "error"]:
                        text2.insert(tk.END,mens)
                    
                
           
                    #logger.debug(mens)                 
                
            await asyncio.sleep(interval)
                  
            
    except tk.TclError as e:
        if "application has been destroyed" not in e.args[0]:
            raise
    
    logger.debug("RUN TK BYE")

# def run_tk(afiles, root, text0, text1, text2, interval):
    
#     logger = logging.getLogger("run_tk")
#     logger.info("INIT TK")    
    
  
#     try:
        
#         while True:
#             root.update()
#             res = set([file.status for file in afiles])
#             #logger.debug(res)
#             if ("init" in res and not "running" in res): 
#                 pass
#             elif (not "init" in res and not "running" in res):                
#                 break
#             else:
#                 text0.delete(1.0, tk.END)
#                 text1.delete(1.0, tk.END)
#                 text2.delete(1.0, tk.END)    
#                 for file in afiles:
#                     mens = file.print_hookup()
#                     if file.status in ["init"]:
#                         text0.insert(tk.END, mens)
#                     if file.status in ["running"]:                        
#                         text1.insert(tk.END, mens)
#                     if file.status in ["done", "error"]:
#                         text2.insert(tk.END,mens)
                    
#                     logger.info(mens)                 
                
#             time.sleep(interval)
                  
            
#     except tk.TclError as e:
#         if "application has been destroyed" not in e.args[0]:
#             raise
    
#     logger.info("RUN TK BYE")

async def run_blocking_task(ex, list_files, root_tk, text0_tk, text1_tk, text2_tk, interval):
    loop = asyncio.get_running_loop()
    
    
    blocking_tasks = [loop.run_in_executor(ex, run_tk, list_files, root_tk, text0_tk, text1_tk, text2_tk, interval)]
    
    completed, pending = await asyncio.wait(blocking_tasks)
    
        
    
async def async_main(list_files, workers, root_tk, text0_tk, text1_tk, text2_tk):
    
    logger = logging.getLogger("async_main")
    

    logger.info([afile.file_orig.name for afile in list_files])
    
    try:
        
         
        async with AioPool(size=workers) as pool:
            
            
            fut1 = [pool.spawn_n(run_tk(list_files, root_tk, text0_tk, text1_tk, text2_tk, 0.25))]
            futures = [pool.spawn_n(file.executor()) for file in list_files]
            

                       

            
            
    except Exception as e:
        logger.info(str(e))
        
    asyncio.get_running_loop().stop()
            


def main():
    
    logger = logging.getLogger("main")
    
    time1 = datetime.now()
    
    parser = argparse.ArgumentParser(description="Async move files")
    parser.add_argument("--orig", help="orig folder", default="", type=str)
    parser.add_argument("--dest", help="dest folder", default="", type=str)
    parser.add_argument("-w", help="simult files", default=16, type=int)
    parser.add_argument("-p", help="parts per file", default=16, type=int)
    
    
    args = parser.parse_args()
    
    vid_orig = [file for file in Path(Path.home(), args.orig).iterdir() if file.is_file() and not file.name.startswith(".")]    
    vid_dest = [Path(args.dest, file.name) for file in vid_orig]
    workers = args.w 
    parts = args.p
    
    logger.info(vid_orig)
    logger.info(vid_dest)
  

    list_files = [AsyncFile(vid1, vid2, parts) for vid1, vid2 in zip(vid_orig, vid_dest)]
    
    #logger.info([afile.file_orig.name for afile in list_files])
    
    
    #ex = ThreadPoolExecutor(max_workers=1)
    try:
        root_tk, text0_tk, text1_tk, text2_tk = init_tk_afiles(len(list_files))
        aiorun.run(async_main(list_files, workers,root_tk, text0_tk, text1_tk, text2_tk), use_uvloop=True) 
        
    except Exception as e:
        logger.info(f"aiorun {e}", exc_info=True)   
    
    logger.info(datetime.now() - time1)


if __name__ == "__main__":
    
    init_logging()
    main()
