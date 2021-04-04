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

_CHUNK_SIZE = 1048576*100
    
async def run_tk(afiles, args_tk, interval):
    
    logger = logging.getLogger("run_tk")    
    
    logger.info("INIT TK")
    
    root, text0, text1, text2 = args_tk
    
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

# async def run_blocking_task(ex, list_files, root_tk, text0_tk, text1_tk, text2_tk, interval):
#     loop = asyncio.get_running_loop()
    
    
#     blocking_tasks = [loop.run_in_executor(ex, run_tk, list_files, root_tk, text0_tk, text1_tk, text2_tk, interval)]
    
#     completed, pending = await asyncio.wait(blocking_tasks)
    
async def worker_run(queue_files, i):
        
        logger = logging.getLogger("worker_run")
        
        logger.debug(f"worker_run[{i}]: launched")       
        await asyncio.sleep(1)
        
        
        while True:
            
            try:
            
                await asyncio.sleep(1)
                file = await queue_files.get()
                logger.debug(f"worker_run[{i}]: get for a file")
                
                if file == "KILL":
                    logger.debug(f"worker_run[{i}]: get KILL, bye")
                    break
                
                
                
                else:
                    logger.debug(f"worker_run[{i}]: start to mv {file.file_orig.name}")
                    task_run = asyncio.create_task(file.executor())
                    task_run.set_name(f"worker_run[{i}][{file.file_orig.name}]")
                    await asyncio.wait([task_run], return_when=asyncio.ALL_COMPLETED)
            except Exception as e:
                logger.error(f"worker_run[{i}]: Error:{str(e)}", exc_info=True)       
    
async def async_main(list_files, workers, args_tk):
    
    logger = logging.getLogger("async_main")
    

    logger.info([afile.file_orig.name for afile in list_files])
    queue_files = asyncio.Queue()
    for file in list_files:
        queue_files.put_nowait(file)
        
    for _ in range(workers):
        queue_files.put_nowait("KILL")
        
    logger.info(list(queue_files._queue))
    
    try:
        
         
            t1 = asyncio.create_task(run_tk(list_files, args_tk, 0.25))
            t1.set_name("tk")
            tasks_run = [asyncio.create_task(worker_run(queue_files,i)) for i in range(workers)]
            for i,t in enumerate(tasks_run):
                t.set_name(f"worker_run[{i}]")
            
            await asyncio.wait([t1] + tasks_run, return_when=asyncio.ALL_COMPLETED)
            
            
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
    
    for i, file in enumerate(vid_orig):
        
        if file.is_symlink():
            logger.info(f"{i}:{file} is symlink to {file.readlink()} file dest {vid_dest[i]}")
            vid_dest[i].symlink_to(file.readlink())
            file.unlink()
            del vid_orig[i]
            del vid_dest[i]
    
    logger.info("Copy symlinks done")    
    logger.info(vid_orig)
    logger.info(vid_dest)
    
  

    list_files = [AsyncFile(vid1, vid2, parts) for vid1, vid2 in zip(vid_orig, vid_dest)]
    
    #logger.info([afile.file_orig.name for afile in list_files])
    
    
   #ex = ThreadPoolExecutor(max_workers=1)
    try:
        args_tk = init_tk_afiles(len(list_files))
        aiorun.run(async_main(list_files, workers,args_tk), use_uvloop=True) 
        
    except Exception as e:
        logger.info(f"aiorun {e}", exc_info=True)   
    
    logger.info(datetime.now() - time1)


if __name__ == "__main__":
    
    init_logging()
    main()
