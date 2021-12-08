#!/usr/bin/env python


import logging
import aiorun 
import shutil
import os


from utils import ( 
    init_logging,    
    init_tk,
    init_argparser,
    patch_http_connection_pool,
    patch_https_connection_pool,
    kill_processes)

from concurrent.futures import ThreadPoolExecutor
    
from codetiming import Timer

from asyncdl import AsyncDL


def main():
    
    init_logging()
    logger = logging.getLogger("async_all")
    
    try:
        
        patch_http_connection_pool(maxsize=1000)
        patch_https_connection_pool(maxsize=1000)
        os.environ['MOZ_HEADLESS_WIDTH'] = '1920'
        os.environ['MOZ_HEADLESS_HEIGHT'] = '1080'
         
        t1 = Timer("execution", text="Time spent with data preparation: {:.2f}", logger=logger.info)
        t2 = Timer("execution", text="Time spent with DL: {:.2f}", logger=logger.info)
        
        args = init_argparser()
        
                
        t1.start()
        
        logger.info(f"Hi, lets dl!\n{args}")
                
        asyncDL = AsyncDL(args)        
        
        with ThreadPoolExecutor(thread_name_prefix="Init", max_workers=2) as ex:
            ex.submit(asyncDL.get_videos_cached)
            ex.submit(asyncDL.get_list_videos)
           

        asyncDL.get_videos_to_dl()    
        
        asyncDL.print_list_videos()      
        
        t1.stop()
        
        t2.start()
        
        if asyncDL.videos_to_dl:    
                
            try:                
                args_tk = init_tk()        
                aiorun.run(asyncDL.async_ex(args_tk), use_uvloop=True)                     
            except Exception as e:
                logger.exception(repr(e))

        t2.stop()
        
        res = asyncDL.get_results_info()     
    
        if args.caplinks:
            
            shutil.copy("/Users/antoniotorres/Projects/common/logs/captured_links.txt", "/Users/antoniotorres/Projects/common/logs/prev_captured_links.txt")
            with open("/Users/antoniotorres/Projects/common/logs/captured_links.txt", "w") as file:
            
                session_errors = res['videos_error_dl'] + res['videos_error_init']
                for _video in set(session_errors):
                    line = _video + "\n"
                    file.write(line) 
    
    finally:        
        asyncDL.exit()

if __name__ == "__main__":
    
    main()
     
        

    
