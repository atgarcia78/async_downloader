#!/usr/bin/env python


import logging
from aiorun import run 
import asyncio
import os
import uvloop


from utils import ( 
    init_logging,
    init_argparser,
    patch_http_connection_pool,
    patch_https_connection_pool,
)

from concurrent.futures import ThreadPoolExecutor
    
from codetiming import Timer

from asyncdl import AsyncDL



init_logging()
logger = logging.getLogger("async_all")

def shutdown_handler(loop):
    logger.info(f"[aiorun] Entering custom shutdown handler")
    raise Exception("aiorun_custom")

def main():
    
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
        
        
        try:
            
            with ThreadPoolExecutor(thread_name_prefix="Init", max_workers=2) as ex:
                ex.submit(asyncDL.get_videos_cached)
                ex.submit(asyncDL.get_list_videos)
            

            asyncDL.get_videos_to_dl()    

            t1.stop()
            
            t2.start()
            
            if asyncDL.videos_to_dl:    

                try:
                    uvloop.install()
                    asyncDL.loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(asyncDL.loop)
                    asyncDL.main_task = asyncDL.loop.create_task(asyncDL.async_ex())                  
                    asyncDL.loop.run_until_complete(asyncDL.main_task)
                except (KeyboardInterrupt, Exception) as e:
                    logger.info(repr(e))
                
                    try:        
                        pending_tasks = asyncio.all_tasks(loop=asyncDL.loop)
                        if pending_tasks:
                            logger.info(f"pending tasks: {pending_tasks}")
                            for task in pending_tasks:
                                task.cancel()
                        
                            asyncDL.loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                            logger.info(f"[async_ex] tasks after cancelletation{asyncio.all_tasks(loop=asyncDL.loop)}")
                        else: logger.info(f"pending tasks: []")
                    finally:
                        asyncio.set_event_loop(None)
                        asyncDL.close()

            t2.stop()
        except Exception as e:
            logger.exception(f"[asyncdl results] {repr(e)}")
        finally:
            asyncDL.get_results_info()
            asyncDL.close()
    
    except Exception as e:
        logger.exception(f"[asyncdl bye] {repr(e)}")

    

if __name__ == "__main__":
    
    
    main()
    
   
     
        

    
