#!/usr/bin/env python
import asyncio
import logging
import os

import uvloop

from asyncdl import AsyncDL
from utils import (init_argparser, init_logging, patch_http_connection_pool,
                   patch_https_connection_pool)

init_logging()
logger = logging.getLogger("async_all")


def main():
    
    try:
        
        patch_http_connection_pool(maxsize=1000)
        patch_https_connection_pool(maxsize=1000)
        os.environ['MOZ_HEADLESS_WIDTH'] = '1920'
        os.environ['MOZ_HEADLESS_HEIGHT'] = '1080'
         
        args = init_argparser()

        logger.info(f"Hi, lets dl!\n{args}")
                
        asyncDL = AsyncDL(args)        

        try:
            
            asyncDL.wait_for_files()
            
            if not asyncDL.nowaitforstartdl:            
                asyncDL.get_list_videos()
                asyncDL.get_videos_to_dl()            

                if not asyncDL.videos_to_dl:
                    raise Exception("no videos to dl")                

            try:
                uvloop.install()
                asyncDL.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(asyncDL.loop)
                asyncDL.main_task = asyncDL.loop.create_task(asyncDL.async_ex())                  
                asyncDL.loop.run_until_complete(asyncDL.main_task)
            except BaseException as e:
                logger.info(repr(e))
            
                try:
                    asyncDL.reset = True
                    asyncDL.stop_console = True
                    asyncDL.pasres_repeat = False        
                    pending_tasks = asyncio.all_tasks(loop=asyncDL.loop)
                    if pending_tasks:
                        logger.debug(f"pending tasks: {pending_tasks}")
                        for task in pending_tasks:
                            task.cancel()
                    
                        asyncDL.loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                        logger.debug(f"[async_ex] tasks after cancelletation: {asyncio.all_tasks(loop=asyncDL.loop)}")
                    else: logger.debug(f"pending tasks: []")
                finally:
                    asyncio.set_event_loop(None)
                    
                if isinstance(e, KeyboardInterrupt):
                    raise
        
        except BaseException as e:
            asyncDL.clean()
            logger.info(f"{repr(e)}")            
            
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:
            asyncDL.get_results_info()
            asyncDL.close()
            
    
    except BaseException as e:
        logger.exception(f"[asyncdl bye] {repr(e)}")
        if isinstance(e, KeyboardInterrupt):
            raise

    

if __name__ == "__main__":    
    
    try:
        main()
    except BaseException as e:
        logger.exception(f"[main] {repr(e)}")
        if isinstance(e, KeyboardInterrupt):
            raise
        
    
   
     
        

    
