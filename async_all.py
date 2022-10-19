#!/usr/bin/env python
import asyncio
import os
import uvloop
import logging
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

            try:
                uvloop.install()
                asyncDL.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(asyncDL.loop)
                asyncDL.main_task = asyncDL.loop.create_task(asyncDL.async_ex())                  
                asyncDL.loop.run_until_complete(asyncDL.main_task)
            except BaseException as e:
                logger.info(repr(e))            

                if isinstance(e, KeyboardInterrupt):
                    raise
            finally:
                asyncio.set_event_loop(None)
                    
                
        
        except BaseException as e:
            asyncDL.clean()
            logger.info(f"{repr(e)}")            
            
            if isinstance(e, KeyboardInterrupt):
                raise
        finally:
            asyncDL.get_results_info()
            asyncDL.close()
            return
            
    
    except BaseException as e:
        if not isinstance(e, SystemExit):
            logger.exception(f"[asyncdl bye] {repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise

    

if __name__ == "__main__":    
    try:
        main()
    except BaseException as e:
        if not isinstance(e, SystemExit):
            logger.exception(f"[main] {repr(e)}")
            if isinstance(e, KeyboardInterrupt):
                raise
        
    
   
     
        

    
