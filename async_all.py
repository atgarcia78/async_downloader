#!/usr/bin/env python


import logging
import aiorun 
import os


from utils import ( 
    init_logging,
    init_argparser,
    patch_http_connection_pool,
    patch_https_connection_pool,
)

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
        
        res = None
        
        try:
            
            with ThreadPoolExecutor(thread_name_prefix="Init", max_workers=2) as ex:
                ex.submit(asyncDL.get_videos_cached)
                ex.submit(asyncDL.get_list_videos)
            

            asyncDL.get_videos_to_dl()    

            t1.stop()
            
            t2.start()
            
            if asyncDL.videos_to_dl:    

                try:                
                    aiorun.run(asyncDL.async_ex(), use_uvloop=True)                     
                except Exception as e:
                    logger.exception(f"[aiorun] {repr(e)}")

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
     
        

    
