#!/usr/bin/env python


from asyncio.events import new_event_loop
from queue import Queue
import logging
from re import U
from urllib.parse import urlparse
import sys
import json
import ast

from tkinter import Tk, Text, END
import tkinter
import uvloop


from asynchttpdownloader import (
    AsyncHTTPDownloader
)

from asynchlsdownloader import (
    AsyncHLSDownloader
)

from common_utils import ( 
    init_logging,
    init_ytdl,
    get_protocol,
    init_argparser,
    foldersize,
    naturalsize

)

from concurrent.futures import (
    ThreadPoolExecutor,
    ALL_COMPLETED,
    wait
)

import asyncio

import aiorun 

from asyncio_pool import AioPool

from time import sleep

async def run_tk(root, text, list_dl, logger, interval):
    '''
    Run a tkinter app in an asyncio event loop.
    '''
    try:
        
        while True:
            root.update()
            text.delete(1.0,END)
            res = set([dl.status for dl in list_dl])
            logger.debug(res)
            if "init" in res: 
                pass
            elif not "downloading" in res:
                logger.debug(res)
                break
            else:    
                for dl in list_dl:
                    mens = dl.print_hookup()
                    text.insert(END, mens)
                    logger.debug(mens)                 
                
            await asyncio.sleep(interval)
                  
            
    except tkinter.TclError as e:
        if "application has been destroyed" not in e.args[0]:
            raise
    logger.debug("RUN TK BYE")


# def worker_hookup(list_dl, logger, text):
    
#     logger.debug("WORKER HOOKUP")
#     sleep(1)
#     try:
        
#         while True:
#             text.delete(1.0,END)
#             for dl in list_dl:
#                 text.insert(END, dl.print_hookup()) 
#             res = [dl.status for dl in list_dl]
#             if not "downloading" in res: break       
#             sleep(0.5)
#     except Exception as e:
#         logger.warning(f"error worker hookup {e}")
        
#     logger.debug("WORKER HOOKUP BYE")    
        

def worker_init_dl(ytdl, queue_vid, nparts, queue_dl, i, logger, queue_nok):
    #worker que lanza los AsyncHLSDownloaders, uno por video
    
    logger.debug(f"worker_init_dl[{i}]: launched")

    while not queue_vid.empty():
        vid = queue_vid.get()
        logger.debug(f"worker_init_dl[{i}]: get for a video to init:")
        logger.debug(f"worker_init_dl[{i}]: {vid}")
        
        try:
            
            info_dict = None
            if vid.get('_type') in ('url_transparent', None, 'video'):
                info_dict = ytdl.process_ie_result(vid,download=False)
            elif vid.get('_type') == 'url':
                info_dict = ytdl.extract_info(vid['url'], download=False)
            else:
                logger.debug(f"Type of result not treated yet: {vid.get('_type')}")
                pass
                
            if info_dict:
                logger.debug(info_dict)
                protocol = get_protocol(info_dict)
                logger.debug(f"protocol: {protocol}")
                if protocol in ('http', 'https'):
                    dl = AsyncHTTPDownloader(info_dict, ytdl, nparts)
                elif protocol in ('m3u8', 'm3u8_native'):
                    dl = AsyncHLSDownloader(info_dict, ytdl, nparts)
                else:
                    logger.warning(f"{vid['url']}: protocol not supported")
                    raise Exception(f"{vid['url']}: protocol not supported")
                
                queue_dl.put(dl)
                logger.debug(f"worker_init_dl[{i}]: DL constructor ok for {vid['url']}")
            else:
                logger.warning(f"{vid['url']}:no info dict")                
                raise Exception(f"{vid['url']}:no info dict")
        except Exception as e:
            logger.warning(f"worker_init_dl[{i}]: DL constructor failed for {vid['url']} - Error: {e}")
            queue_nok.put((vid['url'], f"Error: {e}"))
        
    logger.debug(f"worker_init_dl[{i}]: finds queue init empty, says bye")
    



async def main(list_dl, workers, dl_dict, logger, text, root):

    
    futures = []
    
    # coro = asyncio.to_thread(worker_hookup, list_dl, logger, text) 
    

    try:
        async with AioPool(size=workers) as pool:
            
            futures = [pool.spawn_n(dl.fetch_async()) for dl in list_dl]

            [_, (done, pending)] = await asyncio.gather(
                #coro,
                run_tk(root, text, list_dl, logger, 0.1),
                asyncio.wait(futures, return_when=asyncio.ALL_COMPLETED)                
            )
            
            #done_tasks, pending_tasks = await asyncio.wait(futures, return_when=asyncio.ALL_COMPLETED)

            if pending:
                try:
                    await pool.cancel(pending)
                    logger.debug(f"{len(pending)} tasks pending cancelled")
                except Exception as e:
                    logger.debug(f"{e}")

            await asyncio.gather(*pending, return_exceptions=True)
            
            for task in done:
                try:
                    res = task.result()
                    logger.debug(res)
                except Exception as e:
                    logger.warning(f"{e}")    

    except Exception as e:
        logger.warning(e)

    
    asyncio.get_running_loop().stop()


def main_program(logger):

    args = init_argparser()
    parts = args.p
    workers = args.w
    
    dict_opts = {'format': args.format, 'proxy': args.proxy}
    if args.nocheckcert:
        dict_opts.update({"nocheckcertificate" : True})
    if args.ytdlopts:
        dict_opts.update(ast.literal_eval(args.ytdlopts))

    #lets get the list of videos to download

    #list_videos = []
    
    with (init_ytdl(dict_opts,args.useragent)) as ytdl:
    
        logger.debug(ytdl.params)

        if args.playlist:

            if len(args.target.split(",")) > 1:
                logger.error("only one target is allowed with playlist option")
                sys.exit(127)

            if not args.file:            
            
                url_playlist = args.target
                info = ytdl.extract_info(url_playlist)
                logger.debug(info)                
                list_videos = list(info.get('entries'))


            else:
                with open(args.target, "r") as file_json:
                    info_json = json.loads(file_json.read())
                
                list_videos = list(info_json)

        else: #url no son playlist

            if not args.file:

                list_videos = [{'_type': 'url', 'url': el} for el in args.target.split(",")]

            else:
                def get_info_json(file):
                    with open(file, "r") as f:
                        return json.loads(f.read())
                
                list_videos = [get_info_json(file) for file in args.target.split(",")]


        if args.index:
            if args.index in range(1,len(list_videos)):
                list_videos = [list_videos[args.index-1]]
            else:
                logger.error(f"index video {args.index} out of range [1..{len(list_videos)}]")
                sys.exit(127)
        if args.start and args.end:
            if ((args.start in range(1,len(list_videos))) and (args.end in range(1,len(list_videos))) and (args.start <= args.end)):
                list_videos = list_videos[args.start-1:args.end-1]
        

        logger.debug(list_videos)

        queue_vid = Queue()
        for video in list_videos:
            queue_vid.put(video)

        queue_dl = Queue()
        queue_nok = Queue()

        
        with ThreadPoolExecutor(max_workers=workers) as exe:
            
            futures = [exe.submit(worker_init_dl, ytdl, queue_vid, parts, queue_dl, i, logger, queue_nok) for i in range(len(list_videos))]
     
            done_futs, _ = wait(futures, return_when=ALL_COMPLETED)

                    

        list_dl = []
        dl_dict = dict()

        n_downloads = 0

        while not queue_dl.empty():
            
            dl = queue_dl.get()   
            logger.debug(f"{dl.filename}:{dl.info_dict}")
            if dl.filename.exists():
                logger.info(f"Video already downloaded: {dl.filename} {dl.webpage_url}")
                n_downloads += 1
                dl.remove()
            else:
                list_dl.append(dl)
                logger.info(f"Video will be processed: {dl.webpage_url}")
                dl_dict[dl.info_dict['id']] = dl.webpage_url

        n_nok = 0
        
        while not queue_nok.empty():
            
            v_url, error_m = queue_nok.get()
            logger.debug(f"Video wont be processed {v_url}:{error_m}")
            n_nok += 1
        
        logger.info(f"Request to DL total of {len(list_videos)}: Already DL: {n_downloads} - Number of videos to process: {len(list_dl)} - Can't DL: {n_nok}")
        logger.info(dl_dict)

        if args.nodl:
            return 0


        

                 
        
        # with ThreadPoolExecutor() as ex:
        #     fut = ex.submit(root.mainloop())
        

        res = 1     
        

        try:
            
            root = Tk()
            root.geometry('{}x{}'.format(1250, 100))
            text = Text(root, font=("Source Code Pro", 9))
            text.pack(expand=True, fill='both')
            for dl in list_dl:
                
                text.insert(END, dl.print_hookup())            
            
            res = aiorun.run(main(list_dl, workers, dl_dict, logger, text, root), use_uvloop=True) 
        except Exception as e:
            logger.warning(e, exc_info=True)

    if not list_dl: return 1
    else:
        res = 0
        for dl in list_dl:
            if not dl.filename.exists():
                logger.info(f"{dl.filename}:Not DL")
                res = 1
            else: logger.info(f"{dl.filename}:DL")
                
    return(res)


if __name__ == "__main__":

    init_logging()
    logger = logging.getLogger("async_all")

    return_value = main_program(logger)

    logger.info(f"rescode: {return_value}")

    if return_value != 0:
        sys.exit(return_value)
