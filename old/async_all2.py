#!/usr/bin/env python


from queue import Queue
import logging
import sys
import json
import ast
import tkinter as tk
import asyncio
import aiorun 
from asyncio_pool import AioPool
from pathlib import Path
from tqdm import tqdm


from asynchttpdownloader import (
    AsyncHTTPDownloader
)

from asynchlsdownloader import (
    AsyncHLSDownloader
)

from common_utils import ( 
    init_logging,
    init_ytdl,
    init_tk,
    get_info_dl,
    init_argparser
)

from concurrent.futures import (
    ThreadPoolExecutor,
    ALL_COMPLETED,
    wait
)



async def run_tk(root, text0, text1, text2, list_dl, logger, interval):
    '''
    Run a tkinter app in an asyncio event loop.
    '''
    try:
        
        while True:
            root.update()
            res = set([dl.status for dl in list_dl])
            #logger.debug(res)
            if ("init" in res and not "downloading" in res): 
                pass
            elif (not "init" in res and not "downloading" in res):                
                break
            else:
                text0.delete(1.0, tk.END)
                text1.delete(1.0, tk.END)
                text2.delete(1.0, tk.END)    
                for dl in list_dl:
                    mens = dl.print_hookup()
                    if dl.status in ["init"]:
                        text0.insert(tk.END, mens)
                    if dl.status in ["downloading"]:                        
                        text1.insert(tk.END, mens)
                    if dl.status in ["done", "error"]:
                        text2.insert(tk.END,mens)
                    #logger.debug(mens)                 
                
            await asyncio.sleep(interval)
                  
            
    except tk.TclError as e:
        if "application has been destroyed" not in e.args[0]:
            raise
    
    logger.debug("RUN TK BYE")
    

async def update_tqdm(list_dl, pbar, interval):
            
    data1 = [dl.down_size for dl in list_dl]
    while True:
        data2 = [dl.down_size for dl in list_dl]
        for i, pb in enumerate(pbar):
            pb.update(data2[i] - data1[i])
            data1[i] = data2[i]
        await asyncio.sleep(interval)    


async def worker_init_dl(ytdl, queue_vid, nparts, queue_dl, i, nvideos, logger, queue_nok, queue_aldl, files_cached, mypoolex):
    #worker que lanza los Downloaders, uno por video
    
    logger.debug(f"worker_init_dl[{i}]: launched")
    await asyncio.sleep(1)
    
        
    while True: 
   
        vid = await queue_vid.get()
        logger.debug(f"worker_init_dl[{i}]: get for a video to init:")
        logger.debug(f"worker_init_dl[{i}]: {vid}")
        await asyncio.sleep(1)
        
        if vid == "KILL":
            break
        
        try:            
            
            if vid.get('id') and vid.get('title'):
                vid_name = f"{vid.get('id')}_{vid.get('title')}"
                #next((s for s in files_cached if file in s), None)
                if vid_name in files_cached:
                    await queue_aldl.put(vid)                   
                    logger.info(f"worker_init_dl[{i}] [{vid.get('id')}][{vid.get('title')}]: init DL already DL : progress [{queue_aldl.qsize() + queue_dl.qsize() + queue_nok.qsize()} out of {nvideos}]")
                    await asyncio.sleep(1)
                    continue
            
            info_dict = None
            loop = asyncio.get_running_loop()    
            if vid.get('_type') in ('url_transparent', None, 'video'):
                #info_dict = await loop.run_in_executor(None, ytdl.process_ie_result(vid,download=False))
                with mypoolex:
                    info_dict = await loop.run_in_executor(mypoolex, ytdl.process_ie_result(vid,download=False))
                    
            elif vid.get('_type') == 'url':
                with mypoolex:
                    info_dict = await loop.run_in_executor(mypoolex, ytdl.extract_info(vid['url'], download=False))
            else:
                logger.debug(f"Type of result not treated yet: {vid.get('_type')}")
                pass
                
            if info_dict:
                logger.debug(f"worker_init_dl[{i}] {info_dict}")
                protocol, final_dict = get_info_dl(info_dict)
                await asyncio.sleep(1)
                #logger.debug(f"protocol: {protocol}")
                #logger.debug(final_dict)
                if protocol in ('http', 'https'):
                    with mypoolex:
                        dl = await loop.run_in_executor(mypoolex,AsyncHTTPDownloader(final_dict, ytdl, nparts))
                elif protocol in ('m3u8', 'm3u8_native'):
                    with mypoolex:
                        dl = await loop.run_in_executor(mypoolex, AsyncHLSDownloader(final_dict, ytdl, nparts))
                else:
                    logger.error(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: protocol not supported")
                    await asyncio.sleep(1)
                    raise Exception("protocol not supported")
                
                await queue_dl.put(dl)
                logger.info(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : progress [{queue_aldl.qsize() + queue_dl.qsize() + queue_nok.qsize()} out of {nvideos}]")
                await asyncio.sleep(1)
            else:
                # logger.error(f"{vid['url']}:no info dict")                
                raise Exception("no info dict")
        except Exception as e:
            await queue_nok.put((vid, f"Error:{e}"))
            logger.warning(f"worker_init_dl[{i}]: DL constructor failed for {vid} - Error:{e}")
            
        
    logger.debug(f"worker_init_dl[{i}]: finds queue init empty, says bye")
    asyncio.sleep(1)
    

def init_pbar(list_dl):
    barformat = '{desc}: {percentage:3.0f}% of {total_fmt}|{bar}|at {rate_fmt} ETA {remaining}'
    
    return([tqdm(desc=dl.info_dict['title'], bar_format=barformat, unit='B', unit_scale=True, dynamic_ncols=True, initial=dl.down_size, unit_divisor=1024, smoothing=0, mininterval=0.5, maxinterval = 1, total=dl.filesize, position=i) for i, dl in enumerate(list_dl)])
        


async def async_init(ytdl,list_videos, parts, workers, logger, files_cached):
    
    queue_vid = asyncio.Queue()
    for video in list_videos:
        queue_vid.put_nowait(video)
        
    for _ in range(workers):
        queue_vid.put_nowait("KILL")

    queue_dl = asyncio.Queue()
    queue_nok = asyncio.Queue()
    queue_aldl = asyncio.Queue()
    
    nvideos = len(list_videos)
    
    mypoolex = ThreadPoolExecutor(max_workers=workers)
    
    try:
        async with AioPool(size=workers) as pool:
            
            fut = [pool.spawn_n(worker_init_dl(ytdl,queue_vid, parts, queue_dl, i, nvideos, logger, queue_nok, queue_aldl, files_cached, mypoolex)) for i in range(workers)]
            
    except Exception as e:
        logger.warning(e, exc_info=True)
        
    asyncio.get_running_loop().stop()
    
    return(queue_dl, queue_nok, queue_aldl)        
            
            

async def async_ex(list_dl, workers, logger, text0, text1, text2, root):
    try:
        async with AioPool(size=workers+1) as pool:
            
            fut1 = pool.spawn_n(run_tk(root, text0, text1, text2, list_dl, logger, 0.25))
            #fut2 = pool.spawn_n(update_tqdm(list_dl, pbar, 0.25))
            futures = [pool.spawn_n(dl.fetch_async()) for dl in list_dl]
            #futures.append(fut1, fut2)
            futures.append(fut1)
            
            done, pending = await asyncio.wait(futures, return_when=asyncio.ALL_COMPLETED)

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
    with (init_ytdl(dict_opts,args.useragent)) as ytdl:
    
        #logger.debug(ytdl.params)

        if args.playlist:

 
            if not args.file:            
            

                list_videos = []
                url_pl_list = args.target.split(',')
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futures = [ytdl.extract_info(url_pl) for url_pl in url_pl_list]
                
                for fut in futures:
                    logger.debug(fut)
                    list_videos += fut.get('entries')
                    


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
        

        logger.info(f"Total requested videos: {len(list_videos)}")
        logger.debug(list_videos)
        
        if args.cache:
        
            folder_extra_list = [Path(folder) for folder in args.cache.split(',')]
            files_cached = [f"{file.stem.upper()}"
                             for folder in folder_extra_list
                                #for file in folder.iterdir()
                                for file in folder.rglob('*')
                                    if (file.is_file() or file.is_symlink()) and not file.stem.startswith('.') and file.suffix.lower() in ('.mp4', '.mkv', '.m3u8')]
        else: files_cached = []
        
        list_folders = [Path(Path.home(), "testing"), Path("/Volumes/Pandaext4/videos")]
        
        files_cached = files_cached + [f"{file.stem.upper()}"
                                       for folder in list_folders 
                                        for file in folder.rglob('*')
                                            if (file.is_file() or file.is_symlink()) and not file.stem.startswith('.') and file.suffix.lower() in ('.mp4', '.mkv', '.m3u8')]
        # files_cached = files_cached + [f"{file.stem.upper()}" for file in Path(Path.home(), "testing").rglob('*')
        #                             if (file.is_file() or file.is_symlink()) and not file.stem.startswith('.') and file.suffix.lower() in ('.mp4', '.mkv', '.m3u8')]
           
        logger.info(f"Total cached videos: [{len(files_cached)}]")

        # queue_vid = Queue()
        # for video in list_videos:
        #     queue_vid.put(video)

        # queue_dl = Queue()
        # queue_nok = Queue()
        # queue_aldl = Queue()
        
                 
        
        # if args.nomult:
        #     worker_init_dl(ytdl, queue_vid, parts, queue_dl, 1 , logger, queue_nok)

        #else:
        try:
            queue_dl, queue_nok, queue_aldl = aiorun.run(async_init(ytdl,list_videos, parts, workers, logger, files_cached), use_uvloop=True)
        
        except Exception as e:
            logger.warning(e, exc_info=True)
        
            
            # with ThreadPoolExecutor(max_workers=workers) as exe:
                
            #     futures = [exe.submit(worker_init_dl, ytdl, queue_vid, 
            #                           parts, queue_dl, i, len(list_videos), logger, queue_nok, queue_aldl, files_cached)
            #                for i in range(workers)]
        
                #done_futs, _ = wait(futures, return_when=ALL_COMPLETED)

        
        videos_dl = list(queue_dl.queue)
        
        videos_nok = list(queue_nok.queue)        

        
        videos_aldl = list(queue_aldl.queue)
        
        logger.info(f"Request to DL total of {len(list_videos)}: Already DL: {len(videos_aldl)} - Number of videos to process: {len(videos_dl)} - Can't DL: {len(videos_nok)}")
        
        #logger.info(dl_dict)

        if args.nodl:
            return 0
 
        
        try:
            
            root_tk, text0_tk, text1_tk, text2_tk = init_tk(len(videos_dl))
            
            #pbar = init_pbar(list_dl)            
           
            res = aiorun.run(async_ex(videos_dl, workers, logger, text0_tk, text1_tk, text2_tk, root_tk), use_uvloop=True) 
        
        except Exception as e:
            logger.warning(e, exc_info=True)
            res = 1

    # if not list_dl: return 1
    # else:
        res = 0
        videos_okdl = []
        videos_kodl = []
        
        for dl in videos_dl:
            if not dl.filename.exists():
                if dl.info_dict.get('playlist'):
                    item = f"{dl.webpage_url} --index {dl.info_dict['playlist_index']}"
                else: item = dl.webpage_url
                videos_kodl.append(item)
            else: videos_okdl.append(f"[{dl.info_dict['id']}][{dl.info_dict['title']}]")
            
        logger.info(f"******* FINAL SUMMARY *******")
        logger.info(f"Init request: [{len(list_videos)}]")
        logger.info(f"              Already DL: [{len(videos_aldl)}]")
        logger.info(f"              ERROR init DL: [{len(videos_nok)}]")
        logger.info(f"              Videos to DL: [{len(videos_dl)}]")
        logger.info(f"                            OK DL: [{len(videos_okdl)}]")
        logger.info(f"                            ERROR DL: [{len(videos_kodl)}]")
        
        logger.info(f"******* VIDEO RESULT LISTS *******")        
        logger.info(f"Videos ERROR INIT DL: {','.join(videos_nok)}")
        logger.info(f"Videos ERROR DL: {','.join(videos_kodl)}")
        logger.info(f"Videos ALREADY DL: {videos_aldl}") 
        logger.info(f"Videos DL: {videos_okdl}")
        
    
        
        if not res and (videos_kodl or videos_nok): return 1             
        return res


if __name__ == "__main__":

    init_logging()
    logger = logging.getLogger("async_all")
    

    return_value = main_program(logger)

    logger.info(f"async_all return code: {return_value}")

    if return_value != 0:
        sys.exit(return_value)
