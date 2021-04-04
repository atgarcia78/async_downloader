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
    init_argparser,
    patch_http_connection_pool,
    patch_https_connection_pool
)

from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    ALL_COMPLETED,
    wait
)

from codetiming import Timer



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


def worker_init_dl(ytdl, queue_vid, nparts, queue_dl, i, nvideos, logger, queue_nok, queue_aldl, files_cached):
    #worker que lanza los Downloaders, uno por video
    
    logger.debug(f"worker_init_dl[{i}]: launched")

    while not queue_vid.empty():
        vid = queue_vid.get()
        logger.debug(f"worker_init_dl[{i}]: get for a video to init:")
        logger.debug(f"worker_init_dl[{i}]: {vid}")
        
        try:
            
            
            if vid.get('id') and vid.get('title'):
                vid_name = f"{vid.get('id')}_{vid.get('title')}"
                #next((s for s in files_cached if file in s), None)
                if vid_name in files_cached:
                    queue_aldl.put(vid)
                    logger.info(f"worker_init_dl[{i}] [{vid.get('id')}][{vid.get('title')}]: init DL already DL : progress [{queue_aldl.qsize() + queue_dl.qsize() + queue_nok.qsize()} out of {nvideos}]")
                    continue
            
            info_dict = None    
            if vid.get('_type') in ('url_transparent', None, 'video'):
                info_dict = ytdl.process_ie_result(vid,download=False)
            elif vid.get('_type') == 'url':
                info_dict = ytdl.extract_info(vid['url'], download=False)
            else:
                logger.debug(f"Type of result not treated yet: {vid.get('_type')}")
                pass
                
            if info_dict:
                logger.debug(f"worker_init_dl[{i}] {info_dict}")
                protocol, final_dict = get_info_dl(info_dict)
                #logger.debug(f"protocol: {protocol}")
                #logger.debug(final_dict)
                dl = None
                if protocol in ('http', 'https'):
                    dl = AsyncHTTPDownloader(final_dict, ytdl, nparts)
                elif protocol in ('m3u8', 'm3u8_native'):
                    dl = AsyncHLSDownloader(final_dict, ytdl, nparts)
                else:
                    logger.error(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: protocol not supported")
                    raise Exception("protocol not supported")
                
                if dl:
                    queue_dl.put(dl)
                    logger.info(f"worker_init_dl[{i}] [{dl.info_dict['id']}][{dl.info_dict['title']}]: init DL OK : {protocol} : progress [{queue_aldl.qsize() + queue_dl.qsize() + queue_nok.qsize()} out of {nvideos}]")
                else: 
                    #queue_nok.put((vid, "Error:NoDLinit"))
                    raise Exception("no DL init")
            else:
                # logger.error(f"{vid['url']}:no info dict")                
                raise Exception("no info dict")
        except Exception as e:
            queue_nok.put((vid, f"Error:{e}"))
            logger.warning(f"worker_init_dl[{i}]: DL constructor failed for {vid} - Error:{e}")
            
        
    logger.debug(f"worker_init_dl[{i}]: finds queue init empty, says bye")
    

def init_pbar(list_dl):
    barformat = '{desc}: {percentage:3.0f}% of {total_fmt}|{bar}|at {rate_fmt} ETA {remaining}'
    
    return([tqdm(desc=dl.info_dict['title'], bar_format=barformat, unit='B', unit_scale=True, dynamic_ncols=True, initial=dl.down_size, unit_divisor=1024, smoothing=0, mininterval=0.5, maxinterval = 1, total=dl.filesize, position=i) for i, dl in enumerate(list_dl)])
        


async def async_ex(list_dl, workers, logger, text0, text1, text2, root):
    try:
        async with AioPool(size=workers+1) as pool:
            
            fut1 = pool.spawn_n(run_tk(root, text0, text1, text2, list_dl, logger, 0.25))
            await asyncio.sleep(2)
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


@Timer(name="decorator")
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
                if len(url_pl_list) == 1: 
                    info_dict = ytdl.extract_info(url_pl_list[0])
                    logger.debug(info_dict)
                    list_videos = info_dict.get('entries')
                else:
                    with ThreadPoolExecutor(max_workers=workers) as ex:
                        futures = [ex.submit(ytdl.extract_info, url_pl) for url_pl in url_pl_list]
                    
                    for fut in futures:
                        logger.debug(fut)
                        list_videos += fut.get('entries')
                        


            else:
                with open(args.target, "r") as file_json:
                    info_json = json.loads(file_json.read())
                
                list_videos = list(info_json)

        else: #url no son playlist

            if not args.file:
                list_videos = [{'_type': 'url', 'url': el.strip(r' \'"')} for el in args.target.split('","')]

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
        
        list_folders = [Path(Path.home(), "testing"), Path("/Volumes/Pandaext4/videos"), Path("/Volumes/T7/videos")]
        
        files_cached = files_cached + [f"{file.stem.upper()}"
                                       for folder in list_folders 
                                        for file in folder.rglob('*')
                                            if (file.is_file() or file.is_symlink()) and not file.stem.startswith('.') and file.suffix.lower() in ('.mp4', '.mkv', '.m3u8')]
       
           
        logger.info(f"Total cached videos: [{len(files_cached)}]")

        queue_vid = Queue()
        for video in list_videos:
            queue_vid.put(video)

        queue_dl = Queue()
        queue_nok = Queue()
        queue_aldl = Queue()
        
                 
        
        if args.nomult:
            worker_init_dl(ytdl, queue_vid, parts, queue_dl, 1 , logger, queue_nok)

        else:
            with ThreadPoolExecutor(max_workers=workers) as exe:
                
                futures = [exe.submit(worker_init_dl, ytdl, queue_vid, 
                                      parts, queue_dl, i, len(list_videos), logger, queue_nok, queue_aldl, files_cached)
                           for i in range(workers)]
        
                #done_futs, _ = wait(futures, return_when=ALL_COMPLETED)

        #will store the DL init exit videos
        _videos_dl = list(queue_dl.queue)
        #will store videos nok for problems udirng download        
        _videos_nok = list(queue_nok.queue)
        #will store videos already DL        
        _videos_aldl = list(queue_aldl.queue)
        
        logger.info(f"Request to DL total of {len(list_videos)}: Already DL: {len(_videos_aldl)} - Number of videos to process: {len(_videos_dl)} - Can't DL: {len(_videos_nok)}")
        
        #logger.info(dl_dict)

        if args.nodl:
            return 0
 
        if _videos_dl:
            
            try:
                
                root_tk, text0_tk, text1_tk, text2_tk = init_tk(len(_videos_dl))
                
                #pbar = init_pbar(list_dl)            
            
                res = aiorun.run(async_ex(_videos_dl, workers, logger, text0_tk, text1_tk, text2_tk, root_tk), use_uvloop=True) 
            
            except Exception as e:
                logger.warning(e, exc_info=True)
                res = 1

    # if not list_dl: return 1
    # else:
        res = 0
        videos_okdl = []
        videos_kodl = []
        videos_kodl_str = []        
        if _videos_dl:
            
            for dl in _videos_dl:
                if not dl.filename.exists():
                    videos_kodl.append(f"[{dl.info_dict['id']}][{dl.info_dict['title']}]") 
                    videos_kodl_str.append(f"{dl.info_dict['webpage_url']}")
                else: videos_okdl.append(f"[{dl.info_dict['id']}][{dl.info_dict['title']}]")
                
        videos_nok = []
        videos_nok_str = []
        _videos_nok = list(queue_nok.queue)
        if _videos_nok:
            for vid in _videos_nok:
                #logger.info(vid)
                item = f"[{vid[0].get('id')}][{vid[0].get('title')}]"
                if item == "[][]": item = vid[0].get('url')
                videos_nok.append(item) 
                videos_nok_str.append(f"{vid[0]['url']}")
        
                
        videos_aldl = []        
        _videos_aldl = list(queue_aldl.queue)
        if _videos_aldl:            
            for vid in _videos_aldl:
                logger.info(vid)
                videos_aldl.append(f"[{vid['id']}][{vid['title']}]")
                
        logger.info(f"******* FINAL SUMMARY *******")
        logger.info(f"Init request: Request to DL: [{len(list_videos)}]")
        logger.info(f"              Already DL: [{len(videos_aldl)}]")
        logger.info(f"              ERROR init DL: [{len(videos_nok)}]")
        logger.info(f"Videos to DL: [{len(_videos_dl)}]")
        logger.info(f"              OK DL: [{len(videos_okdl)}]")
        logger.info(f"              ERROR DL: [{len(videos_kodl)}]")
        
        logger.info(f"******* VIDEO RESULT LISTS *******")        
        logger.info(f"Videos ERROR INIT DL: {videos_nok} \n[{','.join(videos_nok_str)}]")
        logger.info(f"Videos ERROR DL: {videos_kodl} \n[{','.join(videos_kodl_str)}]")
        logger.info(f"Videos ALREADY DL: {videos_aldl}") 
        logger.info(f"Videos DL: {videos_okdl}")
        
    
        
        if not res and (videos_kodl or videos_nok): return 1             
        return res


if __name__ == "__main__":

    init_logging()
    logger = logging.getLogger("async_all")
    patch_http_connection_pool(maxsize=100)
    patch_https_connection_pool(maxsize=100)

    return_value = main_program(logger)

    logger.info(f"async_all return code: {return_value}")

    if return_value != 0:
        sys.exit(return_value)
