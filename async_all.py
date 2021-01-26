#!/usr/bin/env python

import argparse
from queue import Queue
import logging


from asynchttpdownloader import (
    AsyncHTTPDownloader
)

from asynchlsdownloader import (
    AsyncHLSDownloader
)

from common_utils import (
    init_logging,
    TaskPool,
    init_ytdl,
    get_protocol,
    ignore_aiohttp_ssl_error

)

from concurrent.futures import (
    ThreadPoolExecutor,
    ALL_COMPLETED,
    wait
)

import asyncio


init_logging()

logger = logging.getLogger("async_all")


def worker_init_dl(ytdl, queue_vid, nparts, queue_dl, i):
    #worker que lanza los AsyncHLSDownloaders, uno por video
    
    logger.info(f"worker_init_dl[{i}]: launched")

    while True:
        if not queue_vid.empty():
            vid = queue_vid.get()
            logger.info(f"worker_init_dl[{i}]: get for a video to init:")
            logger.debug(f"worker_init_dl[{i}]: {vid}")
            
            try:
                
                if vid.get('_type') == 'url_transparent':
                    info_dict = ytdl.process_ie_result(vid,download=False)
                elif vid.get('_type') == 'url':
                    info_dict = ytdl.extract_info(vid['url'], download=False)
                else:
                    info_dict = vid
                    
                if info_dict:
                    logger.debug(info_dict)
                    protocol = get_protocol(info_dict)
                    logger.debug(f"protocol: {protocol}")
                    if protocol in ('http', 'https'):
                        dl = AsyncHTTPDownloader(info_dict, ytdl, nparts)
                    elif protocol in ('m3u8', 'm3u8_native'):
                        dl = AsyncHLSDownloader(info_dict, ytdl, nparts)
                    else:
                        raise Exception("protocol not supported")
                    
                    queue_dl.put(dl)
                    logger.info(f"worker_init_dl[{i}]: DL constructor ok for {vid['url']}")
                else:
                    logger.warning("no info dict")
                    raise Exception("no info dict")
            except Exception as e:
                logger.warning(f"worker_init_dl[{i}]: DL constructor failed for {vid['url']} - Error: {e}")
        else:
            break



async def main(list_dl, workers, dl_dict):

    
    try:

        pool = TaskPool(workers)#create pool of tasks of workers, workers are not launched
        futures = [pool.submit(dl.fetch_async(), dl.video_url) for dl in list_dl]
        await pool.join()

    except Exception as e:
        logger.debug(e)

        

parser = argparse.ArgumentParser(description="Descargar playlist de videos no fragmentados")
parser.add_argument("-w", help="Number of workers", default="10", type=int)
parser.add_argument("-p", help="Number of parts", default="16", type=int)
#parser.add_argument("-v", help="verbose", action="store_true")
parser.add_argument("-f", help="Format preferred of the video in youtube-dl format", default="bestvideo+bestaudio/best", type=str)
parser.add_argument("--playlist", help="URL should be trreated as a playlist", action="store_true") 
parser.add_argument("--index", help="index of a video in a playlist", default="-1", type=int)
parser.add_argument("url")

args = parser.parse_args()

parts = args.p
workers = args.w
f = args.f

ytdl_opts, ytdl = init_ytdl()

list_videos = []

if args.playlist:

    url_playlist = args.url
    info = ytdl.extract_info(url_playlist,download=False)

    logger.info(info)
    
    
    list_videos = list(info.get('entries'))

    if args.index in range(1,len(list_videos)):
        list_videos = [list_videos[args.index-1]]

else: #url no son playlist

    list_urls = args.url.split(',')
    list_videos = [{'_type': 'url', 'url': el} for el in list_urls]


logger.info(list_videos)

queue_vid = Queue()
for video in list_videos:
    queue_vid.put(video)

queue_dl = Queue()

n = len(list_videos)

with ThreadPoolExecutor(max_workers=workers) as exe:
    
    futures = [exe.submit(worker_init_dl, ytdl, queue_vid, parts, queue_dl, i) for i in range(n)]
    

    done_futs, _ = wait(futures, return_when=ALL_COMPLETED)

list_dl = []
dl_dict = dict()

while not queue_dl.empty():

    dl = queue_dl.get()   
    logger.info(f"{dl.filename}:{dl.info_dict}")
    if dl.filename.exists():
        logger.info(f"Video already downloaded: {dl.filename} {dl.video_url}")
        dl.remove()
    else:
        list_dl.append(dl)
        logger.info(f"Video will be processed: {dl.video_url}")
        dl_dict[dl.info_dict['id']] = dl.video_url


logging.info(dl_dict)


try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

ignore_aiohttp_ssl_error(asyncio.get_event_loop())

asyncio.run(main(list_dl, workers, dl_dict))


