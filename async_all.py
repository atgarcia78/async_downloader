#!/usr/bin/env python
import asyncio
import os
import uvloop
import logging
from asyncdl import AsyncDL
from utils import (
    init_argparser,
    init_logging,
    patch_http_connection_pool,
    patch_https_connection_pool)

init_logging()
logger = logging.getLogger("async_all")


def main():

    patch_http_connection_pool(maxsize=1000)
    patch_https_connection_pool(maxsize=1000)
    os.environ['MOZ_HEADLESS_WIDTH'] = '1920'
    os.environ['MOZ_HEADLESS_HEIGHT'] = '1080'

    args = init_argparser()

    logger.info(f"Hi, lets dl!\n{args}")

    asyncDL = AsyncDL(args)
    try:
        uvloop.install()
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        asyncio.run(asyncDL.async_ex())
        asyncDL.get_results_info()
    except BaseException as e:
        logger.exception(f"[main] {repr(e)}")
    finally:
        asyncDL.close()


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        logger.exception(f"[main] {repr(e)}")
