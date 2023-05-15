#!/usr/bin/env python
import asyncio
import logging
from asyncdl import AsyncDL

from utils import (
    init_argparser,
    init_logging,
    init_config
)

init_config()

init_logging()
logger = logging.getLogger("async_all")


def main():

    args = init_argparser()
    logger.info(f"Hi, lets dl!\n{args}")
    asyncDL = AsyncDL(args)

    asyncio.run(asyncDL.async_ex())


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        logger.exception(f"[main] {repr(e)}")
