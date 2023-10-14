#!/usr/bin/env python
import uvloop
import logging
from asyncdl import AsyncDL

from utils import init_argparser, init_config

init_config()
logger = logging.getLogger("async_all")


def main():
    args = init_argparser()
    asyncDL = AsyncDL(args)
    uvloop.run(asyncDL.async_ex())


if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        logger.exception(f"[main] {repr(e)}")
