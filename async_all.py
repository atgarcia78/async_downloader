#!/usr/bin/env python
import logging

import uvloop

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
    except Exception as e:
        logger.error(f"[main] {repr(e)}")
