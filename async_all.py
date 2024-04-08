#!/usr/bin/env python
import logging

import uvloop

from asyncdl import AsyncDL
from supportlogging import LogContext
from utils import init_argparser, init_config

init_config()

logger = logging.getLogger('asyncdl')


def main():
    with LogContext():
        asyncDL = AsyncDL(init_argparser())
        uvloop.run(asyncDL.async_ex())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"[main] {repr(e)}")
