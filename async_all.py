#!/usr/bin/env python
import uvloop

from asyncdl import AsyncDL
from supportlogging import LogContext
from utils import init_argparser, init_config

logger = init_config(log_name='asyncdl')


def main():
    with LogContext():
        asyncDL = AsyncDL(init_argparser())
        uvloop.run(asyncDL.async_ex())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"[main] {repr(e)}")
