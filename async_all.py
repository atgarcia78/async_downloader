#!/usr/bin/env python
import uvloop

from asyncdl import AsyncDL
from supportlogging import LogContext
from utils import init_argparser, init_config

init_config()

def main():
    with LogContext() as ctx:
        try:
            asyncDL = AsyncDL(init_argparser())
            uvloop.run(asyncDL.async_ex())
            asyncDL.get_results_info()
        except Exception as e:
            ctx.logger.exception(f"[main] {repr(e)}")

if __name__ == "__main__":
    main()