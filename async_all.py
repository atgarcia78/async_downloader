#!/usr/bin/env python
import uvloop
from asyncdl import AsyncDL
from utils import init_argparser, init_config

logger, logctx = init_config(log_name='asyncdl')

def main():
    with logctx:
        try:
            asyncDL = AsyncDL(init_argparser())
            uvloop.run(asyncDL.async_ex())
            asyncDL.get_results_info()
        except Exception as e:
            logger.exception(f"[main] {repr(e)}")

if __name__ == "__main__":
    main()