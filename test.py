import logging
import sys

import daiquiri

# Log both to stdout and as JSON in a file called /dev/null. (Requires
# `python-json-logger`)
daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(formatter=daiquiri.formatter.ColorFormatter(fmt=(daiquiri.formatter.DEFAULT_FORMAT))),
        daiquiri.output.File("info.log", formatter=daiquiri.formatter.ColorFormatter(fmt=(daiquiri.formatter.DEFAULT_FORMAT)))
    )
)

logger = daiquiri.getLogger(__name__, subsystem="example")
logger.info("It works and log to stdout and /dev/null with JSON")
logger.error("mal todo")
logger.warning("cuidadin")