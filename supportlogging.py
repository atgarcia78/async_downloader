"""
supportlogging - Python3 Logging

"""
import atexit
import logging
import logging.handlers
import shutil
from copy import copy
from logging.config import (
    ConvertingDict,  # type: ignore
    ConvertingList,  # type: ignore
    valid_ident,
)
from logging.handlers import QueueHandler, QueueListener
from textwrap import fill

try:
    from multiprocess import Queue  # type: ignore
except ImportError:
    logging.error("Please install multiprocess")
    from multiprocessing import Queue

MAPPING = {
    "DEBUG": 34,  # white
    "INFO": 32,  # cyan
    "WARNING": 33,  # yellow
    "ERROR": 31,  # red
    "CRITICAL": 41,  # white on red bg
}

PREFIX = "\033["
SUFFIX = "\033[0m"


class Wintxt(logging.StreamHandler):
    _GUI_LOG = None

    def __init__(self, stream=None):
        super().__init__(self)


class FileFormatter(logging.Formatter):
    def format(self, record):
        file_record = copy(record)
        if file_record.msg.startswith("%no%"):
            file_record.msg = file_record.msg[4:]
        return logging.Formatter.format(self, file_record)


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        colored_record = copy(record)
        levelname = colored_record.levelname
        seq = MAPPING.get(levelname, 37)  # default white
        colored_levelname = ("{0}{1}m{2}{3}").format(PREFIX, seq, levelname, SUFFIX)
        colored_record.levelname = colored_levelname
        # if 'proxy.' in colored_record.name:
        #    colored_record.name = colored_record.name.split('.')[0]
        # if colored_record.msg.startswith("%no%"):
        #     colored_record.msg = colored_record.msg[4:]
        if "%no%" in colored_record.msg:
            colored_record.msg = colored_record.msg.replace("%no%", "")

        else:
            lines = colored_record.msg.splitlines()
            colored_lines = []
            col = shutil.get_terminal_size().columns
            for line in lines:
                _lines = fill(line, (col - 66 - 2), replace_whitespace=False)
                colored_lines += _lines.splitlines()
            _indent = "\n" + " " * 66
            colored_record.msg = f"{_indent}".join(colored_lines)
        return logging.Formatter.format(self, colored_record)


class FilterModule(logging.Filter):
    def __init__(self, patterns):
        super(FilterModule, self).__init__()
        self._patterns = patterns

    def filter(self, record):
        for pattern in self._patterns:
            if pattern in record.name:
                return False
        else:
            return True


class FilterMsg(logging.Filter):
    def __init__(self, patterns):
        super(FilterMsg, self).__init__()
        self._patterns = patterns

    def filter(self, record):
        for pattern in self._patterns:
            if pattern["name"] in record.name:
                for text in pattern["text"]:
                    if text in record.msg:
                        return False
        else:
            return True


def _resolve_handlers(_list):
    if not isinstance(_list, ConvertingList):
        return _list

    # Indexing the list performs the evaluation.
    return [_list[i] for i in range(len(_list))]


def _resolve_queue(q):
    if not isinstance(q, ConvertingDict):
        return q
    if "__resolved_value__" in q:
        return q["__resolved_value__"]

    cname = q.pop("class")
    klass = q.configurator.resolve(cname)
    props = q.pop(".", None)
    kwargs = {k: q[k] for k in q if valid_ident(k)}
    result = klass(**kwargs)
    if props:
        for name, value in props.items():
            setattr(result, name, value)

    q["__resolved_value__"] = result
    return result


class QueueListenerHandler(QueueHandler):
    def __init__(self, handlers, respect_handler_level=False, auto_run=True, queue=Queue(-1)):
        queue = _resolve_queue(queue)
        super().__init__(queue)
        handlers = _resolve_handlers(handlers)
        self._listener = QueueListener(self.queue, *handlers, respect_handler_level=respect_handler_level)
        if auto_run:
            self.start()
            atexit.register(self.stop)

    def start(self):
        self._listener.start()

    def stop(self):
        self._listener.stop()

    def emit(self, record):
        return super().emit(record)
