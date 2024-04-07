"""
supportlogging - Python3 Logging

"""
import contextlib
import logging
import logging.handlers
import shutil
import threading
import time
from copy import copy
from logging.config import (  # type: ignore
    ConvertingDict,
    ConvertingList,
    valid_ident,
)
from logging.handlers import QueueHandler, QueueListener
from queue import Empty, Queue
from textwrap import fill

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

    handlers = []
    _LOCK = threading.Lock()

    def __init__(self, handlers, name):

        with QueueListenerHandler._LOCK:
            if name in QueueListenerHandler.handlers:
                return
            QueueListenerHandler.handlers.append(name)
        self._name_logger = name
        super().__init__(_resolve_queue(Queue(-1)))
        self._listener = SingleThreadQueueListener(
            self.queue, name, *_resolve_handlers(handlers), respect_handler_level=True)
        self.start()

    def start(self):
        self._listener.start()

    def stop(self):
        self._listener.stop()


class SingleThreadQueueListener(QueueListener):

    monitor_thread = None
    listeners = []
    sleep_time = 0.1
    _LOCK = threading.Lock()

    def __init__(self, queue, name, *handlers, respect_handler_level=False):
        self.queue = queue
        self.handlers = handlers
        self._thread = None
        self.respect_handler_level = respect_handler_level
        self._name_logger = name

    @classmethod
    def _start(cls):
        with cls._LOCK:
            if cls.monitor_thread is None or not cls.monitor_thread.is_alive():
                cls.monitor_thread = t = threading.Thread(
                    target=cls._monitor_all, name='logging_monitor', daemon=True)
                t.start()
        return cls.monitor_thread

    @classmethod
    def _join(cls):
        """Waits for the thread to stop.
        Only call this after stopping all listeners.
        """
        if cls.monitor_thread is not None and cls.monitor_thread.is_alive():
            cls.monitor_thread.join()
        cls.monitor_thread = None

    @classmethod
    def _monitor_all(cls):
        noop = lambda: None
        while cls.listeners:
            time.sleep(cls.sleep_time)
            for listener in cls.listeners:
                try:
                    task_done = getattr(listener.queue, 'task_done', noop)
                    while True:
                        if (record := listener.dequeue(False)) is listener._sentinel:
                            with contextlib.suppress(ValueError):
                                cls.listeners.remove(listener)
                        else:
                            listener.handle(record)
                        task_done()
                except Empty:
                    continue

    def start(self):
        SingleThreadQueueListener.listeners.append(self)
        SingleThreadQueueListener._start()

    def stop(self):
        self.enqueue_sentinel()


class LogContext:

    def __enter__(self):
        return self

    def __exit__(self, *args):
        list(map(lambda x: x.stop(), SingleThreadQueueListener.listeners))
        SingleThreadQueueListener._join()


def get_logger(name):
    _logger = logging.getLogger(name)
    _logger.propagate = False
    _handlers = logging.root.handlers[0]._listener.handlers
    QueueListenerHandler(_handlers, name=name)
    return _logger
