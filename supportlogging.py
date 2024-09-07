import contextlib
import json
import logging
import logging.handlers
import shutil
import threading
import time
from copy import copy
from logging.handlers import QueueListener
from pathlib import Path
from queue import Empty
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

class SingleThreadQueueListener(QueueListener):
    monitor_thread = None
    listeners = []
    sleep_time = 0.1
    _LOCK = threading.Lock()

    def __init__(self, queue, *handlers, respect_handler_level=False, _name_logger="root"):

        super().__init__(queue, *handlers, respect_handler_level=respect_handler_level)
        self._name_logger = _name_logger
        self.start()

    @classmethod
    def monitor_running(cls):
        return cls.monitor_thread is not None and cls.monitor_thread.is_alive()

    @classmethod
    def _start(cls):
        with cls._LOCK:
            if not cls.monitor_running():
                cls.monitor_thread = t = threading.Thread(
                    target=cls._monitor_all, name="logging_monitor", daemon=True)
                t.start()
        return cls.monitor_thread

    def start(self):
        SingleThreadQueueListener.listeners.append(self)
        self._thread = SingleThreadQueueListener._start()

    @classmethod
    def _join(cls):
        """Waits for the thread to stop.
        Only call this after stopping all listeners.
        """
        if cls.monitor_running():
            cls.monitor_thread.join()
        cls.monitor_thread = None

    @classmethod
    def _monitor_all(cls):
        while cls.listeners:
            time.sleep(cls.sleep_time)
            for listener in cls.listeners:
                try:
                    task_done = getattr(listener.queue, "task_done", lambda: None)
                    while True:
                        if (record := listener.dequeue(False)) is listener._sentinel:
                            with contextlib.suppress(ValueError):
                                cls.listeners.remove(listener)
                            task_done()
                            break
                        listener.handle(record)
                        task_done()
                except Empty:
                    continue


    def stop(self):
        self.enqueue_sentinel()


def init_logging(log_name, config_path=None, test=False):
    config_json = Path('/Users/antoniotorres/Projects/async_downloader_py312/logging.json')
    with open(config_json, "r") as f:
        config = json.loads(f.read())
    _to_upt = config["handlers"]["info_file_handler"]
    for key, value in _to_upt.items():
        if key == "filename":
            _to_upt[key] = value.format(name=log_name)
            break

    logging.config.dictConfig(config)

    for _name, logger in logging.Logger.manager.loggerDict.items():
        if isinstance(logger, logging.Logger) and any(
            _name.startswith(_) for _ in ("proxy", "plugins.proxy")
        ):
            logger.setLevel(logging.ERROR)

    if test:
        return logging.getLogger("test")
    else:
        return logging.getLogger(log_name)


class LogContext:
    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        list(map(lambda x: x.stop(), SingleThreadQueueListener.listeners))
        SingleThreadQueueListener._join()
