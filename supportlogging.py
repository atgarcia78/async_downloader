"""
supportlogging - Python3 Logging 

"""

import logging
import logging.handlers

from logging.config import ConvertingList, ConvertingDict, valid_ident
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
import atexit

from copy import copy

from textwrap import fill


    

MAPPING = {
    'DEBUG'   : 34, # white
    'INFO'    : 32, # cyan
    'WARNING' : 33, # yellow
    'ERROR'   : 31, # red
    'CRITICAL': 41, # white on red bg
}

PREFIX = '\033['
SUFFIX = '\033[0m'

class FileFormatter(logging.Formatter):

    def __init__(self, pattern):
        logging.Formatter.__init__(self, pattern)

    def format(self, record):
        file_record = copy(record)
        if file_record.msg.startswith("%no%"): file_record.msg = file_record.msg[4:]
        return logging.Formatter.format(self, file_record)
class ColoredFormatter(logging.Formatter):

    def __init__(self, pattern):
        logging.Formatter.__init__(self, pattern, "%H:%M:%S")

    def format(self, record):
        colored_record = copy(record)
        levelname = colored_record.levelname
        seq = MAPPING.get(levelname, 37) # default white
        colored_levelname = ('{0}{1}m{2}{3}') \
            .format(PREFIX, seq, levelname, SUFFIX)
        colored_record.levelname = colored_levelname
        colored_record.msg = fill(colored_record.msg.replace("\n", "\n" + ' '*58), 200, subsequent_indent=' '*58, replace_whitespace=False) if not colored_record.msg.startswith("%no%") else colored_record.msg[4:]
        return logging.Formatter.format(self, colored_record)

class FilterModule(logging.Filter):

    def __init__(self, patterns):
        super(FilterModule, self).__init__()
        self._patterns = patterns

    def filter(self, record):
        for pattern in self._patterns:
            if pattern in record.name: return False
        else: return True
        
class Debug2Info(logging.Filter):

        
    def filter(self, record):
        if record.name == "youtube_dl" and record.levelno ==  logging.DEBUG:
            print("HIT")
            record.levelno = logging.INFO
            record.levelname = logging._levelToName[record.levelno]
        return True
        
        
def _resolve_handlers(l):
    if not isinstance(l, ConvertingList):
        return l

    # Indexing the list performs the evaluation.
    return [l[i] for i in range(len(l))]


def _resolve_queue(q):
    if not isinstance(q, ConvertingDict):
        return q
    if '__resolved_value__' in q:
        return q['__resolved_value__']

    cname = q.pop('class')
    klass = q.configurator.resolve(cname)
    props = q.pop('.', None)
    kwargs = {k: q[k] for k in q if valid_ident(k)}
    result = klass(**kwargs)
    if props:
        for name, value in props.items():
            setattr(result, name, value)

    q['__resolved_value__'] = result
    return result


class QueueListenerHandler(QueueHandler):

    def __init__(self, handlers, respect_handler_level=False, auto_run=True, queue=Queue(-1)):
        queue = _resolve_queue(queue)
        super().__init__(queue)
        handlers = _resolve_handlers(handlers)
        self._listener = QueueListener(
            self.queue,
            *handlers,
            respect_handler_level=respect_handler_level)
        if auto_run:
            self.start()
            atexit.register(self.stop)

    def start(self):
        self._listener.start()

    def stop(self):
        self._listener.stop()

    def emit(self, record):
        return super().emit(record)

    
    

            
    
 
