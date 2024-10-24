import asyncio
import logging
import os
import re
import shlex
import shutil
import subprocess
import time
from concurrent.futures import wait
from queue import Empty, LifoQueue

from utils import CONF_INTERVAL_GUI, MySyncAsyncEvent, ProgressTimer, run_operation_in_executor, mytry_call

_sentinel = str(object())

class TrackingData:
    def __init__(self, std, upt_link, pattern):
        self.std = std
        self.progress_pattern = re.compile(pattern)
        self.upt = upt_link
        self.std_queue = LifoQueue()
        self.timer = ProgressTimer()
        self.logger = logging.getLogger('trackingdata')
    
    def put_std_queue(self, line):
        if line == _sentinel:
            self.std_queue.put_nowait(line)
        elif self.timer.has_elapsed(CONF_INTERVAL_GUI / 4):
            if _parse_output := re.match(self.progress_pattern, line):
                self.std_queue.put_nowait(_parse_output)

    @run_operation_in_executor('handlelines')
    def handle_line(self, proc, **kwargs):
        while proc.poll() is None:
            try:
                if (_parse := self.std_queue.get_nowait()) == _sentinel:
                    self.upt['progress'] = 100.0
                    return True
                else:
                    self.upt['progress'] = float(_parse.group("progress"))
                    time.sleep(CONF_INTERVAL_GUI / 4)
            except Exception as e:
                self.logger.exception(f'[handle_lines] {repr(e)}')
    
    async def async_handle_lines(self, proc):
        while proc.returncode is None:
            try:
                if (_parse := self.std_queue.get_nowait()) == _sentinel:
                    self.upt['progress'] = 100.0
                    return True
                else:
                    self.upt['progress'] = float(_parse.group("progress"))
                    await asyncio.sleep(CONF_INTERVAL_GUI / 4)
            except Empty:
                pass                
            except Exception as e:
                self.logger.exception(f'[async_handle_lines] {repr(e)}')
            finally:
                await asyncio.sleep(0)



class RunAsyncDLProc:

    def __init__(self, cmd: str, shell=True, **kwargs):
        self.cmd = cmd
        self.shell = shell
        self.env = kwargs.get('env') or dict(os.environ)
        self.env['COLUMNS'] = str(shutil.get_terminal_size().columns)
        self.env['LINES'] = '50'
        self.streams = {'stdout': [], 'stderr': []}
        self.tasks = []
        self.logger = logging.getLogger('execproc')
        self.stop = MySyncAsyncEvent('stop_run')
        self.tracker = None
        if trk := kwargs.get('tracker'):
            self.tracker = TrackingData(*trk)

    def _add_line(self, std, line):
        if self.tracker and self.tracker.std == std:
            if line:
                self.tracker.put_std_queue(line)

    def listener_output(self, std, **kwargs):

        @run_operation_in_executor(f'listener_{std}')
        def _wrapper_listener(_std, **kwargs):
            stream = getattr(self.proc, _std)
            to_screen = kwargs.get('to_screen', True)
            chunk = kwargs.get('chunk', 1024)
            blocks = []
            _exit = False

            def _process_line(_data):
                line = ''.join(blocks) + _data
                self.streams[std].append(line)
                if to_screen:
                    print(line, flush=True, end='')
                blocks.clear()
                return line

            def _process_data(_data):
                _lines = []
                while True:
                    n = _data.find('\n')
                    if n == -1:
                        break
                    _lines.append(_process_line(_data[:n + 1]))
                    _data = _data[n + 1:]
                blocks.append(_data)
                return _lines

            while True:
                try:
                    if self.stop.is_set():
                        break
                    if self.proc.poll() is not None:
                        chunk = -1
                        _exit = True

                    if not (data := stream.read(chunk)):
                        _line = _process_line('')
                    else:
                        _line = mytry_call(lambda: _process_data(data)[-1])
                    self._add_line(_std, _line)
                except Exception as e:
                    self.logger.exception(f'[list][{std}] {repr(e)}')
                finally:
                    if _exit:
                        break
            self._add_line(_std, _sentinel)
        try:
            _, fut = _wrapper_listener(std, **kwargs)
            return fut
        except Exception as e:
            self.logger.exception(repr(e))

    def run(self, block=True, to_screen=True):
        self.proc = subprocess.Popen(
            self.cmd if self.shell else shlex.split(self.cmd), shell=self.shell, env=self.env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        if self.proc.poll() is not None:
            return

        self.tasks = [self.listener_output('stdout', to_screen=to_screen), self.listener_output('stderr', to_screen=to_screen)]
        if self.tracker:
            self.tasks.append(self.tracker.handle_line(self.proc))

        self.logger.info(f'my_fut: {self.tasks}')
        if not block:
            return self.tasks
        else:
            wait(self.tasks)
            if self.proc.poll() is None:
                self.proc.wait()
            return self.proc

    async def async_listener_output(self, std, to_screen=True, chunk=1024):
        stream = getattr(self.proc, std)
        blocks = []
        _exit = False

        async def _process_line(_data):
            line = b''.join(blocks) + _data
            line = line.decode('utf-8', 'replace')
            self.streams[std].append(line)
            if to_screen:
                print(line, flush=True, end='')
            blocks.clear()
            return line

        async def _process_data(_data):
            _lines = []
            while True:
                n = _data.find(b'\n')
                if n == -1:
                    break
                _lines.append(await _process_line(_data[:n + 1]))
                _data = _data[n + 1:]
            blocks.append(_data)
            return _lines

        while True:
            try:
                if self.stop.is_set():
                    break
                if self.proc.returncode is not None:
                    chunk = -1
                    _exit = True
                if not (data := await stream.read(chunk)):
                    _line = await _process_line(b'')
                else:
                    _line = _res[-1] if (_res := await _process_data(data)) else None
                self._add_line(std, _line)
            except Exception as e:
                self.logger.exception(f'[asynclist][{std}] {repr(e)}')
            finally:
                if _exit:
                    break
        self._add_line(std, _sentinel)

    async def arun(self, block=True, to_screen=True):
        self.proc = await asyncio.create_subprocess_shell(
            self.cmd, env=self.env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        self.tasks = [
            asyncio.create_task(self.async_listener_output('stderr', to_screen=to_screen)),
            asyncio.create_task(self.async_listener_output('stdout', to_screen=to_screen)),
            asyncio.create_task(self.proc.wait()),
        ]
        if self.tracker:
            self.tasks.append(asyncio.create_task(self.tracker.async_handle_lines(self.proc)))

        if not block:
            return self.tasks
        else:
            await asyncio.wait(self.tasks)
            _streams = list(map(
                lambda x: x.decode(encoding='utf-8', errors='replace') if isinstance(x, bytes) else x,
                list(await self.proc.communicate())))
            self.proc.stdout = _streams[0]
            self.proc.stderr = _streams[1]
            return self.proc
