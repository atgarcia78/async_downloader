import asyncio
import logging
import os
import shlex
import shutil
import subprocess
from concurrent.futures import wait

from utils import MySyncAsyncEvent, run_operation_in_executor


class RunAsyncDLProc:
    stop = MySyncAsyncEvent('stop_run')

    def __init__(self, cmd: str, shell=True, **kwargs):
        self.cmd = cmd
        self.shell = shell
        self.env = kwargs.get('env') or dict(os.environ)
        self.env['COLUMNS'] = str(shutil.get_terminal_size().columns)
        self.env['LINES'] = '50'
        self.streams = {'stdout': [], 'stderr': []}
        self.tasks = []
        self.logger = logging.getLogger('execproc')

    def non_blocking_readlines(self, chunk=1024):
        os.set_blocking(self.proc.stdout.fileno(), False)
        blocks = []

        def _handle_chunk(_data):
            while True:
                n = _data.find('\n')
                if n == -1:
                    break
                yield ''.join(blocks) + _data[:n + 1]
                _data = _data[n + 1:]
                blocks.clear()
            blocks.append(_data)

        while True:
            try:
                data = self.proc.stdout.read(chunk)
                if not data:
                    yield ''.join(blocks)
                    blocks.clear()
                else:
                    _handle_chunk(data)
            except BlockingIOError as e:
                self.logger.error(f'[nonblckreadl] {repr(e)}')
                yield ''
                continue
            except Exception as e:
                self.logger.exception(f'[nonblckreadl] {repr(e)}')
                raise

    @run_operation_in_executor('list_out')
    def listener_output(self, *args, **kwargs):
        stdout_iter = iter(self.non_blocking_readlines())
        _exit = False
        while True:
            try:
                if RunAsyncDLProc.stop.is_set():
                    break
                if self.proc.poll() is not None:
                    _exit = True
                if line := next(stdout_iter):
                    self.streams['stdout'].append(line)
                    print(line, flush=True, end='')
            except StopIteration as e:
                self.logger.error(repr(e))
                break
            except Exception as e:
                self.logger.exception(repr(e))
            finally:
                if _exit:
                    break

    def run(self, noblock=True):
        self.proc = subprocess.Popen(
            self.cmd if self.shell else shlex.split(self.cmd), shell=self.shell, env=self.env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        if self.proc.poll() is not None:
            return

        _, my_fut = self.listener_output()
        self.logger.info(f'my_fut: {my_fut}')
        if noblock:
            return RunAsyncDLProc.stop, my_fut
        else:
            wait([my_fut])

    async def async_listener_output(self, chunk=1024):
        stream = self.proc.stderr
        blocks = []
        _exit = False

        async def _process_line(_data):
            line = b''.join(blocks) + _data
            line = line.decode('utf-8', 'replace')
            self.streams['stderr'].append(line)
            print(line, flush=True, end='')
            blocks.clear()
            await asyncio.sleep(0)

        async def _process_data(_data):
            while True:
                n = _data.find(b'\n')
                if n == -1:
                    break
                await _process_line(_data[:n + 1])
                _data = _data[n + 1:]
            blocks.append(_data)

        while True:
            try:
                if RunAsyncDLProc.stop.is_set():
                    break
                if self.proc.returncode is not None:
                    chunk = -1
                    _exit = True
                if not (data := await stream.read(chunk)):
                    await _process_line(b'')
                else:
                    await _process_data(data)                
            except Exception as e:
                self.logger.exception(f'[asynclist] {repr(e)}')
            finally:
                if _exit:
                    break

    async def arun(self, noblock=True):
        self.proc = await asyncio.create_subprocess_shell(
            self.cmd, env=self.env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        self.tasks = [
            asyncio.create_task(self.async_listener_output()),
            asyncio.create_task(self.proc.wait()),
        ]

        if noblock:
            return RunAsyncDLProc.stop, self.tasks
        else:
            await asyncio.wait(self.tasks)
