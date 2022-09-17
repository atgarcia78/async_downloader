from utils import try_get, init_logging, async_ex_in_executor, traverse_obj, init_gui_rclone, sg, async_wait_time, perform_long_operation
import re
from pathlib import Path
import asyncio
import logging
import shutil
import sys
import uvloop
from concurrent.futures import ThreadPoolExecutor

init_logging()

logger = logging.getLogger("rclone_san")

ex = ThreadPoolExecutor(thread_name_prefix="exe")


class Rclonesan():
    
    _INTERVAL_GUI = 0.2
    
    def __init__(self, folder, transfers, direct):
        self.count = 0
        self.folder = folder
        if not transfers or not transfers.isdecimal(): self.transfers = 6
        else:
            self.transfers = int(transfers)
        if direct and direct != "direct": self.direct = None
        else:
            self.direct = direct
            
        logger.info(f"folder: {self.folder} transfers: {self.transfers} direct: {self.direct}")
        
    
    async def gui(self):
        
        self.window_root = init_gui_rclone()
        
        try:
            list_del = []
            list_mov = {}
            list_rcl = {}
            while True:
                
                await async_wait_time(self._INTERVAL_GUI/2)
                event, value = self.window_root.read(timeout=0)
                if event == sg.TIMEOUT_KEY:
                    continue
                logger.debug(f"{event}:{value}")
                
                if "kill" in event or event == sg.WIN_CLOSED: break
                elif "status" in event:
                    self.window_root['ST'].update(value['status'])                        
                elif "rclone" in event:
                    logger.info(f"{event}:{value}")
                    list_rcl.update(value['rclone'])
                    self.window_root['-ML0-'].update('\n'.join(list(list_rcl.values())))
                elif "move" in event:
                    logger.info(f"{event}:{value}")
                    list_mov.update(value['move'])
                    index, mens = value["move"].popitem()
                    list_rcl.pop(index, None)
                    self.window_root['-ML1-'].update('\n'.join(list(list_mov.values()))) 
                    self.window_root['-ML0-'].update('\n'.join(list(list_rcl.values())))
                elif "del" in event:
                    logger.info(f"{event}:{value}")
                    index, mens = value["del"].popitem()
                    list_del.append(mens)
                    list_mov.pop(index, None)
                    self.window_root['-ML2-'].update('\n'.join(list_del))
                    self.window_root['-ML1-'].update('\n'.join(list(list_mov.values())))  
                       
                    
                await async_wait_time(self._INTERVAL_GUI)
                
        except Exception as e:
            logger.exception(repr(e))
        finally:
            self.window_root.close()
       
    async def worker(self, index, file):
        
        try:
        
            while not file.exists():
                await asyncio.sleep(0)                            

            _text = f"{file.name}:[{index}/{self.num}]"
            mens = {index: _text}            
            
            self.window_root.write_event_value("move", mens)
            if not self.direct:                
                await async_ex_in_executor(ex, shutil.move, str(file), f'/Volumes/WD8_1/videos/{self.folder}')
            
            file2 = Path(f'/Volumes/WD5/videos/{self.folder}', file.name)
            if file2.exists():
                mens = {index: f"{_text} Borramos en WD5"}
                self.window_root.write_event_value("del", mens)
                file2.unlink()
        except Exception as e:
            logger.exception(repr(e))
                
    async def read_stream(self, proc):    
        
        _pattern = r'INFO\s*\:\s*(?P<file>[^\:]+)\:'
        _status = r'Transferred:\s*(?P<prog>[^%]+%),\s*(?P<speed>[^,]+),\sETA\s*(?P<eta>[^\s$]+)\s*'
        _rclone = r'\*\s*(?P<file>[^\:]+)\:'
        
        try:            
            stream = proc.stdout
            list_rclone = {}            
            while not proc.returncode:
                try:                        
                    line = await stream.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    continue                
                if line: 
                    _line = re.sub('[\t\n]', '', line.decode('utf-8'))
                    #logger.debug(_line)
                    _file_rcl = try_get(re.search(_rclone, _line), lambda x: x.group('file').split('…')[0])
                    if _file_rcl and _file_rcl not in list(list_rclone.keys()):
                        async with self.alock:
                            self.count += 1
                            _index = self.count
                            list_rclone.update({_file_rcl: _index})
                        self.window_root.write_event_value("rclone", {_index: f'{_file_rcl}:[{_index}/{self.num}]'})  
                    _prog, _speed, _eta = try_get(re.search(_status, _line), lambda x: x.group('prog', 'speed', 'eta') if x else ("", "", ""))                    # type: ignore
                    if _prog:
                        mens = f"[{_prog}] DL[{_speed}] ETA[{_eta}]"
                        self.window_root.write_event_value("status", mens)
                    _file = try_get(re.search(_pattern, _line), lambda x: x.group('file').split('…')[0])
                    if _file:                        
                        file = Path(self._dest, _file)
                        _index = None
                        for key,ind in list_rclone.items():
                            if key in _file:
                                _index = ind
                                break
                        self._tasks.append(asyncio.create_task(self.worker(_index, file)))
  
                    await asyncio.sleep(0)                                          
                    
                else: break
            
        except Exception as e:
            logger.exception(repr(e))

    async def main(self):
        
        try:
            if not self.direct: self._dest =  f'/Users/antoniotorres/testing/{self.folder}'
            else: self._dest = f'/Volumes/WD8_1/videos/{self.folder}'
            cmd = f"rclone -Pv --no-traverse --no-check-dest --retries 1 --transfers {self.transfers} copy /Volumes/WD5/videos/{self.folder} {self._dest}"
            logger.info(cmd)
            
            Path(f'/Volumes/WD8_1/videos/{folder}').mkdir(parents=True, exist_ok=True)
            files = [file for file in Path(f'/Volumes/WD5/videos/{self.folder}').iterdir()]
            _final = []
            for f in files:
                if f.name.startswith('.'): f.unlink()
                else: _final.append(f)
                
            self.num = len(_final)        
        
            self.alock = asyncio.Lock()
        
            self._tasks = [] 
            
            task_gui = asyncio.create_task(self.gui())
            
            await asyncio.sleep(1)
            
            self.proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE) 
            
            await asyncio.gather(self.read_stream(self.proc), self.proc.wait())
            await asyncio.wait(self._tasks)
            
            self.window_root.write_event_value("kill", "kill")
            
            await asyncio.wait([task_gui])
            
        except Exception as e:
            logger.exception(repr(e))
        
    
if __name__ == "__main__":
    folder = traverse_obj(sys.argv, (1))
    if not folder:
        sys.exit()
    transfers = traverse_obj(sys.argv, (2))
    direct = traverse_obj(sys.argv, (3))
    uvloop.install()
    asyncio.set_event_loop(loop:=asyncio.new_event_loop())
    rclonesan = Rclonesan(folder, transfers, direct)
    main_task = loop.create_task(rclonesan.main())                  
    loop.run_until_complete(main_task)