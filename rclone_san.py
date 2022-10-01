from utils import (try_get, init_logging, async_ex_in_executor, 
                   traverse_obj, init_gui_rclone, sg, async_wait_time, 
                   rclone_init_args, wait_until, async_wait_until)
import re
from pathlib import Path
import asyncio
import logging
import shutil
import sys
import uvloop
from concurrent.futures import ThreadPoolExecutor
import aiofiles.os as os

init_logging()

logger = logging.getLogger("rclone_san")

ex = ThreadPoolExecutor(thread_name_prefix="exe")


class Rclonesan():
    
    _INTERVAL_GUI = 0.2
    
    def __init__(self, origfolder, args):
        self.count = 0
        self.orig_path = origfolder
        self.folder = self.orig_path.split('/')[-1]
        self.transfers = args.transfers
        self.direct = args.direct
        self.dest = f'{args.dest}/{self.folder}'

        if not Path(self.orig_path).exists(): raise Exception("doesnt exist")
        
        logger.info(f"folder: {self.folder} transfers: {self.transfers} direct: {self.direct}")
        
        files = [file for file in Path(f'{self.orig_path}').iterdir()]
        _final = []
        for f in files:
            if f.name.startswith('.'): f.unlink()
            else: _final.append(f)
                
        self.num = len(_final) 
        
        if self.num == 0: raise Exception("folder empty")
        
        

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
                elif "init" in event:
                    list_del = []
                    list_mov = {}
                    list_rcl = {}
                    self.window_root['ST'].update("Waiting for info")
                    self.window_root['-ML0-'].update("Waiting for info")
                    self.window_root['-ML1-'].update("Waiting for info")
                    self.window_root['-ML2-'].update("Waiting for info")
                    
                           
                    
                await async_wait_time(self._INTERVAL_GUI)
                
        except Exception as e:
            logger.exception(repr(e))
        finally:
            self.window_root.close()
       
    async def worker(self, index, file):
        
        try:            


            await async_wait_until(5, cor=os.path.exists, args=(file,), interv=1)
            
            _text = f"{file.name}:[{index}/{self.num}]"
            mens = {index: _text}            
            
            await async_ex_in_executor(ex, self.window_root.write_event_value, "move", mens)
            if not self.direct:
                async with self.sem:
                    file_dest = Path(self.dest, file.name)
                    logger.info(f"Moving: {str(file)} -> {str(file_dest)}")                
                    await async_ex_in_executor(ex, shutil.copyfile, str(file), str(file_dest))
                if (await os.path.exists(file_dest)):
                    filesize = (await os.stat(file)).st_size
                    file_destsize = (await os.stat(file_dest)).st_size
                    if (filesize - 100 <= file_destsize <= filesize + 100):
                       file.unlink()
                    else: logger.warning(f"Issue when copying [{str(file)}][{filesize}] to [{str(file_dest)}][{file_destsize}], sizes differ. Please check")
                else: logger.warning(f"Issue when copying (1)[{str(file)}] to (2)[{str(file_dest)}], (2) doesnt exist. Please check")
                
            file2 = Path(f'{self.orig_path}', file.name)
            if (await os.path.exists(file2)):
                mens = {index: f"{_text} Borramos en orig"}
                await async_ex_in_executor(ex, self.window_root.write_event_value, "del", mens)
                file2.unlink()
        except Exception as e:
            logger.exception(repr(e))
                
    async def parser(self, data):
                    
        try:        
            _file_rc, _prog, _speed, _eta, _file_cp = data.values()
            logger.debug(f"{_file_rc}, {_prog}, {_speed}, {_eta}, {_file_cp}")
            
            if _file_rc:
                _file_rc = _file_rc.split('…')[0]
                if _file_rc not in list(self.list_rclone.keys()):
                    async with self.alock:
                        self.count += 1
                        _index = self.count
                        self.list_rclone.update({_file_rc: _index})
                    await async_ex_in_executor(ex, self.window_root.write_event_value,"rclone", {_index: f'{_file_rc}:[{_index}/{self.num}]'})
                            
            if _prog:
                mens = f"[{_prog}] DL[{_speed}] ETA[{_eta}]"
                await async_ex_in_executor(ex, self.window_root.write_event_value, "status", mens)
            
        
            if _file_cp:                        
                _file_cp = _file_cp.split('…')[0]
                _index = None
                for key,ind in self.list_rclone.items():
                    if key in _file_cp:
                        _index = ind
                        break
                if not _index:
                    logger.warning(f"{_file_cp} not registered in rclone\n{self.list_rclone}")
                else:
                    file = Path(self._dest, _file_cp)
                    self._tasks.append(asyncio.create_task(self.worker(_index, file)))
        except Exception as e:
            logger.exception(repr(e))
    
    async def read_stream(self, proc):    
        
        pat = r'(?:(?:(\*\s*(?P<file_rc>[^\:]+)\:[^T\$]+)|^)(?:$|(Transferred:\s*(?P<prog>[^%]+%),\s*(?P<speed>[^,]+),\sETA\s*(?P<eta>[^\s$]+)\s*)))|(?:INFO\s*\:\s*(?P<file_cp>[^\:]+)\:)'
        
        comp = re.compile(pat)
        
        try:            
            stream = proc.stdout
         
            while not proc.returncode:
                
                await asyncio.sleep(0)
                
                try:                        
                    line = await stream.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    continue                
                if line: 

                    _line = line.decode('utf-8').strip()
                    #logger.debug(_line)
                    
                    data = try_get(comp.search(_line), lambda x: x.groupdict() if x else None)
                    #logger.debug(data)
                    if data: self._tasks.append(asyncio.create_task(self.parser(data)))
                
                else: break

            
        except Exception as e:
            logger.exception(repr(e))

    async def main(self):
        
        self.window_root = None
        
        try:
            Path(self.dest).mkdir(parents=True, exist_ok=True)
            if not self.direct:               
                self._dest =   f'/Users/antoniotorres/testing/{self.folder}'
                Path(self._dest).mkdir(parents=True, exist_ok=True)
            else:
                self._dest = self.dest
            
            cmd = f"rclone -Pv --no-traverse --no-check-dest --retries 1 --transfers {self.transfers} copy {self.orig_path} {self._dest}"
            logger.info(cmd)            
            
            self.alock = asyncio.Lock()
            self.sem = asyncio.Semaphore(self.transfers)
            task_gui = asyncio.create_task(self.gui())
        
            n = 0
            while True:
                
                self._tasks = [] 
            
                self.list_rclone = {}    
            
                await asyncio.sleep(1)
            
                self.proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE) 
            
                await asyncio.gather(self.read_stream(self.proc), self.proc.wait())
                await asyncio.wait(self._tasks)
                
                if self.proc.returncode == 0: break
                else: 
                    n += 1
                    if n > 3: raise Exception(f"[{self.orig_path}] max repeats rclone")
                    else:
                        logger.info(f"[{self.orig_path}] rclone returncode error {self.proc.returncode}. Reset[{n}]")
                        await async_ex_in_executor(ex, self.window_root.write_event_value, "init", {})
                        #await async_wait_until(30, cor=os.path.exists, args=('/Volumes/WD5',), interv=5)
                        continue
                    
        except Exception as e:
            logger.exception(repr(e))
        finally:
            if self.window_root:
                await async_ex_in_executor(ex, self.window_root.write_event_value, "kill", {})
                await asyncio.wait([task_gui])
                
                 
if __name__ == "__main__":
    
    args = rclone_init_args()

    uvloop.install()
    asyncio.set_event_loop(loop:=asyncio.new_event_loop())
    
    for folder in args.origfolders:
        try:
            logger.info("************* " + folder)
            #wait_until(60, statement=lambda x: Path(x).exists(), args=('/Volumes/WD5',), interv=5)
            rclonesan = Rclonesan(folder, args)
            main_task = loop.create_task(rclonesan.main())                  
            loop.run_until_complete(main_task)
        except Exception as e:
            logger.warning(f"[{folder}] {repr(e)}")