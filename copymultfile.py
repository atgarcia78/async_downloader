# coding: utf-8        
from concurrent.futures.thread import ThreadPoolExecutor

from pathlib import Path

import logging
from common_utils import (
    init_logging, 
    init_tk_afiles
    
)
import tkinter as tk
from asyncfile import AsyncFile
from asyncadbfile import AsyncADBFile
import PySimpleGUI as sg

import argparse
import time

from codetiming import Timer
import threading
from send2trash import send2trash

import aiorun




def run_tk(afiles, window, interval):
    
    logger = logging.getLogger("run_tk")    
    
    logger.info("INIT TK")
    
    #root, text0, text1, text2 = args_tk
    
    try:
        
        while True:            
            res = set([file.status for file in afiles])
            #logger.info(res)
            window.write_event_value('-PROGRESS-', None)
            #if ("init" in res and not "running" in res): 
            
            if (not "init" in res and not "running" in res):                
            #    window.write_event_value('-PROGRESS-', None)
                break
            #else:
            #    window.write_event_value('-PROGRESS-', None)

              
            time.sleep(interval)   
            
                  
            
    except Exception as e:        
            raise
    
    logger.info("RUN TK BYE")


    
def copy_main(list_files, workers, window):
    
    logger = logging.getLogger("copy_main")
    

    logger.info([afile.file_orig.name for afile in list_files])   
    
             
        
    
    try:

        with ThreadPoolExecutor(thread_name_prefix="copyfile", max_workers=workers+1) as ex:
                fut_tk = ex.submit(run_tk, list_files, window, 0.25)
                fut = [ex.submit(file.executor) for file in list_files]
         
    except Exception as e:
        logger.info(str(e))
        
    window.write_event_value('-DONE-', None)
            

       
    

@Timer(name="decorator")
def main():
    
    logger = logging.getLogger("main")
    
        
    parser = argparse.ArgumentParser(description="Async move files")
    parser.add_argument("--orig", help="orig folder", default="", type=str)
    parser.add_argument("--dest", help="dest folder", default="", type=str)
    parser.add_argument("-w", help="simult files", default=16, type=int)
    parser.add_argument("-p", help="parts per file", default=16, type=int)
    parser.add_argument("--symlink", help="move orig files of symlinks within the folder", default="", type=str)
    parser.add_argument("--adborig", help="android orig folder", default="", type=str)
    #parser.add_argument("--adbdest", help="android dest folder", default="", type=str)
    
    
    args = parser.parse_args()
    
    workers = args.w 
    parts = args.p
    
    if args.adborig:
        
        adbfiles = AsyncADBFile(args.adborig, args.dest, workers)
        aiorun.run(adbfiles.run(), use_uvloop=True) 
        
        
    else:
        
        if not args.symlink: #the symlink is moved, not the original file
            
            
            vid_orig = [file for file in Path(args.orig).iterdir() if file.is_file() and not file.name.startswith(".")]    
            vid_dest = [Path(args.dest, file.name) for file in vid_orig]
            
            logger.info(vid_orig)
            logger.info(vid_dest)
            
            vid_orig_final = []
            vid_dest_final = []
            for i, file in enumerate(vid_orig):
                
                if file.is_symlink():
                    logger.info(f"{i}:{file} is symlink to {file.readlink()} file dest {vid_dest[i]}")
                    if not file.readlink() == vid_dest[i]:
                        if not vid_dest[i].exists():
                            vid_dest[i].symlink_to(file.readlink())
                    file.unlink()
                
                else:
                    if vid_dest[i].exists() and (file.stat().st_size == vid_dest[i].stat().st_size):
                        logger.info(f"{i}:{file} is already in dest {vid_dest[i]}")
                        try:
                            send2trash(str(file))
                        except Exception as e:
                            logger.error(f"[{file}] error to trash: {str(e)}")
                    else: 
                        vid_orig_final.append(file)
                        vid_dest_final.append(vid_dest[i])

            
            logger.info("Copy symlinks done")    
            logger.info(vid_orig_final)
            logger.info(vid_dest_final)
        
            
        else:
            vid_symlink = [file for file in Path(args.symlink).iterdir() if file.is_symlink()]
            vid_orig = [file.readlink() for file in vid_symlink]
            vid_dest = [Path(str(file)) for file in vid_symlink]
            
            vid_orig_final = []
            vid_dest_final = []
            
            for i, vid in enumerate(vid_orig):
                if vid.exists(): 
                    vid_symlink[i].unlink()
                    vid_orig_final.append(vid)
                    vid_dest_final.append(vid_dest[i])
                    
            logger.info(vid_orig_final)
            logger.info(vid_dest_final)   
            
            

        list_files = [AsyncFile(vid1, vid2, parts) for vid1, vid2 in zip(vid_orig_final, vid_dest_final)]
        
        total_size = sum([file.size for file in list_files])
        
        col1 = [ [sg.Text('Waiting to enter in pool', font='Any 15')],
                [sg.MLine(key='-ML1-'+sg.WRITE_ONLY_KEY, size=(70,30), font='Any 8', default_text="Waiting")]            
                ]
        
        col2 = [ [sg.Text('Now running', font='Any 15')],
                [sg.MLine(key='-ML2-'+sg.WRITE_ONLY_KEY, size=(100,30), font='Any 8', default_text="Waiting")]            
                ]
        
        col3 = [ [sg.Text('Done/Error', font='Any 15')],
                [sg.MLine(key='-ML3-'+sg.WRITE_ONLY_KEY, size=(100,30), font='Any 8', default_text="Waiting")]            
                ]
        
        layout = [  
                    [sg.Column(col1), sg.Column(col2), sg.Column(col3)],
                    [sg.Button('Show'), sg.Button('Go'), sg.Button('Exit')]
                
                ]

        window = sg.Window('COPY FILE', layout, finalize=True)
        
        
        
        while True:
            event, values = window.read()
            if event in ['Go']:
                threading.Thread(target=copy_main, args=(list_files, workers, window,), daemon=True).start()            
            elif event in ['-DONE-', 'Exit']:
                break
            elif event in ['-PROGRESS-']:
                window['-ML1-'+sg.WRITE_ONLY_KEY].update('')
                window['-ML2-'+sg.WRITE_ONLY_KEY].update('')
                window['-ML3-'+sg.WRITE_ONLY_KEY].update('')
                
                for file in list_files:
                    mens = file.print_hookup()
                    #logger.info(f"{file.status}:{mens}")                                        
                    if file.status in ["init"]:
                        window['-ML1-'+sg.WRITE_ONLY_KEY].print(mens)
                    if file.status in ["running"]:                        
                        window['-ML2-'+sg.WRITE_ONLY_KEY].print(mens)
                    if file.status in ["done", "error"]:
                        window['-ML3-'+sg.WRITE_ONLY_KEY].print(mens)


if __name__ == "__main__":
    
    init_logging()
    main()
