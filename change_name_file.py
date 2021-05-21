# coding: utf-8
import re
import logging
from pathlib import Path
import json

from common_utils import init_logging


init_logging()

logger = logging.getLogger("change_name_file")

folder = Path("/Volumes/Pandaext4/videos")

for file in folder.rglob('*'): 
    name = file.stem
    if file.is_file() and name.startswith('.'):
        logger.info(f'Borrado: {file}')
        file.unlink()
        continue
        
    ext = file.suffix
    curr_path = file.parent
    res = re.findall(r'^20(?:19|20|21)(?:0[1-9]|10|11|12)\d\d_(.*)',name)
    if name.startswith('20') and res and file.is_file():
        new_name = res[0] + ext
        file.rename(Path(curr_path,new_name))
        logger.info(f'Rename: {file} -> {Path(curr_path,new_name)}')
    else: logger.info(f'{file}')
    
        
        
        
    
    
