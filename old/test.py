# coding: utf-8
import re
import time
import httpx
from selenium.webdriver import Firefox, FirefoxProfile, FirefoxOptions, DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from browsermobproxy import Server
import sys
import traceback
import random
_FF_PROF = [        
            "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/0khfuzdw.selenium0","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/xxy6gx94.selenium","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/wajv55x1.selenium2","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/yhlzl1xp.selenium3","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/7mt9y40a.selenium4","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/cs2cluq5.selenium5_sin_proxy", "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/f7zfxja0.selenium_noproxy"
        ]

server = Server("/Users/antoniotorres/Projects/async_downloader/venv/lib/python3.9/site-packages/browsermobproxy/browsermob-proxy-2.1.4/bin/browsermob-proxy", options={'port': 9999})
#server.start()
proxy = server.create_proxy()
#proxy.start()
opts = FirefoxOptions()
opts.headless = False
prof_id = random.randint(0,5) 
prof_ff = FirefoxProfile(_FF_PROF[prof_id])
prof_ff.set_proxy(proxy.selenium_proxy())

try:
    driver = Firefox(options=opts,firefox_profile=prof_ff)
except Exception as e:
    print(e.__repr__())
    
    
    
    
