# coding: utf-8
import re
import time
import httpx
from selenium.webdriver import Firefox, FirefoxProfile, FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By


_FF_PROF = [        
            "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/0khfuzdw.selenium0","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/xxy6gx94.selenium","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/wajv55x1.selenium2","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/yhlzl1xp.selenium3","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/7mt9y40a.selenium4","/Users/antoniotorres/Library/Application Support/Firefox/Profiles/cs2cluq5.selenium5_sin_proxy", "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/f7zfxja0.selenium_noproxy"
        ]
opts = FirefoxOptions()
opts.headless = False
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-application-cache")
opts.add_argument("--disable-gpu")
opts.add_argument("--disable-dev-shm-usage")
prof_ff = FirefoxProfile(_FF_PROF[0])

def wait_until(driver, time, method):        
        
    try:
        el = WebDriverWait(driver, time).until(method)
    except Exception as e:
        el = None

    return el   


driver = Firefox(firefox_binary="/Applications/Firefox Nightly.app/Contents/MacOS/firefox", options=opts, firefox_profile=prof_ff)
 

    
    
    
    
