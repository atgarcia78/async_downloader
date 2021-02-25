from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
import shutil
import re
import time
from youtube_dl.utils import sanitize_filename
from selenium.common.exceptions import TimeoutException
import hashlib
import httpx
from queue import Queue
from concurrent.futures import (
    ThreadPoolExecutor,
    ALL_COMPLETED,
    wait
)


_DICT_BYTES = {'KB': 1024, 'MB': 1024*1024, 'GB' : 1024*1024*1024}

def get_data(q_in, q_out, i):

    opts = Options()
    opts.headless = True
    profile = "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/oo5x1t4e.selenium"
    driver = Firefox(options=opts, firefox_profile=profile)
    

    waitd = WebDriverWait(driver, 30)

    while not q_in.empty():

        url = q_in.get()
        print(f"{i}:{url}")
        
        driver.get(url)

        try:
            el1 = waitd.until(ec.presence_of_element_located((By.LINK_TEXT, "DOWNLOAD")))
                
        except Exception as e:
            pass
        
        el11 = driver.find_element_by_xpath("/html/body/section[1]/div[1]/header/h1")
        el12 = driver.find_element_by_xpath("/html/body/section/div[1]/header/p/strong")
        title = el11.text.split('.')[0].replace("-", "_")
        ext = el11.text.split('.')[1].lower()
        est_size = el12.text.split(' ')

        print(f"{i}:[redirect] {el1.get_attribute('href')}")
        driver.get(el1.get_attribute('href'))
        time.sleep(1)
        try:
            el2 = waitd.until(ec.presence_of_element_located((By.LINK_TEXT, "CONTINUE")))
        except Exception as e:
            pass

        print(f"{i}:[redirect] {el2.get_attribute('href')}")
        driver.get(el2.get_attribute('href'))
        time.sleep(5)
        try:
            el3 = waitd.until(ec.element_to_be_clickable((By.ID,"btn-main")))
        except Exception as e:
            pass
        el3.click()
        time.sleep(5)
        try:
            el4 = waitd.until(ec.element_to_be_clickable((By.ID, "btn-main")))
        except Exception as e:
            pass

        el4.click()
        time.sleep(1)
        try:
            el5 = waitd.until(ec.presence_of_element_located((By.LINK_TEXT, "DOWNLOAD")))
        except Exception as e:
            pass

        print(f"{i}:[redirect] {driver.current_url}")
        video_url = el5.get_attribute('href')

        str_id = f"{i}:{title}{est_size[0]}"
        videoid = int(hashlib.sha256(str_id.encode('utf-8')).hexdigest(),16) % 10**8

        print(title)
        print(videoid)

        try:
            
            res = httpx.head(video_url)
            if res.status_code >= 400:
                filesize = None
            else:
                filesize = int(res.headers.get('content-length', None))
        except Exception as e:
            filesize = None

        if not filesize:
            filesize = float(est_size[0])*_DICT_BYTES[est_size[1]]

        print(f"{i}:{filesize}")
        
        q_out.put({'videoid' : videoid, 'title' : title, 'url' : video_url, 'size': filesize})
        
    driver.close()
    driver.quit()


url = 'https://gaybeeg.info/site/harlem-hookups/page/21/'

opts = Options()
opts.headless = True
profile = "/Users/antoniotorres/Library/Application Support/Firefox/Profiles/oo5x1t4e.selenium"
driver = Firefox(options=opts, firefox_profile=profile)
#driver.minimize_window()
driver.get(url)
waitd = WebDriverWait(driver, 30)
try:    
    el_list = waitd.until(ec.presence_of_all_elements_located((By.XPATH, "//a[@href]")))
except TimeoutException:
    el_list = None
        
     
entries = [{'_type':'url', 'url': el.get_attribute('href'), 'ie_key' : "NetDNA"} for el in el_list if "dna-storage" in el.get_attribute('outerHTML')]
            
print(entries)

driver.close()
driver.quit()

queue_in = Queue() 

queue_out = Queue()

for entry in entries:
    queue_in.put(entry['url'])
    
workers = 5
    
with ThreadPoolExecutor(max_workers=workers) as exe:
    
    futures = [exe.submit(get_data, queue_in, queue_out, i) for i in range(workers)]

    done_futs, _ = wait(futures, return_when=ALL_COMPLETED)       

while not queue_out.empty():
    print(queue_out.get())



    




