from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
import shutil
import re
import time
from youtube_dl.utils import sanitize_filename


url = 'https://netdna-storage.com/f/qmfoIieE/EV-Agustin-Darko.mp4.html'

options = Options()
options.add_argument(f"user-agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 11.2; rv:85.0) Gecko/20100101 Firefox/85.0'")
options.add_argument("headless=True")
options.add_argument("user-data-dir=/Users/antoniotorres/Library/Application Support/Google/Chrome")
driver = webdriver.Chrome(executable_path=shutil.which('chromedriver'),options=options)
#driver.minimize_window()

driver.get(url)

wait = WebDriverWait(driver, 120)
el1 = None

try:
    el1 = wait.until(ec.presence_of_element_located((By.LINK_TEXT, "DOWNLOAD")))
except Exception as e:
    pass
el11 = driver.find_element_by_xpath("/html/body/section[1]/div[1]/header/h1")
title = el11.text.split('.')[0].replace("-", "_")
driver.get(el1.get_attribute('href'))
time.sleep(1)
try:
    el2 = wait.until(ec.presence_of_element_located((By.LINK_TEXT, "CONTINUE")))
except Exception as e:
    pass

driver.get(el2.get_attribute('href'))
time.sleep(5)
try:
    el3 = wait.until(ec.element_to_be_clickable((By.ID,"btn-main")))
except Exception as e:
    pass
el3.click()
time.sleep(5)
try:
    el4 = wait.until(ec.element_to_be_clickable((By.ID, "btn-main")))
except Exception as e:
    pass

el4.click()
time.sleep(1)
try:
    el5 = wait.until(ec.presence_of_element_located((By.LINK_TEXT, "DOWNLOAD")))
except Exception as e:
    pass

video_url = el5.get_attribute('href')


format_video = {
    'format_id' : "http-mp4",
    'url' : video_url,
    'ext' : 'mp4'
}

print(format_video)

info_dict = {
    'id': "test",
    'title': sanitize_filename(title,restricted=True),
    'formats': [format_video],
    'ext': 'mp4'
}

