from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
import shutil
import re
import time
from youtube_dl.utils import sanitize_filename
from selenium.common.exceptions import TimeoutException


url = 'https://gaybeeg.info/site/harlem-hookups/page/21/'

options = Options()
options.add_argument(f"user-agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 11.2; rv:85.0) Gecko/20100101 Firefox/85.0'")
options.add_argument("headless=True")
options.add_argument("user-data-dir=/Users/antoniotorres/Library/Application Support/Google/Chrome")
driver = webdriver.Chrome(executable_path=shutil.which('chromedriver'),options=options)
#driver.minimize_window()

driver.get(url)
wait = WebDriverWait(driver, 30)


try:    
    el_list = wait.until(ec.presence_of_all_elements_located((By.XPATH, "//a[@href]")))
except TimeoutException:
    el_list = None
    
    
print(el_list)    
if not el_list:
    webpage = driver.page_source
    regex_str = r'href=\"(https?://netdna-storage.com/[^\"]+)\"'
    el_list = re.findall(regex_str, webpage)
    entries = [{"_type": "url", "url" : el, "ie_key": "NetDNA"} for el in el_list]
else:
    entries = [{"_type": "url", "url" : el.get_attribute('href') , "ie_key": "NetDNA"} for el in el_list]
    
print(entries)

driver.close()
driver.quit()


