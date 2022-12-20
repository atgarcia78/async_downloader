
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException, TimeoutException
import copy
import logging
from backoff import constant, on_exception
import threading
import shutil
import tempfile

logger = logging.getLogger("mydriver")
retry_on_driver_except = on_exception(constant, WebDriverException, max_tries=3, raise_on_giveup=True, interval=2)


class MyDriver(Firefox):

    _LOCK = threading.Lock()
    _PROF = '/Users/antoniotorres/Library/Application Support/Firefox/Profiles/b33yk6rw.selenium'

    def __init__(self, **kwargs):
        self.noheadless = kwargs.get('noheadless', False)
        self.devtools = kwargs.get('devtools', False)
        self.host = kwargs.get('host', None)
        self.port = kwargs.get('port', None)
        self.driver = self.get_driver()


    @retry_on_driver_except
    def get_driver(self):

        tempdir = tempfile.mkdtemp(prefix='asyncall-')
        shutil.rmtree(tempdir, ignore_errors=True)
        res = shutil.copytree(MyDriver._PROF, tempdir, dirs_exist_ok=True)
        if res != tempdir:
            raise OSError("error when creating profile folder")

        opts = FirefoxOptions()

        if not self.noheadless:
            opts.add_argument("--headless")

        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-application-cache")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--profile")
        opts.add_argument(tempdir)

        if self.devtools:
            opts.add_argument("--devtools")
            opts.set_preference("devtools.toolbox.selectedTool", "netmonitor")
            opts.set_preference("devtools.netmonitor.persistlog", False)
            opts.set_preference("devtools.debugger.skip-pausing", True)

        if self.host and self.port:
            opts.set_preference("network.proxy.type", 1)
            opts.set_preference("network.proxy.http", self.host)
            opts.set_preference("network.proxy.http_port", int(self.port))
            opts.set_preference("network.proxy.https", self.host)
            opts.set_preference("network.proxy.https_port", int(self.port))
            opts.set_preference("network.proxy.ssl", self.host)
            opts.set_preference("network.proxy.ssl_port", int(self.port))
            opts.set_preference("network.proxy.ftp", self.host)
            opts.set_preference("network.proxy.ftp_port", int(self.port))
            opts.set_preference("network.proxy.socks", self.host)
            opts.set_preference("network.proxy.socks_port", int(self.port))

        else:
            opts.set_preference("network.proxy.type", 0)

        opts.set_preference("dom.webdriver.enabled", False)
        opts.set_preference("useAutomationExtension", False)

        opts.page_load_strategy = 'eager'

        serv = Service(log_path="/dev/null")

        
        def return_driver():
            _driver = None
            try:
                _driver = Firefox(service=serv, options=opts)
                _driver.maximize_window()
                self.wait_until(driver=_driver, timeout=0.5)
                _driver.set_script_timeout(20)
                _driver.set_page_load_timeout(25)
                return _driver
            except Exception as e:
                logger.exception(f'Firefox fails starting - {str(e)}')
                if _driver:
                    _driver.quit()
                    shutil.rmtree(tempdir, ignore_errors=True)
                raise WebDriverException(str(e))
                
        with MyDriver._LOCK:
            return(return_driver())

    @retry_on_driver_except
    def get_har(self, _method="GET", _mimetype=None):

        try:
        
            _res = try_get(self.driver.execute_async_script("HAR.triggerExport().then(arguments[0]);"), lambda x: x.get('entries') if x else None)

        except Exception as e:
            logger.exception(f"[get_har] {str(e)}")
            raise


        _res_filt = [el for el in _res if all([traverse_obj(el, ('request', 'method')) in _method, int(traverse_obj(el, ('response', 'bodySize'), default='0')) >= 0, not any([_ in traverse_obj(el, ('response', 'content', 'mimeType'), default='') for _ in ('image', 'css', 'font', 'octet-stream')])])]

        if _mimetype:
            if isinstance(_mimetype, str):
                _mimetype = [_mimetype]
            _res_filt = [el for el in _res_filt if any([_ in traverse_obj(el, ('response', 'content', 'mimeType'), default='') for _ in _mimetype])]

        return copy.deepcopy(_res_filt)

    def scan_har_for_request(self, _valid_url, _method="GET", _mimetype=None, _all=False, timeout=10, response=True, inclheaders=False, check_event=None):

        _har_old = []

        _list_hints_old = []
        _list_hints = []
        _first = True

        _started = time.monotonic()

        while True:

            _newhar = self.get_har(_method=_method, _mimetype=_mimetype)
            _har = _newhar[len(_har_old):]
            _har_old = _newhar
            for entry in _har:

                _url = traverse_obj(entry, ('request', 'url'))
                if not _url or not re.search(_valid_url, _url):
                    continue

                _hint = {}

                _hint.update({'url': _url})
                if inclheaders:
                    _req_headers = {val[0]: val[1] for header in traverse_obj(entry, ('request', 'headers')) if header['name'] != 'Host' and (val := list(header.values()))}
                    _hint = {'headers': _req_headers}

                if response:
                    _resp_status = traverse_obj(entry, ('response', 'status'))
                    _resp_content = traverse_obj(entry, ('response', 'content', 'text'))

                    _hint.update({'content': _resp_content, 'status': int_or_none(_resp_status)})

                if not _all:
                    return (_hint)
                else:
                    _list_hints.append(_hint)

                if check_event:
                    if isinstance(check_event, Callable):
                        check_event()
                    elif isinstance(check_event, Event):
                        if check_event.is_set():
                            raise StatusStop("stop event")

            if _all and not _first and (len(_list_hints) == len(_list_hints_old)):
                return (_list_hints)

            if (time.monotonic() - _started) >= timeout:
                if _all:
                    return (_list_hints)
                else:
                    return 
            else:
                if _all:
                    _list_hints_old = _list_hints
                    if _first:
                        _first = False
                        if not _list_hints:
                            time.sleep(0.5)
                        else:
                            time.sleep(0.01)
                    else:
                        time.sleep(0.01)
                else:
                    if _first:
                        _first = False
                        time.sleep(0.5)
                    else:
                        time.sleep(0.01)

    def scan_har_for_json(self, _valid_url, _method="GET", _all=False, timeout=10, inclheaders=False, check_event=None):

        _hints = self.scan_for_request(_valid_url,  _method=_method, _mimetype="json", _all=_all, timeout=timeout, inclheaders=inclheaders, check_event=None)

        def func_getter(x):
            _info_json = json.loads(re.sub('[\t\n]', '', html.unescape(x.get('content')))) if x.get('content') else ""
            if inclheaders:
                return (_info_json, x.get('headers'))
            else:
                return _info_json

        if not _all:
            return try_get(_hints, func_getter)

        else:
            if _hints:
                _list_info_json = []
                for el in _hints:
                    _info_json = try_get(el, func_getter)
                    if _info_json:
                        _list_info_json.append(_info_json)

                return _list_info_json

    def wait_until(self, driver=None, timeout=60, method=ec.title_is("DUMMYFORWAIT"), poll_freq=0.5):

        try:
            el = WebDriverWait(driver or self.driver, timeout, poll_frequency=poll_freq).until(method)
        except Exception as e:
            el = None

        return el

    def rm_driver(self):

        tempdir = self.driver.caps.get('moz:profile')
        try:
            self.driver.quit()
        except Exception:
            pass
        finally:
            if tempdir:
                shutil.rmtree(tempdir, ignore_errors=True)


    def get(self, *args, **kwargs):
        return self.driver.get(*args, **kwargs)

    def find_element(self, *args, **kwargs):
        return self.driver.find_element(*args, **kwargs)

    def find_elements(self, *args, **kwargs):
        return self.driver.find_elements(*args, **kwargs)

    @property
    def current_url(self):
        return self.driver.current_url
    