import multiprocessing
import requests
import logging
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

log = logging.getLogger('multiprocessing_download')
log.addHandler(logging.StreamHandler())
log.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)


class DownloadManager(object):
    _AUTHENTICATION_URL = 'https://urs.earthdata.nasa.gov/oauth/authorize'

    def __init__(self, username, pw):
        if username == "" or pw == "":
            raise ValueError('Please provide a correct username and/or password')
        self._username = username
        self._password = pw
        self._authenticated_session = self._authenticate(self._username, self._password)

    def _authenticate(self, username, pw):
        s = requests.Session()
        s.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.85 Safari/537.36'}
        s.auth = (self._username, self._password)
        s.cookies=self._authorize_with_urllib()

        r = s.get('http://goldsmr4.sci.gsfc.nasa.gov:80/opendap/MERRA2/M2T1NXSLV.5.12.4/')
        log.debug('Authentication Status')
        log.debug(r.status_code)
        log.debug(r.headers)
        log.debug(list(r.cookies))

        log.debug('Sessions Data')
        log.debug(s.cookies)
        log.debug(s.headers)
        return s

    def _download_and_save_file(self, url, file_path):
        r = self._authenticated_session.get(url, stream=True, data={'url': self._AUTHENTICATION_URL})
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        return r.status_code

    def _mp_download_wrapper(self, url_item):
        query, _, file_path = url_item
        log.debug('Downloading:\t' + query)
        self._download_and_save_file(query, file_path)

    def init_download(self, url_items, nr_of_processes=4):
        p = multiprocessing.Pool(nr_of_processes)
        p.map(self._mp_download_wrapper, url_items)

    def _authorize_with_urllib(self):
        import urllib.response
        from http import cookiejar

        username = self._username
        password = self._password
        top_level_url = "https://urs.earthdata.nasa.gov"

        # create an authorization handler
        p = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        p.add_password(None, top_level_url, username, password);

        auth_handler = urllib.request.HTTPBasicAuthHandler(p)
        auth_cookie_jar = cookiejar.CookieJar()
        cookie_jar = urllib.request.HTTPCookieProcessor(auth_cookie_jar)
        opener = urllib.request.build_opener(auth_handler, cookie_jar)

        urllib.request.install_opener(opener)

        try:
            result = opener.open('http://goldsmr4.sci.gsfc.nasa.gov:80/opendap/MERRA2/M2T1NXSLV.5.12.4/')
            log.debug(list(auth_cookie_jar)[0])
            log.debug(list(auth_cookie_jar)[1])
        except IOError as e:
            log.warning(e)
            raise IOError

        return auth_cookie_jar

# if __name__ == '__main__':
#     DownloadManager('', '')
#

