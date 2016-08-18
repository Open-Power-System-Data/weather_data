__author__ = "Jan Urbansky"

# TODO: Change and describe structure of the links that have to be provided.
# TODO: Test multithreading vs multprocessing
# TODO: Proper readme with examples.

import multiprocessing
import requests
import logging
import yaml
import os
import urllib.response
from http import cookiejar
import urllib.error

log = logging.getLogger('opendap_download')

class DownloadManager(object):
    __AUTHENTICATION_URL = 'https://urs.earthdata.nasa.gov/oauth/authorize'
    __username = ''
    __password = ''
    __links = []
    __download_path = ''
    _authenticated_session = None

    def __init__(self, username='', password='', links=None, download_path=''):
        self.set_username_and_password(username, password)
        self.links = links
        self.download_path = download_path
        log.debug('Init DownloadManager')

    @property
    def links(self):
        return self.__links

    @links.setter
    def links(self, links):
        # TODO: Check if links have the right structure? Read filename from links?
        self.__links = links

    @property
    def download_path(self, file_path):
        return self.__download_path

    @download_path.setter
    def download_path(self, file_path):
        raise NotImplementedError
        self.__download_path = file_path

    def set_username_and_password(self, username, password):
        self.__username = username
        self.__password = password

    def read_credentials_from_yaml(self, file_path_to_yaml):
        with open(file_path_to_yaml, 'r') as f:
            credentials = yaml.load(f)
            log.debug('Credentials: ' + str(credentials))
            self.set_username_and_password(credentials['username'], credentials['password'])


    def _mp_download_wrapper(self, url_item):
        """If this is __ the """
        query, _, file_path = url_item
        log.debug('Downloading:\t' + query)
        self.__download_and_save_file(query, file_path)

    def start_download(self, nr_of_processes=4):
        self._authenticated_session = self.__authenticate()
        p = multiprocessing.Pool(nr_of_processes)
        p.map(self._mp_download_wrapper, self.links)

    def __download_and_save_file(self, url, file_path):
        r = self._authenticated_session.get(url, stream=True)
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        return r.status_code

    def __authenticate(self):
        s = requests.Session()
        s.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.85 Safari/537.36'}
        s.auth = (self.__username, self.__password)
        s.cookies = self.__authorize_with_urllib()

        r = s.get('http://goldsmr4.sci.gsfc.nasa.gov:80/opendap/MERRA2/M2T1NXSLV.5.12.4/')
        log.debug('Authentication Status')
        log.debug(r.status_code)
        log.debug(r.headers)
        log.debug(r.cookies)

        log.debug('Sessions Data')
        log.debug(s.cookies)
        log.debug(s.headers)
        return s

    def __authorize_with_urllib(self):
        username = self.__username
        password = self.__password
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
            # Request the website and initialiaze the BasicAuth. This will populate the auth_cookie_jar
            result = opener.open('http://goldsmr4.sci.gsfc.nasa.gov:80/opendap/MERRA2/M2T1NXSLV.5.12.4/')
            log.debug(list(auth_cookie_jar)[0])
            log.debug(list(auth_cookie_jar)[1])
        except urllib.error.HTTPError:
            raise ValueError('Username and or Password are not correct!')
        except IOError as e:
            log.warning(e)
            raise IOError

        return auth_cookie_jar



if __name__ == '__main__':
    log.addHandler(logging.StreamHandler())
    logging.basicConfig(level=logging.DEBUG)
    dl = DownloadManager()
    dl.read_credentials_from_yaml((os.path.join(os.path.dirname(os.path.realpath(__file__)), 'authentication.yaml')))
    dl.links = [1, 2, 3, 4]
    dl.start_download()
