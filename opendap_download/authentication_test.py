__author__ = 'Jan'

import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from opendap_download.multi_processing_download import DownloadManager

_URL = 'https://urs.earthdata.nasa.gov/oauth/authorize/'


def _authenticate(username, pw):
    auth = HTTPDigestAuth(username, pw)
    r = requests.get(_URL, auth=auth)
    print(r.status_code)
    print(r.headers)


_authenticate(username='', pw='')
dl = DownloadManager('', '')
