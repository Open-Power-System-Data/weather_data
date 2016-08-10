__author__ = 'Jan'

import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth

_URL = 'https://urs.earthdata.nasa.gov/oauth/authorize/'
username = ''
pw = ''

auth = HTTPDigestAuth(username, pw)
r = requests.get(_URL, auth=auth)
print(r.status_code)
print(r.headers)