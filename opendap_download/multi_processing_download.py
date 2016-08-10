import multiprocessing
import requests
import logging

log = logging.getLogger('multiprocessing_download')
log.addHandler(logging.StreamHandler())
log.setLevel(logging.DEBUG)


def _download_and_save_file(url, file_path):
    r = requests.get(url, stream=True)
    with open(file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return r.status_code


def _mp_download_wrapper(url_item):
    query, _, file_path = url_item
    log.debug('Downloading:\t' + query)
    _download_and_save_file(query, file_path)


def init_download(url_items, nr_of_processes=4):
    p = multiprocessing.Pool(nr_of_processes)
    p.map(_mp_download_wrapper, url_items)


if __name__ == '__main__':
    raise NotImplementedError
