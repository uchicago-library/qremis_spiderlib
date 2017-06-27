import requests
from time import sleep
from logging import getLogger
from redlock import RedLockFactory, RedLockError
from .lib import response_200_json


log = getLogger(__name__)


def make_lock_factory(connection_details):
    return RedLockFactory(connection_details)


def iter_object_pages(qremis_api_url, cursor="0"):
    obj_list_url = qremis_api_url + "/object_list"
    while cursor:
        page_json = response_200_json(
            requests.get(obj_list_url, data={"cursor": cursor, "limit": 500})
        )
        cursor = page_json['pagination']['next_cursor']
        yield page_json


def iter_ids(page):
    for x in page['object_list']:
        yield x['id']


class QremisApiSpider:
    def __init__(self, qremis_api_url, filter_callback, work_callback, lock_factory):
        self.qremis_api_url = qremis_api_url
        self.filter_callback = filter_callback
        self.work_callback = work_callback
        self.lock_factory = lock_factory

    def crawl(self, delay=.1):
        while True:
            try:
                for page in iter_object_pages(self.qremis_api_url):
                    for id in iter_ids(page):
                        # Be nice to the web server/locking server
                        sleep(delay)
                        try:
                            with self.lock_factory.create_lock(id):
                                if self.filter_callback(id, self.qremis_api_url):
                                    self.work_callback(id, self.qremis_api_url)
                        except RedLockError:
                            # We couldn't acquire the lock,
                            # another worker is already processing
                            # this entry
                            pass
            except Exception as e:
                # Log the uncaught exception, sleep 5 seconds, go again
                log.critical(str(e))
                sleep(5)
