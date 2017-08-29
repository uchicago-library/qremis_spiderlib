import requests
from time import sleep
from logging import getLogger
from redlock import RedLockFactory, RedLockError
from .lib import response_200_json


log = getLogger(__name__)


def make_lock_factory(connection_details):
    return RedLockFactory(connection_details)


def iter_object_pages(qremis_api_url, cursor="0"):
    log.debug("Getting object pages...")
    obj_list_url = qremis_api_url + "/object_list"
    while cursor:
        log.debug("Next page: cursor={}".format(cursor))
        page_json = response_200_json(
            requests.get(obj_list_url, data={"cursor": cursor, "limit": 500})
        )
        cursor = page_json['pagination']['next_cursor']
        yield page_json


def iter_ids(page):
    log.debug("Yielding ids from a page...")
    for x in page['object_list']:
        yield x['id']


class QremisApiSpider:
    def __init__(self, qremis_api_url, filter_callback, work_callback, lock_factory):
        self.qremis_api_url = qremis_api_url
        self.filter_callback = filter_callback
        self.work_callback = work_callback
        self.lock_factory = lock_factory

    def crawl(self, delay=.1):
        log.debug("Beginning Crawl...")
        log.debug("Delay={}".format(str(delay)))
        while True:
            try:
                for page in iter_object_pages(self.qremis_api_url):
                    for id in iter_ids(page):
                        # Be nice to the web server/locking server
                        sleep(delay)
                        try:
                            with self.lock_factory.create_lock(id):
                                if self.filter_callback(id, self.qremis_api_url):
                                    log.debug("Filter callback returned True, processing. Id = {}".format(id))
                                    self.work_callback(id, self.qremis_api_url)
                                else:
                                    log.debug("Filter calback returned false, skipping. Id = {}".format(id))
                        except RedLockError:
                            log.debug("Couldn't obtain the lock. Id = {}".format(id))
                            # We couldn't acquire the lock,
                            # another worker is already processing
                            # this entry
                            pass
            except Exception as e:
                # Log the uncaught exception, sleep 5 seconds, go again
                log.critical(str(e))
                sleep(5)
