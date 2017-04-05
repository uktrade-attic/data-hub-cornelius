# -*- coding: utf-8 -*-
import json
import logging
import urllib.parse

import scrapy
from redis import StrictRedis

from scraper import settings, auth

logger = logging.getLogger(__name__)


def _get_cookie_domain(url):
    netloc = urllib.parse.urlparse(url).netloc
    parts = netloc.split(".")
    parts[0] = ""
    parent_sub_domain = ".".join(parts)
    return parent_sub_domain


def get_cookies():
    login_url = "{}/?whr={}".format(
        settings.CDMS_BASE_URL,
        settings.CDMS_ADFS_URL)
    session = auth.login(
        login_url,
        settings.CDMS_USERNAME,
        settings.CDMS_PASSWORD,
        user_agent=settings.USER_AGENT)
    cookie_filter = _get_cookie_domain(settings.CDMS_BASE_URL)
    for domain in session.cookies.list_domains():
        if not domain == cookie_filter:
            session.cookies.clear(domain)
    cookies = session.cookies.get_dict()
    return cookies


class OdataSpider(scrapy.Spider):
    name = 'odata'
    allowed_domains = settings.ALLOWED_DOMAINS
    start_urls = settings.START_URLS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = None
        self.cookies = None

        if settings.REDIS_ENABLED:
            self.cache = StrictRedis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB)

    def start_requests(self):
        self._refresh_cookies()
        self._queue_previous_urls()

        for url in settings.START_URLS:
            logger.info('Queuing initial URL: %s', url)
            yield self._make_request(url, callback=self.parse_homepage)

    def parse_homepage(self, response):
        if response.url.strip("/").endswith(".svc"):
            try:
                data = json.loads(response.body.decode("utf-8"))
            except Exception:
                print(response.body)
                raise
            # yield data
            items = data['d']['EntitySets']
            for item in items:
                if item in settings.SCRAPE_ENTITIES:
                    url = response.urljoin(item)
                    logger.info('Queuing entity URL: %s', url)
                    yield self._make_request(url, callback=self.parse_itempage)
                else:
                    logger.info('Skipping entity: %s', item)

    def parse_itempage(self, response):
        logger.info('%d response received for URL: %s', response.status,
                    response.request.url)
        self._remove_url_from_cache(response.url)
        data = json.loads(response.body.decode("utf-8"))

        # yield data
        if '__next' in data['d']:
            url = data['d']['__next']
            self._add_url_to_cache(url)
            logger.info('Queuing next URL: %s', url)
            yield self._make_request(url, callback=self.parse_itempage)

    def _queue_previous_urls(self):
        for url in self._previous_urls():
            url = url.decode("utf-8")
            logger.info('Queuing URL from redis: %s', url)
            yield self._make_request(url, callback=self.parse_homepage)

    def _previous_urls(self):
        return self.cache.sscan_iter('urls') if self.cache else ()

    def _add_url_to_cache(self, url):
        if self.cache:
            self.cache.sadd('urls', url)

    def _remove_url_from_cache(self, url):
        if self.cache:
            self.cache.srem('urls', url)

    def _refresh_cookies(self):
        self.cookies = get_cookies()

    def _make_request(self, url, callback):
        return scrapy.Request(
            url, callback=callback, cookies=self.cookies,
            errback=self._handle_error,
            meta={
                'dont_redirect': True
            })

    def _retry(self, response):
        num_retries = response.request.meta.get('retry_times', 0) + 1
        if num_retries >= 5:
            logger.error('Max attempts exceeded for URL: %s',
                         response.request.url)
            return

        logger.info('Queuing retry for URL: %s', response.request.url)
        self.cookies = get_cookies()

        new_request = response.request.replace(cookies=self.cookies,
                                               dont_filter=True)
        new_request.meta['retry_times'] = num_retries
        return new_request

    def _handle_error(self, failure):
        response = getattr(failure.value, 'response')
        if response and response.status == 302:
            yield self._retry(response)
        else:
            logger.error('Scrapy error\nResponse\n%s:Traceback:\n%s',
                         response, failure.getTraceback())
