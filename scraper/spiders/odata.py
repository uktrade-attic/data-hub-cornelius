# -*- coding: utf-8 -*-
import json
import logging
import urllib.parse

import scrapy
from redis import StrictRedis

from scraper import auth

logger = logging.getLogger(__name__)


class OdataSpider(scrapy.Spider):
    """A Scrapy spider for OData v1 services."""

    name = 'odata'

    def __init__(self, *args, **kwargs):
        """Initialises the spider."""
        super().__init__(*args, **kwargs)

        self.allowed_domains = None
        self._cache = None
        self._cookies = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Creates the spider.

        This is overridden to get access to the settings in order to perform
        initialisation.

        Refer to https://doc.scrapy.org/en/latest/topics/settings.html#how-to-access-settings
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.setup()
        return spider

    def setup(self):
        """Performs additional initialisation once settings are available."""
        self.allowed_domains = self.settings['ALLOWED_DOMAINS']
        self.start_urls = self.settings['START_URLS']

        if self.settings['REDIS_ENABLED']:
            self._cache = StrictRedis(
                host=self.settings['REDIS_HOST'],
                port=self.settings['REDIS_PORT'],
                db=self.settings['REDIS_DB'])

    def start_requests(self):
        """Queues the initial request(s) in the scraping job.

        Typically this queues a request for the service root URL that
        returns a list of entity types.
        """
        self._refresh_cookies()
        self._queue_previous_urls()

        for url in self.settings['START_URLS']:
            logger.info('Queuing initial URL: %s', url)
            yield self._make_request(url, callback=self.parse_homepage)

    def parse_homepage(self, response):
        """Processes a response for a service root URL.

        This includes parsing the response, and queuing requests
        for each entity collection specified in the response.
        """
        if response.url.strip("/").endswith(".svc"):
            try:
                data = json.loads(response.body.decode("utf-8"))
            except Exception:
                logger.error(response.body)
                raise
            # yield data
            items = data['d']['EntitySets']
            for item in items:
                url = response.urljoin(item)
                logger.info('Queuing entity URL: %s', url)
                yield self._make_request(url, callback=self.parse_itempage)

    def parse_itempage(self, response):
        """Processes a response for an entity collection endpoint.

        This includes parsing the response, and queuing a request for the
        next page (if there is one).
        """
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
        """Queues incomplete URLs from previous runs.

        (Does nothing if the cache server is disabled.)
        """
        for url in self._previous_urls():
            url = url.decode("utf-8")
            logger.info('Queuing URL from redis: %s', url)
            yield self._make_request(url, callback=self.parse_itempage)

    def _previous_urls(self):
        """Returns incomplete URLs from the cache server (if enabled)."""
        return self._cache.sscan_iter('urls') if self._cache else ()

    def _add_url_to_cache(self, url):
        """Stores a URL in the cache server (if enabled)."""
        if self._cache:
            self._cache.sadd('urls', url)

    def _remove_url_from_cache(self, url):
        """Removes a URL from the cache server (if enabled)."""
        if self._cache:
            self._cache.srem('urls', url)

    def _refresh_cookies(self):
        """Creates a new session with the OData service."""
        self._cookies = _get_cookies(self.settings)

    def _make_request(self, url, callback):
        """Creates a Scrapy request object.

        The request object is created using the cookies for the current
        session, with redirects disabled and an error callback specified.

        Note that this does not actually queue the request.
        """
        return scrapy.Request(
            url, callback=callback, cookies=self._cookies,
            errback=self._handle_error,
            meta={
                'dont_redirect': True
            })

    def _retry(self, response):
        """Retries a failed request (up to a configured number of attempts)."""
        num_retries = response.request.meta.get('retry_times', 0) + 1
        if num_retries >= 5:
            logger.error('Max attempts exceeded for URL: %s',
                         response.request.url)
            return

        logger.info('Queuing retry for URL: %s', response.request.url)
        self._refresh_cookies()

        new_request = response.request.replace(cookies=self._cookies,
                                               dont_filter=True)
        new_request.meta['retry_times'] = num_retries
        return new_request

    def _handle_error(self, failure):
        """Handles Scrapy request errors.

        This function is passed as the error callback when making Scrapy
        requests.
        """
        response = getattr(failure.value, 'response')
        if response and response.status == 302:
            # This is typically due to session expiry.
            yield self._retry(response)
        else:
            # Log the response status code, URL and traceback, and then
            # carries on.
            # Often these are 403s.
            logger.error('Scrapy error\nResponse\n%s:Traceback:\n%s',
                         response, failure.getTraceback())


def _get_cookie_domain(url):
    netloc = urllib.parse.urlparse(url).netloc
    parts = netloc.split(".")
    parts[0] = ""
    parent_sub_domain = ".".join(parts)
    return parent_sub_domain


def _get_cookies(settings):
    login_url = "{}/?whr={}".format(
        settings['CDMS_BASE_URL'],
        settings['CDMS_ADFS_URL'])
    session = auth.login(
        login_url,
        settings['CDMS_USERNAME'],
        settings['CDMS_PASSWORD'],
        user_agent=settings['USER_AGENT'])
    cookie_filter = _get_cookie_domain(settings['CDMS_BASE_URL'])
    for domain in session.cookies.list_domains():
        if not domain == cookie_filter:
            session.cookies.clear(domain)
    cookies = session.cookies.get_dict()
    return cookies
