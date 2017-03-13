# -*- coding: utf-8 -*-
import json
import urllib

import scrapy

from scraper import settings, auth

from redis import StrictRedis


def get_cookies():
    login_url = "{}/?whr={}".format(
        settings.CDMS_BASE_URL,
        settings.CDMS_ADFS_URL)
    session = auth.login(
        login_url,
        settings.CDMS_USERNAME,
        settings.CDMS_PASSWORD,
        user_agent=settings.USER_AGENT)
    cookie_filter = ".{}".format(
        urllib.parse.urlparse(settings.CDMS_BASE_URL).netloc)
    for domain in session.cookies.list_domains():
        if not domain == cookie_filter:
            session.cookies.clear(domain)
    cookies = session.cookies.get_dict()
    return cookies


def retry(response):
    cookies = get_cookies()
    request = response.request.copy()
    request.cookies = cookies
    return request


class OdataSpider(scrapy.Spider):
    name = 'odata'
    allowed_domains = settings.ALLOWED_DOMAINS
    start_urls = settings.START_URLS

    def _previous_urls(self):
        return self.cache.sscan_iter('urls')

    def __init__(self, *args, **kwargs):
        super(OdataSpider, self).__init__(*args, **kwargs)
        self.cache = StrictRedis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB)

    def start_requests(self):
        cookies = get_cookies()
        for url in self._previous_urls():
            yield scrapy.Request(
                url.decode("utf-8"),
                cookies=cookies,
                callback=self.parse_homepage)
        for url in settings.START_URLS:
            yield scrapy.Request(
                url,
                cookies=cookies,
                callback=self.parse_homepage)

    def parse_homepage(self, response):
        if response.url.strip("/").endswith(".svc"):
            data = json.loads(response.body.decode("utf-8"))
            # yield data
            items = data['d']['EntitySets']
            for item in items:
                if item in settings.SCRAPE_ENTITIES:
                    yield scrapy.Request(
                        response.urljoin(item),
                        callback=self.parse_itempage)

    def parse_itempage(self, response):
        if response.status == 302:
            yield retry(response)
        elif response.status == 200:
            self.cache.srem('urls', response.url)
        data = json.loads(response.body.decode("utf-8"))
        # yield data
        if '__next' in data['d']:
            url = data['d']['__next']
            self.cache.sadd('urls', url)
            yield scrapy.Request(
                url,
                callback=self.parse_itempage)
