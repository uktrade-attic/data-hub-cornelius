# -*- coding: utf-8 -*-
import json

import scrapy

from scraper import settings, auth



class OdataSpider(scrapy.Spider):
    name = "odata"
    allowed_domains = settings.ALLOWED_DOMAINS
    start_urls = settings.START_URLS

    def start_requests(self):
        login_url = '{}/?whr={}'.format(
            settings.CDMS_BASE_URL,
            settings.CDMS_ADFS_URL)
        session = auth.login(
            login_url,
            settings.CDMS_USERNAME,
            settings.CDMS_PASSWORD,
            user_agent=settings.USER_AGENT)
        for domain in session.cookies.list_domains():
            if not domain == ".cdms.ukti.gov.uk":
                session.cookies.clear(domain)
        cookies = session.cookies.get_dict()
        for url in settings.START_URLS:
            req = scrapy.Request(
                url,
                cookies=cookies,
                callback=self.parse_homepage)
            yield req

    def parse_homepage(self, response):
        if response.url.strip("/").endswith(".svc"):
            data = json.loads(response.body)
            yield data
            items = data['d']['EntitySets']
            for item in items:
                yield scrapy.Request(
                    response.urljoin(item),
                    callback=self.parse_itempage)

    def parse_itempage(self, response):
        data = json.loads(response.body)
        yield data
