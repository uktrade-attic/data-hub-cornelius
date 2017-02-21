# -*- coding: utf-8 -*-
import json

import scrapy

from scraper import settings


class OdataSpider(scrapy.Spider):
    name = "odata"
    allowed_domains = settings.ALLOWED_DOMAINS
    start_urls = settings.START_URLS

    def parse(self, response):
        if response.url.strip("/").endswith(".svc"):
            data = json.loads(response.body)
            items = data['value']
            for item in items:
                yield scrapy.Request(response.urljoin(item['url']))
