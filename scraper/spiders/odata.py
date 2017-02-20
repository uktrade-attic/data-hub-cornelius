# -*- coding: utf-8 -*-
import scrapy
from lxml import etree

from scraper import settings


def is_xml(response):
    if b"application/xml" in response.headers['Content-Type']:
        return True


class OdataSpider(scrapy.Spider):
    name = "odata"
    allowed_domains = settings.ALLOWED_DOMAINS
    start_urls = settings.START_URLS

    def parse(self, response):
        if is_xml(response):
            root = etree.fromstring(response.body)
            print(root)

            links = root.xpath('''//*[@href]''')
            print(links)

            for link in links:
                print(response.urljoin(link.attrib["href"]))
                yield scrapy.Request(response.urljoin(link.attrib["href"]))

        yield None

    #     for href in response.css("a::attr('href')").extract():
    #         if href.startswith("/report/"):
    #             url = response.urljoin(href)
    #             yield scrapy.Request(url, callback=self.parse_match)
    #     for href in response.css("option::attr('value')").extract():
    #         if href.startswith("/schedule/"):
    #             url = response.urljoin(href)
    #             yield scrapy.Request(url)

    # def parse_match(self, response):
    #     data = collect_tables(response)
    #     yield data
