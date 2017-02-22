# -*- coding: utf-8 -*-
import json

import scrapy
from pyquery import PyQuery

from scraper import settings


def submit_form(source, next_func, url=None, params=None):
    """
    It submits the form contained in the `source` param optionally
    overriding form `params` and form `url`.

    This is needed as UKTI has a few STSes and the token has to be
    validated by all of them.  For more details, check:
    https://msdn.microsoft.com/en-us/library/aa480563.aspx
    """
    html_parser = PyQuery(source)
    form_action = html_parser('form').attr('action')

    # get all inputs in the source + optional params passed in
    data = {
        field.get('name'): field.get('value')
        for field in html_parser('input')
    }
    if params:
        data.update(params)

    url = url or form_action
    return scrapy.FormRequest(
        url,
        method="POST",
        formdata=data,
        callback=next_func)


def handle_form(form_action, next_func):
    def _inner(resp):
        assert resp.status == 200

        html_parser = PyQuery(resp.body)
        assert form_action != html_parser('form').attr('action')

        return next_func(resp)
    return _inner


class OdataSpider(scrapy.Spider):
    name = "odata"
    allowed_domains = settings.ALLOWED_DOMAINS
    start_urls = settings.START_URLS

    def start_requests(self):
        login_url = '{}/?whr={}'.format(
            settings.CDMS_BASE_URL,
            settings.CDMS_ADFS_URL)

        yield scrapy.Request(login_url, callback=self.login_2)

    def login_2(self, resp):
        assert resp.status == 200
        html_parser = PyQuery(resp.text)
        username_field_name = html_parser('input[name*="Username"]').attr('name')
        password_field_name = html_parser('input[name*="Password"]').attr('name')

        # 2. submit the login form with username and password
        return submit_form(
            resp.body,
            next_func=self.login_3,
            url=resp.url,
            params={
                username_field_name: settings.CDMS_USERNAME,
                password_field_name: settings.CDMS_PASSWORD})

    def login_3(self, resp):
        # 3. and 4. re-submit the resulting form containing the security token
        # so that the next STS can validate it
        return submit_form(resp.body, self.login_4)

    def login_4(self, resp):
        # 5. re-submit the form again to validate the token and get as result
        # the authenticated cookie
        return submit_form(resp.body, self.start_crawl)

    def start_crawl(self, resp):
        for url in settings.START_URLS:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        if response.url.strip("/").endswith(".svc"):
            data = json.loads(response.body)
            print(data)
            print("Flibble")
            yield data
            # items = data['value']
            # for item in items:
            #     yield scrapy.Request(response.urljoin(item['url']))
