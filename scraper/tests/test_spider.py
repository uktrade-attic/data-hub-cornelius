# -*- coding: utf-8 -*-

import unittest.mock

from scraper.spiders import odata


class MockSettings(object):
    CDMS_BASE_URL = "http://flim.flam.example.com"
    CDMS_ADFS_URL = "http://blom.blap.example.com"
    CDMS_USERNAME = "mr_flibble"
    CDMS_PASSWORD = "P455w0rd"
    USER_AGENT = "firefox"


class MockCookies(object):
    def __init__(self):
        self.domains = [
            ".flam.example.com",
            ".blap.example.com"]

    def list_domains(self):
        return self.domains

    def clear(self, domain):
        self.domains.remove(domain)

    def get_dict(self):
        return self.domains


class MockSession(object):
    cookies = MockCookies()


def mock_login(*args, **kwargs):
    return MockSession()


def test_get_cookie_domain():
    data = "http://flim.flam.example.com"
    expected = ".flam.example.com"
    result = odata._get_cookie_domain(data)
    assert result == expected, (result, expected)


@unittest.mock.patch("scraper.spiders.odata.settings", MockSettings())
@unittest.mock.patch("scraper.spiders.odata.auth.login", mock_login)
def test_get_cookies():
    expected = [".flam.example.com"]
    result = odata._get_cookies()
    assert result == expected, (result, expected)
