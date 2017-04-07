# -*- coding: utf-8 -*-

from unittest.mock import patch

from scraper.spiders import odata

MOCK_SETTINGS = {
    "CDMS_BASE_URL": "http://flim.flam.example.com",
    "CDMS_ADFS_URL": "http://blom.blap.example.com",
    "CDMS_USERNAME": "mr_flibble",
    "CDMS_PASSWORD": "P455w0rd",
    "USER_AGENT": "firefox",
    "S3CACHE_BUCKET": "example.com_flibble"
}


class MockCookies:
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


class MockSession():
    cookies = MockCookies()


def mock_login(*args, **kwargs):
    return MockSession()


def test_get_cookie_domain():
    data = "http://flim.flam.example.com"
    expected = ".flam.example.com"
    result = odata._get_cookie_domain(data)
    assert result == expected, (result, expected)


@patch("scraper.spiders.odata.auth.login", mock_login)
def test_get_cookies():
    expected = [".flam.example.com"]
    result = odata._get_cookies(MOCK_SETTINGS)
    assert result == expected, (result, expected)
