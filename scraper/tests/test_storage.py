import pickle
from unittest import mock

import freezegun

import scrapy

from scraper.storage import S3CacheStorage

key = "CACHE/req/2658b62c0bbafabe653244ce31a10d647fd45a5e/"
mock_data = {
    '{}pickled_meta'.format(key): pickle.dumps(
        {'response_url': "http://example.com/res", 'status': 200}),
    '{}response_body'.format(key): b'''{"name": "value"}''',
    '{}response_headers'.format(key): b'''X-Example: foo\nX-Other: bar'''
}

mock_settings = {
    'AWS_ACCESS_KEY_ID': "flibble",
    'AWS_SECRET_ACCESS_KEY': "fl00bl3",
    'AWS_REGION': "eu-north-5",
    'S3CACHE_BUCKET': "example.com_flibble"}


def mock_get_s3_text(bucket, fingerprint):
    return mock_data[fingerprint]


@mock.patch("scraper.storage.boto3")
@mock.patch("scraper.storage.get_s3_text", mock_get_s3_text)
def test_retrieve_response(mock_boto3):
    s3_cache_storage = S3CacheStorage(mock_settings)
    mock_bucket = mock_boto3.resource('s3').Bucket(mock_settings['S3CACHE_BUCKET'])
    assert s3_cache_storage.bucket == mock_bucket

    request = scrapy.http.Request("http://example.com/req")
    res = s3_cache_storage.retrieve_response(None, request)
    assert res.url == "http://example.com/res"
    assert res.status == 200
    assert res.headers == {b"X-Example": [b"foo"], b"X-Other": [b"bar"]}


@freezegun.freeze_time("2017-02-14 13:00")
@mock.patch("scraper.storage.boto3")
@mock.patch("scraper.storage.send_s3_text")
def test_store_response(mock_send_s3_text, mock_boto3):
    s3_cache_storage = S3CacheStorage(mock_settings)
    mock_bucket = mock_boto3.resource('s3').Bucket(mock_settings['S3CACHE_BUCKET'])
    assert s3_cache_storage.bucket == mock_bucket

    request = scrapy.http.Request(
        "http://example.com/flumble",
        headers={"X-Example": "foo"},
        body=b"Sample Request Body")
    response = scrapy.http.Response(
        "http://example.com/flumble",
        headers={"X-Other": "bar"},
        body=b"Sample Response Body")
    s3_cache_storage.store_response(None, request, response)

    fingerprint = "CACHE/flumble/d5b392310d7376bf4f739105b25ad1b7e5d52f19"

    expected_calls = [
        mock.call(
            mock_bucket,
            '{}/meta'.format(fingerprint),
            b" ".join([
                b"{'url': 'http://example.com/flumble', 'method': 'GET',",
                b"'status': 200, 'response_url': 'http://example.com/flumble',",
                b"'timestamp': 1487077200.0}"])),
        mock.call(
            mock_bucket,
            '{}/pickled_meta'.format(fingerprint),
            b"\x80\x02}q\x00(X\x03\x00\x00\x00urlq\x01X\x1a\x00\x00\x00http://example.com/flumbleq\x02X\x06\x00\x00\x00methodq\x03X\x03\x00\x00\x00GETq\x04X\x06\x00\x00\x00statusq\x05K\xc8X\x0c\x00\x00\x00response_urlq\x06X\x1a\x00\x00\x00http://example.com/flumbleq\x07X\t\x00\x00\x00timestampq\x08GA\xd6(\xbf\xd4\x00\x00\x00u."),  # noqa
        mock.call(
            mock_bucket,
            '{}/request_headers'.format(fingerprint),
            b'X-Example: foo'),
        mock.call(
            mock_bucket,
            '{}/request_body'.format(fingerprint),
            b"Sample Request Body"),
        mock.call(
            mock_bucket,
            '{}/response_headers'.format(fingerprint),
            b'X-Other: bar'),
        mock.call(
            mock_bucket,
            '{}/response_body'.format(fingerprint),
            b"Sample Response Body")]

    assert mock_send_s3_text.mock_calls == expected_calls
