# -*- coding: utf-8 -*-

import io
import pickle
from time import time

from w3lib.http import headers_raw_to_dict, headers_dict_to_raw

from scrapy.http import Headers
from scrapy.responsetypes import responsetypes
from scrapy.utils.request import request_fingerprint

import boto3
import botocore

def get_s3_text(bucket, fingerprint, f_type):
    body = io.BytesIO()
    key = "/".join([fingerprint, f_type])
    bucket.download_fileobj(key, body)
    body.seek(0)
    text = body.read()
    return text

def send_s3_text(bucket, fingerprint, f_type, body):
    body = io.BytesIO(body)
    key = "/".join([fingerprint, f_type])
    bucket.upload_fileobj(body, key)
    body.close()


class S3CacheStorage(object):
    def __init__(self, settings):
        self.aws_access_key = settings['AWS_ACCESS_KEY_ID']
        self.aws_secret_key = settings['AWS_SECRET_ACCESS_KEY']
        self.bucket_name = settings['S3CACHE_BUCKET']
        assert self.bucket_name, "No bucket configured"
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(self.bucket_name)

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

    def retrieve_response(self, spider, request):
        fingerprint = request_fingerprint(request)
        try:
            _metadata = get_s3_text(self.bucket, fingerprint, 'pickled_meta')
            body = get_s3_text(self.bucket, fingerprint, 'response_body')
            rawheaders = get_s3_text(self.bucket, fingerprint, 'response_headers')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return None
            else:
                raise

        metadata = pickle.loads(_metadata)

        url = metadata.get('response_url')
        status = metadata['status']
        headers = Headers(headers_raw_to_dict(rawheaders))
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response

    def store_response(self, spider, request, response):
        fingerprint = request_fingerprint(request)
        metadata = {
            'url': request.url,
            'method': request.method,
            'status': response.status,
            'response_url': response.url,
            'timestamp': time(),
        }

        pairs = (
            ('meta', repr(metadata).encode('utf8')),
            ('pickled_meta', pickle.dumps(metadata, protocol=2)),
            ('request_headers', headers_dict_to_raw(request.headers)),
            ('request_body', request.body),
            ('response_headers', headers_dict_to_raw(response.headers)),
            ('response_body', response.body),
        )
        for key, body in pairs:
            send_s3_text(self.bucket, fingerprint, key, body)
