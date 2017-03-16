# -*- coding: utf-8 -*-

from scraper import settings, storage
from etl import leeloo

import os
import json
import functools

import boto3


__here__ = os.path.dirname(__file__)
OUTPUT_DIR = os.path.join(__here__, "..", "..", "output")


def get_bucket():
    bucket_name = settings.S3CACHE_BUCKET
    assert bucket_name, "No bucket configured"
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    return bucket


_parts = frozenset(['optevia_servicedeliverySet', 'reponse_body'])


def filter_keys(bucket):
    for key in bucket.objects.all():
        if "optevia_servicedeliverySet" in key.key:
            if "response_body" in key.key:
                yield key.key


def get_objects(bucket):
    for key in filter_keys(bucket):
        text = storage.get_s3_text(bucket, key)
        yield text


def etl_data(items):
    """Convert to leeloo json"""
    for text in items:
        data = json.loads(text)
        items = data['d']['results']
        for item in items:
            yield leeloo.service_delivery(item)


def write_data(data, output_filename):
    "Write data to output file"
    for item in data:
        with open(output_filename, "a") as f:
            writeline = functools.partial(print, file=f)
            writeline(json.dumps(item))


def main():
    bucket = get_bucket()
    items = etl_data(get_objects(bucket))
    output_filename = os.path.join(OUTPUT_DIR, "service_deliveries.jsonlines")
    write_data(items, output_filename)


if __name__ == '__main__':
    main()
