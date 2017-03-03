# -*- coding: utf-8 -*-

import os
import tempfile
import json
import functools

import boto3

import settings, storage


def get_objects(bucket):
    for key in bucket.objects.all():
        if key.key.strip("/").startswith(storage.cache_key_prefix):
            text = storage.get_s3_text(bucket, key.key)
            yield text

def get_bucket():
    bucket_name = settings.S3CACHE_BUCKET
    assert bucket_name, "No bucket configured"
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    return bucket

def local_cache(input_dir):
    for (dirpath, _, filenames) in os.walk(input_dir):
        for filename in filenames:
            if filename == "response_body":
                with open(os.path.join(dirpath, filename)) as f:
                    yield f.read()

def collect_data(items):
    for text in items:
        # print(repr(text))
        try:
            data = json.loads(text)
        except json.decoder.JSONDecodeError:
            print(repr(data))
            raise
        if "EntitySets" in data['d']:
            continue
        items = data['d']['results']
        for item in items:
            yield item

def write_data(data, f_name):
    with open(f_name, "w") as f:
        writeline = functools.partial(print, file=f)
        for item in collect_data(data):
            writeline(item)

def main():
    data = local_cache(settings.INPUT_DIR)
    write_data(data, settings.OUTPUT_FILE)
    # bucket = get_bucket()
    # for text in get_objects(bucket):

if __name__ == '__main__':
    main()
