import functools
import json
import os

# import boto3

# import settings
# import storage

__here__ = os.path.dirname(__file__)
INPUT_DIR = os.path.join(__here__, "..", "cache", "CACHE", "XRMServices", "2011")
OUTPUT_DIR = os.path.join(__here__, "..", "output")

# def get_objects(bucket):
#     for key in bucket.objects.all():
#         if key.key.strip("/").startswith(storage.cache_key_prefix):
#             text = storage.get_s3_text(bucket, key.key)
#             yield text

# def get_bucket():
#     bucket_name = settings.S3CACHE_BUCKET
#     assert bucket_name, "No bucket configured"
#     s3 = boto3.resource('s3')
#     bucket = s3.Bucket(bucket_name)
#     return bucket


def local_cache(input_dir):
    """Walk input dir and yield contents."""
    for (dirpath, _, filenames) in os.walk(input_dir):
        for filename in filenames:
            if filename == "response_body":
                with open(os.path.join(dirpath, filename)) as f:
                    text = f.read()
                    if "<title>Object moved</title>" in text:
                        print(os.path.join(dirpath, filename))  # noqa: T003
                        continue
                    else:
                        yield text


def collect_data(items):
    """Convert to json."""
    for text in items:
        data = json.loads(text)
        if 'd' in data:
            continue
        if 'EntitySets' in data['d']:
            continue
        items = data['d']['results']
        for item in items:
            yield item


def write_data(data, output_dir):
    """Write data to output file."""
    for item in collect_data(data):
        key = item['__metadata']['type'].split(".")[-1] + ".jsonlines"
        filename = os.path.join(output_dir, key)
        with open(filename, "a") as f:
            writeline = functools.partial(print, file=f)  # noqa: T101
            writeline(json.dumps(item))


def main():
    """Do the stuff."""
    data = local_cache(INPUT_DIR)
    write_data(data, OUTPUT_DIR)


if __name__ == '__main__':
    main()
