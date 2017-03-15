# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

from scraper import settings

INDEX = settings.ES_INDEX


def get_elasticsearch_client():
    """Return an instance of the elasticsearch client or similar."""
    return Elasticsearch([{
        'host': settings.ES_HOST,
        'port': settings.ES_PORT
    }])


def create_es_index(es_client):
    client = IndicesClient(es_client)
    if not client.exists(index=INDEX):
        client.create(index=INDEX)


class ESPipeline(object):

    def open_spider(self, spider):
        self.client = get_elasticsearch_client()
        create_es_index(self.client)

    def process_item(self, item, spider):
        if self.exists(item):
            self.update(item)
        else:
            self.create(item)
        return item

    def create(self, item):
        item_id = item.pop('id')
        self.client.create(
            index=INDEX,
            doc_type=item['type'],
            id=item_id,
            body=item
        )

    def update(self, item):
        item_id = item.pop('id')
        self.client.update(
            index=INDEX,
            doc_type=item['type'],
            id=item_id,
            body=item
        )

    def exists(self, item):
        return self.client.exists(
            index=INDEX,
            doc_type=item['type'],
            id=item['id'],
            realtime=True
        )