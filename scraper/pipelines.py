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
        doc_type = item['__metadata']['type'].split('.')[-1]
        item_id = item.pop(doc_type+'Id')
        if self.exists(doc_type, item_id):
            self.update(item, doc_type, item_id)
        else:
            self.create(item, doc_type, item_id)
        return item

    def create(self, item, doc_type, item_id):
        self.client.create(
            index=INDEX,
            doc_type=doc_type,
            id=item_id,
            body=item
        )

    def update(self, item, doc_type, item_id):
        self.client.update(
            index=INDEX,
            doc_type=doc_type,
            id=item_id,
            body=item
        )

    def exists(self, doc_type, item_id):
        return self.client.exists(
            index=INDEX,
            doc_type=doc_type,
            id=item_id,
            realtime=True
        )
