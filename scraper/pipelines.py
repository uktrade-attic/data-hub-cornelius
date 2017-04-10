# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class ScraperPipeline:
    """Example scraper pipeline class."""

    def process_item(self, item, spider):
        """Callback method to process an item."""
        return item
