import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.item import Item, Field
from markdownify import markdownify


class ArticleItem(Item):
    url = Field()
    title = Field()
    content = Field()


class AustliiSpider(CrawlSpider):
    name = "austlii"
    allowed_domains = ["austlii.edu.au"]
    start_urls = ["https://www.austlii.edu.au/databases.html"]
    link_extractor = LinkExtractor(allow=())
    rules = (Rule(link_extractor, callback="parse_item", follow=True),)

    def parse(self, response):
        for link in self.link_extractor.extract_links(response):
            yield scrapy.Request(link.url, callback=self.parse_item)

    def parse_item(self, response):
        if response.css("article"):
            article = ArticleItem()
            article["url"] = response.url
            article["title"] = response.css("title::text").get()
            article["content"] = markdownify(response.css("article").get())
            yield article

        for link in self.link_extractor.extract_links(response):
            yield scrapy.Request(link.url, callback=self.parse_item)
