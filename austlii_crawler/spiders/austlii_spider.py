import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.item import Item, Field
from markdownify import markdownify
import weaviate
import tiktoken

OPENAI_API_KEY = ""
OPENAI_MODEL = ""
WEAVIATE_INSTANCE_URL = (
    ""
)
WEAVIATE_API_KEY = ""
WEAVIATE_MAX_TOKEN_LIMIT = 8000


class WeaviateService:
    def __init__(self):
        self.api_key = WEAVIATE_API_KEY
        self.instance_url = WEAVIATE_INSTANCE_URL
        self.openai_api_key = OPENAI_API_KEY
        self.weaviate_client = self.initialize_weaviate_client()

    def initialize_weaviate_client(self):
        weaviate_client = weaviate.Client(
            url=self.instance_url,
            auth_client_secret=weaviate.AuthApiKey(api_key=self.api_key),
            additional_headers={"X-OpenAI-Api-Key": self.openai_api_key},
        )
        return weaviate_client

    def set_data(self, data_object):
        uuid = self.weaviate_client.data_object.create(
            class_name="legal", data_object=data_object
        )
        return uuid

    def is_new_data(self, article):
        response = (
            self.weaviate_client.query.get("legal", ["title", "url"])
            .with_where(
                {
                    "operator": "Or",
                    "operands": [
                        {
                            "path": "title",
                            "operator": "Equal",
                            "valueText": article["title"],
                        },
                        {
                            "path": "url",
                            "operator": "Equal",
                            "valueText": article["url"],
                        },
                    ],
                }
            )
            .with_limit(1)
            .do()
        )

        data = (
            list(response["data"]["Get"]["Legal"])
            if response["data"]["Get"]["Legal"]
            else []
        )

        return True if len(data) == 0 else False


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

    encoding = tiktoken.encoding_for_model(OPENAI_MODEL)
    weaviate_service = WeaviateService()

    def parse(self, response):
        for link in self.link_extractor.extract_links(response):
            yield scrapy.Request(link.url, callback=self.parse_item)

    def parse_item(self, response):
        if response.css("article"):
            article = ArticleItem()
            article["url"] = response.url
            article["title"] = response.css("title::text").get()
            article["content"] = markdownify(response.css("article").get())
            if self.weaviate_service.is_new_data(article):
                print("---------------------------------------------------------------")
                try:
                    print(f"Title: {article["title"].strip()}")
                    print(f"URL: {article["url"]}")
                    upload_data = {
                        "url": article["url"],
                        "title": article["title"],
                        "content": article["content"],
                    }
                    chunks_count = self.upload_to_weaviate(upload_data)
                    print(f"Uploaded Success: {chunks_count} Chunks")
                except Exception as error:
                    print(f"Upload Failed: {error}")
                print("---------------------------------------------------------------")

                yield article

        for link in self.link_extractor.extract_links(response):
            yield scrapy.Request(link.url, callback=self.parse_item)

    def split_text_into_chunks(self, text, limit):
        tokens = self.encoding.encode(text)
        chunks = [tokens[i : i + limit] for i in range(0, len(tokens), limit)]
        return [self.encoding.decode(chunk) for chunk in chunks]

    def upload_to_weaviate(self, data, limit=0):
        max_token_limit = limit if limit > 0 else WEAVIATE_MAX_TOKEN_LIMIT
        content = data.get("content", "")
        chunks = self.split_text_into_chunks(content, max_token_limit)
        for index, chunk in enumerate(chunks):
            try:
                chunk_data = data.copy()
                chunk_data["content"] = chunk
                uuid = self.weaviate_service.set_data(chunk_data)
            except Exception as error:
                if "maximum context length is 8192 tokens" in str(error):
                    self.upload_to_weaviate(chunk_data, 5000)
                else:
                    raise Exception(error)
            print(f"[{index+1}] Chunk Upload UUID: {uuid}")
        return len(chunks)
