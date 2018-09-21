# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class GatewaySpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    # url地址
    url = scrapy.Field()
    # 内容长度
    content_length = scrapy.Field()
    # mac
    mac = scrapy.Field()

    pass
