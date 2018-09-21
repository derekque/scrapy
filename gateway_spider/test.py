import scrapy
from scrapy.http import Request
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from hdfs import InsecureClient

class Test():

    client = InsecureClient('http://172.19.10.33:9000', user='hadoop')

    with client.read('/user/hive/warehouse/polaris.db/t_polaris_urls_gmacs/pdate=20180920') as reader:
        features = reader.read()
        print features
