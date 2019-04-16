import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from hdfs import InsecureClient
import gzip
from cStringIO import StringIO
from io import BytesIO
import binascii
import zlib
import datetime

#
d1 = datetime.datetime(2018,10,22)
d2 = datetime.datetime(2018,9,22)
d3 = datetime.datetime.today().strftime('%Y%m%d')
d4 = datetime.datetime.today() - datetime.timedelta(days=1)
print len(unicode('新闻','utf-8'))
# print d1 > d4

# print int((d1-d2).days)
#
# for index in range(1,(d1-d2).days):
#     d2 = d2 + datetime.timedelta(days=1)
#     print d2.strftime('%Y%m%d')

# string = '20181022'
# print string[0:4]






# print 1
#
# class Test():
#
#
#     def __init__(self):
#         print 3
#         self.deadloop()
#
#     def deadloop(self):
#         print '123'
#         while True:
#             pass
#
# t = Test()
# t.deadloop()

    # client.download('/user/hive/warehouse/polaris.db/t_polaris_term_list/pdate=20180814','E:/')

    # client = InsecureClient('http://172.19.10.33:50070', user='hadoop')

    # yesterday = datetime.date.today()-datetime.timedelta(days=1)
    # print yesterday.strftime('%Y%m%d')

    # with client.read('/user/hive/warehouse/polaris.db/t_polaris_urls_gmacs/pdate=20180918/part-r-00000.gz',chunk_size=1024) as reader:
    # with client.read('/user/hive/warehouse/polaris.db/t_polaris_urls_gmacs/pdate=20180918/part-r-00000.gz',encoding='utf-8',delimiter='\n') as reader:
        # for line in reader:
        #     # print binascii.hexlify(line)
        #     decompressed = compressor.decompress(line)
        #     print decompressed
        #     buf = StringIO(line)
        #     f = gzip.GzipFile(mode='rb',fileobj=buf)
        #     try:
        #         data = f.read()
        #         print data
        #     finally:
        #         f.close()

    # print client.content('/user/hive/warehouse/polaris.db/t_polaris_urls_gmacs/pdate=20180918/')
    # print client.list('/user/hive/warehouse/polaris.db/t_polaris_urls_gmacs/pdate=20180918/')
    # print client.status('/user/hive/warehouse/polaris.db/t_polaris_urls_gmacs/pdate=20180918/part-r-00000.gz')
'''
    with client.read('/user/hive/warehouse/polaris.db/t_polaris_urls_gmacs/pdate=20180918/part-r-00000.gz') as reader:
        # print reader.read()
        buf = StringIO(reader.read())
        with gzip.GzipFile(mode='rb',fileobj=buf) as gzf:
            for line in gzf:
                print line
'''