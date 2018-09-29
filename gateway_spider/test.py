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

compressor = zlib.decompressobj(0)


class Test():
    # client.download('/user/hive/warehouse/polaris.db/t_polaris_term_list/pdate=20180814','E:/')

    # client = InsecureClient('http://172.19.10.33:50070', user='hadoop')

    yesterday = datetime.date.today()-datetime.timedelta(days=1)
    print yesterday.strftime('%Y%m%d')

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