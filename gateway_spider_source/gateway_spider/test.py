import scrapy
from scrapy.http import Request
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import re
import tarfile
import gzip
import os


path = 'E:/tarfiles'
daytarfiles = os.listdir(path)
for daytarfile in daytarfiles:
    if (not os.path.isdir(daytarfile)) and (daytarfile.endswith('tar.gz')):
        tar = tarfile.open(path +'/' +daytarfile)
        gznames = tar.getnames()
        for gzname in gznames:
            gzpath = path+'/'+daytarfile.split('.')[0]
            tar.extract(gzname,gzpath)
            gzfiles = os.listdir(gzpath)
            for gzfile in gzfiles:
                if (not os.path.isdir(gzfile)) and gzfile.endswith('gz'):
                    os.chdir(gzpath)
                    gzfile = gzip.GzipFile(gzfile)
                    print gzfile
        tar.close()
