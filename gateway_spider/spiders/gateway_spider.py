import textutil
import scrapy
from scrapy.http import Request
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import tarfile
import os
import gzip
import logging
import logging.handlers
from scrapy.exceptions import DontCloseSpider
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import time
import multiprocessing
import traceback
from bs4 import BeautifulSoup
import lxml
from hdfs import InsecureClient

e = multiprocessing.Event()

class Myspider (scrapy.Spider):
    name = 'gateway_spider'

    # file path and name
    tarfilepath = 'E:/urltarfiles'
    # tarfilepath = '/home/app/urltarfiles'
    tempfilepath = 'E:/temp/20180816_url_gmaclist'
    # tempfilepath = '/opt/temp/20180816_url_gmaclist'
    staticfilepath = 'E:/result'
    # staticfilepath = '/opt/result'
    logfilename = 'url_parse_result.log'
    urlfilename = 'invalid_result.log'

    # temp variables
    gzfilename = None
    macList = None
    dealingurlLines = None
    dealinggzfilename = None
    gzfileindex = 0
    gzfiles = None
    gzpath = None

    # statistic
    startTime = time.asctime( time.localtime(time.time()) )
    startTimeUnix = time.time()
    endTime = None
    scrawlegzfiles = 0
    scrawledpage = 0
    validpage = 0
    nothtmlpage = 0
    htmlbutnottextpage = 0
    scrawlSpeed = 0
    scrawresponse = 0

    # logging
    contentlogger = logging.getLogger('contentlogger')
    invalidurllogger = logging.getLogger('invalidurllogger')

    # hdfs client initial
    client = InsecureClient('http://172.19.10.33:9000', user='cmiot01')

    def __init__(self):
        super(Myspider, self).__init__()
        if not os.path.exists(self.staticfilepath):
            os.mkdir(self.staticfilepath)

        self.contentlogger.setLevel(logging.INFO)
        self.invalidurllogger.setLevel(logging.INFO)
        urlfilehandler = logging.FileHandler(self.staticfilepath+'/'+self.urlfilename)
        urlfilehandler.setLevel(logging.INFO)
        self.invalidurllogger.addHandler(urlfilehandler)
        # 800MB per logfile
        rotatehandler = logging.handlers.RotatingFileHandler(self.staticfilepath+'/'+self.logfilename,maxBytes=838860800,backupCount=1000)
        self.contentlogger.addHandler(rotatehandler)

        # dispatcher.connect(self.spider_idle, signals.spider_idle)

        # 1.open directory list tarfiles
        # daytarfiles = os.listdir(self.tarfilepath)
        # for daytarfile in daytarfiles:
        #     if (os.path.isdir(daytarfile)) and (not daytarfile.endswith('tar.gz')):
        #         continue
        #     else:
        #         # 2.resolve tarfile
        #         tar = tarfile.open(self.tarfilepath + '/' + daytarfile)
        #         gznames = tar.getnames()
        #         self.gzpath = self.tempfilepath + '/' + daytarfile.split('.')[0]
        #         # 3.extract gz files
        #         if not os.path.exists(self.gzpath):
        #             for gzname in gznames:
        #                 tar.extract(gzname, self.gzpath)
        #             tar.close()
        #
        # self.gzfiles = os.listdir(self.gzpath)

    # def spider_idle(self, spider):
    #     print('spider idle happens, restart request, current gzfilename:'+ self.gzfiles[self.gzfileindex])
    #     self.start_requests()
    #     raise DontCloseSpider

    def start_requests(self):
        # print('current filename:'+self.gzfiles[self.gzfileindex])
        # os.chdir(self.gzpath)
        # os.chdir(self.tempfilepath)
        self.gzfiles = os.listdir(self.tempfilepath)
        # self.scrawlegzfiles += 1
        try:
            for gzfile in self.gzfiles:

                self.scrawlegzfiles += 1

                with gzip.open(self.tempfilepath+'/'+gzfile,'rb') as gzf:


                    for line in gzf:
                        try:
                            content = line.replace('\n','',1).split('\t')
                            if not content[0].endswith('.ts') and not content[0].endswith('.jpg'):
                                self.macList = content[1]
                                self.scrawledpage += 1
                                # print('read url line :'+str(self.scrawledpage))
                                # yield Request(content[0], self.parse)
                                yield Request('http://2223.gungunbook.net/favicon.ico', self.parse)
                        except Exception as e:
                            traceback.print_exc()
                            print('parse file exception:'+str(e))
        except Exception as e:
            traceback.print_exc()
            print('read file exception'+str(e))

    def parse(self, response):
        os.chdir(self.staticfilepath)
        contenttype = response.headers
        if contenttype['Content-Type'].count('text/html') > 0:
            text = response.body.decode(response.encoding)
            if text.startswith('<'):
                soup = BeautifulSoup(text, features="lxml", from_encoding="utf-8")
                resultcontent = ''
                for p in soup.find_all('p'):
                    [x.extract() for x in p.findAll('script')]
                    content = p.get_text(strip=True)
                    print content
                    print len(content)
                    if len(content) > 5:
                        resultcontent += content
                # resultcontent = textutil.filtertext(text)
                self.contentlogger.info('url:'+response.url)
                self.contentlogger.info('mac:'+self.macList)
                self.contentlogger.info('text:'+resultcontent+'\n')
                self.validpage += 1
            else:
                self.htmlbutnottextpage += 1
                self.invalidurllogger.info(response.url)
        else:
            self.nothtmlpage += 1
            self.invalidurllogger.info(response.url)

        self.scrawlSpeed = self.scrawledpage/(time.time() - self.startTimeUnix)
        self.endTime = time.asctime( time.localtime(time.time()) )
        staticfp = open('statistic.txt', 'w')
        staticfp.write('start_time:'+self.startTime+'\n')
        staticfp.write('scrawled_gzfiles:'+str(self.scrawlegzfiles)+'\n')
        staticfp.write('scrawled_page:'+str(self.scrawledpage)+'\n')
        staticfp.write('scrawled_speed:'+str(self.scrawlSpeed)+'\n')
        staticfp.write('valid_page:'+str(self.validpage)+'\n')
        staticfp.write('invalid_page:'+str(self.nothtmlpage+self.htmlbutnottextpage)+'\n')
        staticfp.write("end_time:"+self.endTime+'\n')
        # compress files