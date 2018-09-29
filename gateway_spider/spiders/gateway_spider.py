import scrapy
from scrapy.http import Request
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from .. import textutil
import os
import gzip
import logging
import logging.handlers
import time
import datetime
import traceback
from bs4 import BeautifulSoup
import lxml
from hdfs import InsecureClient
import ConfigParser
from cStringIO import StringIO
import shutil

class Myspider (scrapy.Spider):
    name = 'gateway_spider'

    # config
    config = ConfigParser.RawConfigParser()
    config.read('../scrapy.cfg')
    config.read('scrapy.cfg')
    hadooppath = config.get('spider','hadooppath')
    hadoopuser = config.get('spider','hadoopuser')
    urlgzfilepath = config.get('spider','urlgzfilepath')
    resultfilepath = config.get('spider','resultfilepath')+'/pdate='+time.strftime('%Y%m%d',time.localtime())
    mod = config.get('spider','mod')
    index = config.get('spider','index')
    iscluster = config.get('spider','iscluster')
    logsize = config.get('spider','logsize')

    # localfile path
    statisticfilepath = 'C:\scrapy'+time.strftime('%Y%m%d',time.localtime())+str(index) if sys.platform == 'win32' else '/opt/scrapy'+time.strftime('%Y%m%d',time.localtime())+str(index)
    if os.path.exists(statisticfilepath):
        shutil.rmtree(statisticfilepath)
        os.mkdir(statisticfilepath)
    else:
        os.mkdir(statisticfilepath)

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
    metalogger = logging.getLogger('metalogger')
    invalidurllogger = logging.getLogger('invalidurllogger')

    # default yesterday
    yesterday = (datetime.date.today()-datetime.timedelta(days=1)).strftime('%Y%m%d')
    urldate = '/pdate='+yesterday if config.get('spider','urldate') == '' else '/pdate='+config.get('spider','urldate')
    # parsedate = '/pdate='+yesterday if config.get('spider','parsedate') == '' else '/pdate='+config.get('spider','parsedate')

    # result files
    logfilename = 'parse_result.'+index+'.log'
    urlfilename = 'invalid_urls.log'
    metafilename = 'title_meta.log'

    # hadoopclient
    try:
        hadoopclient = InsecureClient(hadooppath, hadoopuser)
    except Exception as e:
        traceback.print_exc(e)

    def __init__(self):
        super(Myspider, self).__init__()

        self.contentlogger.setLevel(logging.INFO)
        self.invalidurllogger.setLevel(logging.INFO)
        self.metalogger.setLevel(logging.INFO)

        urlfilehandler = logging.FileHandler(self.statisticfilepath +'/'+self.urlfilename)
        urlfilehandler.setLevel(logging.INFO)
        self.invalidurllogger.addHandler(urlfilehandler)

        metafilehandler = logging.FileHandler(self.statisticfilepath+'/'+self.metafilename)
        metafilehandler.setLevel(logging.INFO)
        self.metalogger.addHandler(metafilehandler)

        # 800MB per logfile
        rotatehandler = logging.handlers.RotatingFileHandler(self.statisticfilepath +'/'+self.logfilename ,maxBytes=self.logsize,backupCount=1000)
        self.contentlogger.addHandler(rotatehandler)

    def start_requests(self):
        self.gzfiles = self.hadoopclient.list(self.urlgzfilepath + self.urldate)

        try:
            for gzfile in self.gzfiles:
                if not gzfile.endswith('gz') or (self.iscluster == '1' and (int(str(gzfile)[7:12]) % int(self.mod)) != int(self.index)):
                    continue
                self.scrawlegzfiles += 1
                with self.hadoopclient.read(self.urlgzfilepath + self.urldate + '/' + gzfile) as reader:
                    buf = StringIO(reader.read())
                    with gzip.GzipFile(mode='rb', fileobj=buf) as gzf:
                        for line in gzf:
                            try:
                                content = line.replace('\n','',1).split('\t')
                                if not content[0].endswith('.ts') and not content[0].endswith('.jpg'):
                                    self.macList = content[1]
                                    self.scrawledpage += 1
                                    yield Request(content[0], self.parse)
                                    # yield Request('http://2223.gungunbook.net/favicon.ico', self.parse)
                            except Exception as e:
                                traceback.print_exc()
                                print('parse file exception:'+str(e))
        except Exception as e:
            traceback.print_exc()
            print('read file exception:'+str(e))

        # compress result files and upload to hdfs
        #1. check result files exist
        logging.info('end crawl and start compress files...')
        for logfile in os.listdir(self.statisticfilepath):
            if str(logfile).startswith(self.logfilename):
                #2. compress files one by one
                with open(self.statisticfilepath+os.path.sep+logfile,'rb') as f_in, gzip.open(self.statisticfilepath+os.path.sep+logfile+'.gz','wb') as f_out:
                    shutil.copyfileobj(f_in,f_out)

        logging.info('end compressing and start upload files...')
        #3. upload files to hdfs one by one
        for compressfile in os.listdir(self.statisticfilepath):
            if compressfile.endswith('gz') and os.path.getsize(self.statisticfilepath+os.path.sep+compressfile) > 0:
                try:
                    if None == self.hadoopclient.status(self.resultfilepath,False):
                        self.hadoopclient.makedirs(self.resultfilepath)
                    path = self.hadoopclient.upload(self.resultfilepath,self.statisticfilepath+os.path.sep+compressfile,False,0)
                    if path != None:
                        print('upload '+compressfile+' to '+path+' successfully!')
                    else:
                        print('upload '+compressfile+' failed!')
                except Exception as e:
                    traceback.print_exc()
                    print('upload file exception:' + str(e))

    def parse(self, response):
        contenttype = response.headers
        if contenttype['Content-Type'].count('text/html') > 0:
            text = response.body.decode(response.encoding)
            if text.startswith('<'):
                # title and meta
                soup = BeautifulSoup(text, features="lxml", from_encoding="utf-8")
                metaandtitle = ''
                metaandtitle += soup.title.string
                for meta in soup.find_all("meta"):
                    if meta.has_attr("name") and meta["name"].lower().strip() == "keywords":
                        metaandtitle += meta["content"]
                    if meta.has_attr("name") and meta["name"].lower().strip() == "description":
                        metaandtitle += meta["content"]
                metaandtitle = textutil.filtertext(metaandtitle)
                if metaandtitle != '':
                    self.metalogger.info(response.url)
                    self.metalogger.info(metaandtitle)

                # content
                resultcontent = ''
                resultcontent += metaandtitle
                for p in soup.find_all('p'):
                    [x.extract() for x in p.findAll('script')]
                    content = p.get_text(strip=True)
                    if len(content) > 5:
                        resultcontent += content
                resultcontent = textutil.filtertext(text)
                if resultcontent != '':
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
        staticfp = open(self.statisticfilepath+os.path.sep+'statistic.txt', 'w')
        staticfp.write('start_time:'+self.startTime+'\n')
        # logging.info('start_time:'+self.startTime+'\n')
        staticfp.write('scrawled_gzfiles:'+str(self.scrawlegzfiles)+'\n')
        # logging.info('scrawled_gzfiles:'+str(self.scrawlegzfiles)+'\n')
        staticfp.write('scrawled_page:'+str(self.scrawledpage)+'\n')
        # logging.info('scrawled_page:'+str(self.scrawledpage)+'\n')
        staticfp.write('scrawled_speed:'+str(self.scrawlSpeed)+'\n')
        # logging.info('scrawled_speed:'+str(self.scrawlSpeed)+'\n')
        staticfp.write('valid_page:'+str(self.validpage)+'\n')
        # logging.info('valid_page:'+str(self.validpage)+'\n')
        staticfp.write('invalid_page:'+str(self.nothtmlpage+self.htmlbutnottextpage)+'\n')
        # logging.info('invalid_page:'+str(self.nothtmlpage+self.htmlbutnottextpage)+'\n')
        staticfp.write("end_time:"+self.endTime+'\n')
        # logging.info("end_time:"+self.endTime+'\n')
