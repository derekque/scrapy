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
from hdfs import InsecureClient
import ConfigParser
from cStringIO import StringIO
import shutil
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json


class Myspider (scrapy.Spider):
    name = 'gateway_spider'

    # config
    config = ConfigParser.RawConfigParser()
    config.read('../scrapy.cfg')
    config.read('scrapy.cfg')
    hadooppath = config.get('spider','hadooppath')
    hadoopuser = config.get('spider','hadoopuser')
    urlgzfilepath = config.get('spider','urlgzfilepath')
    # resultfilepath = config.get('spider','resultfilepath')+'/pdate='+time.strftime('%Y%m%d',time.localtime())
    resultfilepath = config.get('spider','resultfilepath')
    mod = config.get('spider','mod')
    index = config.get('spider','index')
    iscluster = config.get('spider','iscluster')
    logsize = config.get('spider','logsize')
    bootstrap_servers=config.get('spider','bootstrap_servers')

    # localfile path
    time_stamp = time.strftime('%Y%m%d',time.localtime())+str(index)
    statisticfilepath = 'C:\scrapy'+time_stamp if sys.platform == 'win32' else '/opt/scrapy'+time_stamp
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
    currentdate = None

    # difference = 0

    # logging
    contentlogger = logging.getLogger('contentlogger')
    # metalogger = logging.getLogger('metalogger')
    invalidurllogger = logging.getLogger('invalidurllogger')

    # default yesterday
    yesterday = (datetime.date.today()-datetime.timedelta(days=1)).strftime('%Y%m%d')
    fromdate_stamp = yesterday if config.get('spider','fromdate').strip() == '' else config.get('spider','fromdate').strip()
    todate_stamp = yesterday if config.get('spider','todate').strip() == '' else config.get('spider','todate').strip()
    fromdate = datetime.datetime(int(fromdate_stamp[0:4]),int(fromdate_stamp[4:6]),int(fromdate_stamp[6:8]))
    todate = datetime.datetime(int(todate_stamp[0:4]),int(todate_stamp[4:6]),int(todate_stamp[6:8]))

    # fromdate = '/pdate='+yesterday if config.get('spider','fromdate') == '' else '/pdate='+config.get('spider','fromdate')
    # todate = '/pdate='+yesterday if config.get('spider','todate') == '' else '/pdate='+config.get('spider','todate')

    # result files
    logfilename = None
    urlfilename = 'invalid_urls.log'
    # metafilename = 'title_meta.log'

    contentfilehandler = logging.FileHandler('temp.txt')
    contentfilehandler.setLevel(logging.INFO)
    contentlogger.addHandler(contentfilehandler)

    # hadoopclient
    try:
        hadoopclient = InsecureClient(hadooppath, hadoopuser)
    except Exception as e:
        traceback.print_exc(e)

    # kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda m: json.dumps(m).encode('utf-8'))



    def __init__(self):
        super(Myspider, self).__init__()

        self.contentlogger.setLevel(logging.INFO)
        self.invalidurllogger.setLevel(logging.INFO)
        # self.metalogger.setLevel(logging.INFO)

        urlfilehandler = logging.FileHandler(self.statisticfilepath +'/'+self.urlfilename)
        urlfilehandler.setLevel(logging.INFO)
        self.invalidurllogger.addHandler(urlfilehandler)


        # metafilehandler = logging.FileHandler(self.statisticfilepath+'/'+self.metafilename)
        # metafilehandler.setLevel(logging.INFO)
        # self.metalogger.addHandler(metafilehandler)

        # 800MB per logfile
        # rotatehandler = logging.handlers.RotatingFileHandler(self.statisticfilepath +'/'+self.logfilename ,maxBytes=self.logsize,backupCount=1000)
        # self.contentlogger.addHandler(rotatehandler)

    def start_requests(self):
        days = (self.todate - self.fromdate).days + 1
        self.currentdate = self.fromdate
        logging.info('start crawl '+str(days)+' days data from '+self.fromdate_stamp+' to '+self.todate_stamp)
        if days < 0:
            logging.info('date config error, please check the config.')
        else:
            for index in range(0,days):
                currentyesterday = datetime.datetime.today() - datetime.timedelta(days=1)
                if self.currentdate > currentyesterday:
                    self.currentdate - datetime.timedelta(days=1)
                    break
                currentdatepath = self.urlgzfilepath +'/pdate='+ self.currentdate.strftime('%Y%m%d')
                logging.info('current reading date:' + self.currentdate.strftime('%Y%m%d'))
                logging.info('current reading path:' + currentdatepath)

                self.logfilename = 'parseresult_'+self.index+'_'+self.currentdate.strftime('%Y%m%d')+'.log'

                self.contentlogger.removeHandler(self.contentfilehandler)

                self.contentfilehandler = logging.FileHandler(self.statisticfilepath + '/' + self.logfilename)
                self.contentfilehandler.setLevel(logging.INFO)
                self.contentlogger.addHandler(self.contentfilehandler)

                self.gzfiles = self.hadoopclient.list(currentdatepath)
                #1. read gz files one by one
                try:
                    for gzfile in self.gzfiles:
                        if not gzfile.endswith('gz') or (self.iscluster == '1' and (int(str(gzfile)[7:12]) % int(self.mod)) != int(self.index)):
                            continue
                        self.scrawlegzfiles += 1
                        with self.hadoopclient.read(currentdatepath + '/' + gzfile) as reader:
                            buf = StringIO(reader.read())
                            with gzip.GzipFile(mode='rb', fileobj=buf) as gzf:
                                for line in gzf:
                                    # print line
                                    try:
                                        content = line.replace('\n','',1).split('\t')
                                        self.macList = content[1]
                                        self.scrawledpage += 1
                                        # yield Request(content[0], meta={'item':self.macList},callback=self.parse)
                                        yield Request('http://2223.gungunbook.net/favicon.ico',meta={'item':self.macList}, callback=self.parse)
                                        # yield Request('http://172.19.0.9:10080/users/sign_in',meta={'item':self.macList}, callback=self.parse)
                                        # self.difference += 1
                                    except Exception as e:
                                        traceback.print_exc()
                                        logging.info('parse file exception:'+str(e))
                except Exception as e:
                    traceback.print_exc()
                    logging.info('read file exception:'+str(e))
                '''
                # print self.crawler.stats.get_stats()

                '''

                # compress result files and upload to hdfs
                #1. check result files exist
                logging.info('end crawl and start compress files...')
                for logfile in os.listdir(self.statisticfilepath):
                    if str(logfile) == self.logfilename:
                        #2. compress files one by one
                        with open(self.statisticfilepath+os.path.sep+logfile,'rb') as f_in, gzip.open(self.statisticfilepath+os.path.sep+logfile+'.gz','wb') as f_out:
                            shutil.copyfileobj(f_in,f_out)
                        # os.remove(self.statisticfilepath + os.path.sep + logfile)
                logging.info('end compressing and start upload files...')
                #3. upload files to hdfs one by one
                destPath = self.resultfilepath + '/pdate=' + self.currentdate.strftime('%Y%m%d')
                for compressfile in os.listdir(self.statisticfilepath):
                    if compressfile.endswith('gz') and os.path.getsize(self.statisticfilepath+os.path.sep+compressfile) > 0:
                        try:
                            if None == self.hadoopclient.status(destPath,False):
                                self.hadoopclient.makedirs(destPath)
                            path = self.hadoopclient.upload(destPath,self.statisticfilepath+os.path.sep+compressfile,True,0)
                            if path != None:
                                logging.info('upload '+compressfile+' to '+path+' successfully!')
                                # os.rename(self.statisticfilepath + os.path.sep + compressfile,self.statisticfilepath + os.path.sep + compressfile + str(self.currentdate))
                                os.remove(self.statisticfilepath + os.path.sep + compressfile)
                            else:
                                logging.info('upload '+compressfile+' failed!')
                        except Exception as e:
                            traceback.print_exc()
                            logging.info('file operation exception:' + str(e))

                self.currentdate = self.currentdate + datetime.timedelta(days=1)

    def parse(self, response):
        # self.difference -= 1
        maclist = str(response.meta['item'])
        # contentlogger = str(response.meta['logger'])
        contenttype = response.headers
        if contenttype['Content-Type'].count('text/html') > 0:
            # text = response.body
            text = response.body.decode(response.encoding)
            # print text
            if text.startswith('<'):
                soup = BeautifulSoup(text, features="lxml", from_encoding="utf-8")
                # response body
                body = textutil.filtertext(soup.text)
                # title and meta
                metaandtitle = ''
                metaandtitle += soup.title.string
                for meta in soup.find_all("meta"):
                    if meta.has_attr("name") and meta["name"].lower().strip() == "keywords":
                        metaandtitle += meta["content"]
                    if meta.has_attr("name") and meta["name"].lower().strip() == "description":
                        metaandtitle += meta["content"]
                metaandtitle = textutil.filtertext(metaandtitle)
                # if metaandtitle != '' and len(metaandtitle) > 20:
                if metaandtitle != '' and len(metaandtitle) > 0:
                    self.contentlogger.info(response.url)
                    self.contentlogger.info(maclist)
                    self.contentlogger.info(metaandtitle+'\n')
                    dict = {'url':response.url,'maclist':maclist,'title':metaandtitle,'timestamp':time.time(),'body':body}
                    try:
                        future = self.producer.send('title-topic', key=b'title', value = dict)
                        record_metadata = future.get(timeout=10)
                    except KafkaError:
                        # Decide what to do if produce request failed...
                            print KafkaError.message
                            pass
                    print (record_metadata.topic)
                    print (record_metadata.partition)
                    print (record_metadata.offset)

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
        # staticfp.write('start_time:'+str(self.difference)+'\n')
        staticfp.write('current_date:'+self.currentdate.strftime('%Y%m%d')+'\n')
        staticfp.write('scrawled_gzfiles:'+str(self.scrawlegzfiles)+'\n')
        staticfp.write('scrawled_page:'+str(self.scrawledpage)+'\n')
        staticfp.write('scrawled_speed:'+str(self.scrawlSpeed)+'\n')
        staticfp.write('valid_page:'+str(self.validpage)+'\n')
        staticfp.write('invalid_page:'+str(self.nothtmlpage+self.htmlbutnottextpage)+'\n')
        staticfp.write("end_time:"+self.endTime+'\n')
