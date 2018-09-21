import scrapy
from scrapy.http import Request
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import re
import tarfile
import os
import gzip
import time

class Myspider (scrapy.Spider):
    name = 'gateway_spider'
    macList = None
    # fileblacklist = ['.JPG','.cab']
    # urlblacklist = ['youxi567.com','mini1.cn','duokan.com']
    # tarfilepath = 'E:/urltarfiles'
    tarfilepath = '/home/app/urltarfiles'

    startTime = time.asctime( time.localtime(time.time()) )
    startTimeUnix = time.time()
    endTime = None

    scrawledpage = 0
    validpage = 0
    nothtmlpage = 0
    htmlbutnottextpage = 0

    scrawlSpeed = 0

    # current gzfile name
    gzfilename = None
    # tarfilepath = '/home/app/spider/'

    def start_requests(self):
        # open file
        daytarfiles = os.listdir(self.tarfilepath)
        for daytarfile in daytarfiles:
            if (os.path.isdir(daytarfile)) and (not daytarfile.endswith('tar.gz')):
                continue
            else:
                tar = tarfile.open(self.tarfilepath + '/' + daytarfile)
                gznames = tar.getnames()
                gzpath = self.tarfilepath + '/' + daytarfile.split('.')[0]
                for gzname in gznames:
                    tar.extract(gzname, gzpath)
                tar.close()

                gzfiles = os.listdir(gzpath)
                for gzfile in gzfiles:
                    if (os.path.isdir(gzfile)) and (not gzfile.endswith('gz')):
                        continue
                    else:
                        os.chdir(gzpath)
                        self.gzfilename = gzfile
                        gzfile = gzip.GzipFile(gzfile)
                        # print ('self.gzfilename:'+ str(self.gzfilename))
                        # f = open(gzfile,'r')
                        for line in gzfile.readlines():
                            # line = gzfile.readline()
                            print line
                            content = line.split('\t')
                            print content
                            url = content[0]
                            # filter files
                            # for x in self.fileblacklist:
                            #     if x in url:
                            #         continue
                            # # filter urls
                            # for y in self.urlblacklist:
                            #     if y in url:
                            #         continue
                            self.macList = content[1]
                            self.scrawledpage += 1
                            #filter the blacklist
                            # yield Request('http://6822.9669.cn/minjie/272073.html', self.parse)
                            # yield Request('http://1024.hlork9.com/pw/htm_data/18/1808/1225175.html', self.parse)
                            # yield Request('http://html.read.duokan.com/mfsv2/download/fdsc3/p01PohN4oFNu/9JieTx63kpfG4.html', self.parse)
                            yield Request(content[0], self.parse)
                        # gzfile.close()



    def parse(self, response):
        contentType = response.headers
        # print contentType
        if contentType['Content-Type'].count('text/html') > 0:
        #     with open('result.txt', 'a') as fw:
        #     print response.text
            # fw.write(text+'\n\n\n')
            # text = response.text.replace('\r','').replace('\n','').replace('\t','').replace(' ','')
            # print response.body
            # print response.encoding
            # print contentType
            # text = response.body
            text = response.body.decode(response.encoding)
            # text = response.text
            # replace script
            if text.startswith('<'):

                text1 = re.sub(r'<script(.|\n)+?</script>','',text)
                # replace style css
                text2 = re.sub(r'<style(.|\n)+?</style>','',text1)
                # replace tag
                text3 = re.sub(r'<.+?>|\s*',' ',text2)
                # text4 = re.sub('[\^\\u4e00-\\u9fa5]+','',text3)
                text4 = re.sub('[\^\\x00-\\xff]+','',text3)
                resulttextname = str(self.gzfilename).split('.')[0]+'.txt'
                # print ('resulttextname:'+resulttextname)
                fp = open(resulttextname,'a')
                fp.write(response.url+'\n')
                # fp.write(str(contentType)+'\n')
                # fp.write(response.headers+'\n')
                fp.write(self.macList)
                # fp.write(text)
                fp.write(text4)
                fp.write('\n\n')
                fp.close()
                self.validpage += 1
            else:
                self.htmlbutnottextpage += 1
                tfp = open('htmlbutnottextpage.txt','a')
                tfp.write(response.url+'\n')
        else:
            self.nothtmlpage += 1
            hfp = open('nothtmlpage.txt', 'a')
            hfp.write(response.url + '\n')

        self.scrawlSpeed = self.scrawledpage/(time.time() - self.startTimeUnix)
        self.endTime = time.asctime( time.localtime(time.time()) )
        staticfp = open('statistic.txt', 'w')
        staticfp.write('start_time:'+self.startTime+'\n'+'scrawled_page:'+str(self.scrawledpage)+'\n'+'scrawled_speed:'+str(self.scrawlSpeed)+'\n'+'valid_page:'+str(self.validpage)+'\n'+'not_html_page:'+str(self.nothtmlpage)+'\n'+'not_text_page:'+str(self.htmlbutnottextpage)+'\n'+"end_time:"+self.endTime+'\n')
