# coding: utf-8
import sys
import urllib
import urllib2
import textutil
import re
import requests
import StringIO
import gzip
from BeautifulSoup import BeautifulSoup

question_word = "物联网 智能 老年辅助器械"
total = 0
type = sys.getfilesystemencoding()

headersParameters = {
        'Connection': 'Keep-Alive',
        'Accept': 'text/html, application/xhtml+xml, */*',
        'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'User-Agent': 'Mozilla/6.1 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'
    }


url = "http://www.baidu.com/s?wd=" + urllib.quote(question_word.decode(sys.stdin.encoding).encode('gbk'))

filename = unicode(question_word+'.txt','utf-8')

file = open(filename,'w')

while(True):
    if(total > 250):
        break

    request = urllib2.Request(url,data=None,headers=headersParameters)
    # htmlpage = urllib2.urlopen(request).read()
    # htmlpage = urllib2.urlopen(url).read()
    htmlpage = requests.get(url,timeout=2,headers=headersParameters).text
    # print htmlpage
    soup = BeautifulSoup(htmlpage)

    next = re.findall(' href\=\"(\/s\?wd\=[\w\d\%\&\=\_\-]*?)\" class\=\"n\"', htmlpage)
    # print next
    # print next[-1]
    if len(next) > 0:
        url = 'https://www.baidu.com'+next[-1]
        # print url

    for result in soup.findAll('div','result c-container'):
        # print result.a
        text1 = re.sub(r'<.+?>|\s*', '', str(result.a))
        text2 = re.sub(r'<.+?>|\s*', '', text1)
        print text2
        total += 1
        file.write(question_word+' '+text2+'\n')


# for result_table in soup.findAll("table", {"class": "result"}):
#     a_click = result_table.find("a")
#     print "-----标题----\n" + a_click.renderContents()  # 标题
#     print "----链接----\n" + str(a_click.get("href"))  # 链接
#     print "----描述----\n" + result_table.find("div", {"class": "c-abstract"}).renderContents()  # 描述
#     print