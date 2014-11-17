# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import threading
import MySQLdb
from scrapy import log
import urllib2
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
import os
#just for windows
# from win32com import client as wc

class SpiderNewsGovPipeline(object):

    HEADERS = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:24.0) Gecko/20100101 Firefox/24.0"
            # "Referer": "http://ear.duomi.com/wp-content/plugins/audio-player/assets/player.swf?ver=2.0.4.1"
            # "Host": "stream0.kxt.fm",
            # "Connection": "keep-alive",
            # "Accept-Language": "zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3",
            # "Accept-Encoding": "gzip, deflate",
            # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }

    SELECT_NEWS_SDPC_BY_TITLE = "SELECT * FROM news_gov WHERE type='%s' AND title='%s'"
    INSERT_NEWS_SDPC = ("INSERT INTO news_gov (gov_name, type, title, day, year, num, key_words, article, gov_others, attachments, attachments_content) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    ROOT_PATH = r"/Users/hailong/Workspace/python_workspace/spider_news_gov"

    lock = threading.RLock()
    conn=MySQLdb.connect(user='root', passwd='123123', db='news', autocommit=True)
    cursor = conn.cursor()


    def insert(self, gov_name, type1, title, day, year, num, key_words, article, gov_others, attachments, attachments_content):
        self.lock.acquire()
        try:
            rows = self.cursor.execute(self.SELECT_NEWS_SDPC_BY_TITLE % (type1, title))
        except:
            rows = 0
        if rows > 0:
            log.msg(gov_name + "::" + type1 + " '" + title + "' has already saved !", level=log.INFO)
            return
            self.lock.release()
        else:
            news = (gov_name, type1, title, day, year, num, key_words, article, gov_others, attachments, attachments_content)
            try:
                self.cursor.execute(self.INSERT_NEWS_SDPC, news)
                log.msg(gov_name + "::" + type1 + " '" + title + "' saved successfully", level=log.INFO)
            except:
                log.msg("MySQL exception !!!", level=log.ERROR)
            self.lock.release()

    def parse_pdf(self, file_name):
        try:
            laparams = LAParams()
            caching = True
            outfile = "tmp"
            codec = 'utf-8'
            rsrcmgr = PDFResourceManager(caching=caching)
            outfp = file(outfile, 'w')
            device = TextConverter(rsrcmgr, outfp, codec=codec, laparams=laparams)
            fp = file(file_name, 'rb')
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            for page in PDFPage.get_pages(fp):
                interpreter.process_page(page)
            fp.close()
            device.close()
            outfp.close()
            content = ""
            f = open(outfile)
            for line in f:
                # content += line.strip() + "\n\r"
                content += line.strip()
            f.close()
            return content
        except:
            return "parse pdf error :("

    def parse_word(self, file_path):
        try:
            word = wc.Dispatch('Word.Application')
            doc = word.Documents.Open(file_path)
            #maybe need absolute path
            outfile = r'tmp_txt.txt'
            doc.SaveAs(outfile, 4)
            doc.Close()
            f = open(outfile)
            for line in f:
                # content += line.strip() + "\n\r"
                content += line.strip()
            f.close()
            return content
        except:
            return "parse word error :("

    def download(self, url, file_name):
        try:
            log.msg("Start download attachment: " + file_name)
            req = urllib2.Request(url, headers = self.HEADERS)
            data = urllib2.urlopen(req).read()
            f = open(file_name, "wb")
            f.write(data)
            f.close
            log.msg("End download attachment: " + file_name)
        except :
            log.msg("Download exception")

    def get_file_size(self, file_name):
        try:
            filepath = os.path.join(self.ROOT_PATH, file_name)
            return os.path.getsize(filepath)
        except:
            return 500001

    def process_item(self, item, spider):
        gov_name = item['gov_name']
        type1 = item["type1"]
        title = item["title"]
        day = item["day"]
        year = item["year"]
        num = item["num"]
        key_words = item["key_words"]
        article = item["article"]
        gov_others = item["gov_others"]
        attachments = item["attachments"]
        _attachments = attachments.split("::")
        attachments_content = ""
        for i in range(0, len(_attachments)-1):
            attachment_array = _attachments[i].split("/")
            filename = _attachments[i].split("/")[len(attachment_array)-1]
            self.download(_attachments[i], gov_name+"_"+type1+"_"+title+"_"+filename)
            if filename.endswith(".pdf"):
                if self.get_file_size(gov_name+"_"+type1+"_"+title+"_"+filename) <= 500000:
                    attachments_content += self.parse_pdf(gov_name+"_"+type1+"_"+title+"_"+filename) + "\n\r***************\n\r"
            # parse word only support windows platform
            # if filename.endswith(".doc") or filename.endswith(".docx"):
            #     if self.get_file_size(gov_name+"_"+type1+"_"+title+"_"+filename) <= 500000:
            #         attachments_content += self.parse_word(gov_name+"_"+type1+"_"+title+"_"+filename) + "\n\r***************\n\r"
        self.insert(gov_name, type1, title, day, year, num, key_words, article, gov_others, attachments, attachments_content)
