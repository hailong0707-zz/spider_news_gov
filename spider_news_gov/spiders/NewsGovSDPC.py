# -*- coding: utf-8 -*-
import scrapy
from spider_news_gov.items import SpiderNewsGovItem
from bs4 import BeautifulSoup
from scrapy import log
import threading
import MySQLdb


class NewsgovsdpcSpider(scrapy.Spider):
    name = "NewsGovSDPC"
    allowed_domains = ["www.sdpc.gov.cn"]
    start_urls = (
        "http://www.sdpc.gov.cn/zcfb/zcfbl/index.html",
        "http://www.sdpc.gov.cn/zcfb/zcfbgg/index.html",
        "http://www.sdpc.gov.cn/zcfb/wengao/index.html",
        "http://www.sdpc.gov.cn/zcfb/zcfbghwb/index.html",
        "http://www.sdpc.gov.cn/zcfb/zcfbtz/index.html",
        "http://www.sdpc.gov.cn/zcfb/jd/index.html",
        "http://www.sdpc.gov.cn/zcfb/zcfbqt/index.html"
    )

    GOV_NAME_SDPC = u"中国发改委"
    FLAG_INTERRUPT = True
    SELECT_NEWS_SDPC_BY_TITLE = "SELECT title FROM news_gov WHERE type='%s' AND title='%s'"

    lock = threading.RLock()
    conn=MySQLdb.connect(user='root', passwd='123123', db='news', autocommit=True)
    cursor = conn.cursor()

    def is_news_not_saved(self, type1, title):
        if self.FLAG_INTERRUPT:
            self.lock.acquire()
            rows = self.cursor.execute(self.SELECT_NEWS_SDPC_BY_TITLE % (type1, title))
            if rows > 0:
                log.msg(self.GOV_NAME_SDPC + "::" + type1 + " saved all finished !", level=log.INFO)
                return False
            else:
                return True
            self.lock.release()
        else:
            return True

    def parse_news_sdpc(self, response):
        url = response.url
        item = SpiderNewsGovItem()
        type1 = response.meta['type1']
        title = response.meta['title']
        day = response.meta['day']
        year = response.meta['year']
        num = response.meta['num']
        article = key_words = gov_others = attachments = ""
        if type1 == u"文告":
            attachments = url + "::";
        else:
            try:
                response = response.body.decode("utf-8")
                soup = BeautifulSoup(response)
                article_array = soup.find(id="zoom").find_all("p")
                for i in range(0, len(article_array)):
                    p = article_array[i].text.strip()
                    article += p + "\n\r"
                    if len(p.split(u"：")) == 2 and (p.split(u"：")[0].endswith(u"长") or p.split(u"：")[0].endswith(u"主任")):
                        gov_others += p.split(u"：")[0] + "::"
                blue_links = soup.find_all(color="#0000ff")
                for i in range(0, len(blue_links)):
                    blue_link_parent = ""
                    try:
                        blue_link_parent = blue_links[i].parent["href"]
                    except:
                        blue_link_parent = ""
                    if blue_link_parent != "":
                        attachments += url.split("/t")[0] + blue_link_parent.replace("./", "/") + "::"
            except:
                log.msg("News " + url + " parse ERROR !!!", level=log.ERROR)
                return
        item['gov_name'] = self.GOV_NAME_SDPC
        item["type1"] = type1
        item["title"] = title
        item["day"] = day
        item["year"] = year
        item["num"] = num
        item["key_words"] = key_words
        item["article"] = article
        item["gov_others"] = gov_others
        item["attachments"] = attachments
        return item


    def get_type_from_url(self, url):
        type2 = url.split("/")[4]
        if type2 == "zcfbl":
            return u"发展改革委令"
        elif type2 == "zcfbgg":
            return u"公告"
        elif type2 == "wengao":
            return u"文告"
        elif type2 == "zcfbghwb":
            return u"规划文本"
        elif type2 == "zcfbtz":
            return u"通知"
        elif type2 == "jd":
            return u"解读"
        elif type2 == "zcfbqt":
            return u"其他"
        else:
            return u"无"

    def get_root_url(self, url):
        return url.split("/index")[0]

    def get_template_url(self, url):
        type2 = url.split("/")[4]
        if type2 == "zcfbl":
            return "http://www.sdpc.gov.cn/zcfb/zcfbl/index_%s.html"
        elif type2 == "zcfbgg":
            return "http://www.sdpc.gov.cn/zcfb/zcfbgg/index_%s.html"
        elif type2 == "wengao":
            return "http://www.sdpc.gov.cn/zcfb/wengao/index_%s.html"
        elif type2 == "zcfbghwb":
            return "http://www.sdpc.gov.cn/zcfb/zcfbghwb/index_%s.html"
        elif type2 == "zcfbtz":
            return "http://www.sdpc.gov.cn/zcfb/zcfbtz/index_%s.html"
        elif type2 == "jd":
            return "http://www.sdpc.gov.cn/zcfb/jd/index_%s.html"
        elif type2 == "zcfbqt":
            return "http://www.sdpc.gov.cn/zcfb/zcfbqt/index_%s.html"

    def parse(self, response):
        url = response.url
        type1 = self.get_type_from_url(url)
        items = []
        try:
            response = response.body.decode("utf-8")
        except:
            items.append(self.make_requests_from_url(url))
            log.msg("Page " + url + " parse ERROR, try again !", level=log.ERROR)
            return items
        soup = BeautifulSoup(response)
        links = soup.find_all(class_="li")
        need_parse_next_page = True
        type1 = title = day = year = num = "" 
        for i in range(0, len(links)):
            link = (self.get_root_url(url) + links[i].a["href"]).replace("./", "/")
            type1 = self.get_type_from_url(url)
            title = links[i].a.text.replace(u"\u3000", " ").strip()
            _day = links[i].font.text.strip()
            day = _day.split("/")[1] + "/" + _day.split("/")[2]
            year = _day.split("/")[0]
            #" ", "通知" "文告" "意见" "规划"一般作为文件号的分隔符
            _title = title.split(" ")
            _title = _title[len(_title)-1].split(u"通知")
            _title = _title[len(_title)-1].split(u"文告")
            _title = _title[len(_title)-1].split(u"意见")
            _title = _title[len(_title)-1].split(u"规划")
            num = _title[len(_title)-1]
            need_parse_next_page = self.is_news_not_saved(type1, title)
            if not need_parse_next_page:
                break
            items.append(self.make_requests_from_url(link).replace(callback=self.parse_news_sdpc, meta={'type1': type1, "title": title, "day": day, "year": year, "num": num}))
        if need_parse_next_page:
            current_page_num = soup.find(type="text/javascript").text.strip().split(",")[1].strip()
            max_page_num = soup.find(type="text/javascript").text.strip().split(",")[0].split("(")[1]
            if (int(current_page_num) + 1) < int(max_page_num):
                next_page_link = self.get_template_url(url) % str(int(current_page_num)+1)
                items.append(self.make_requests_from_url(next_page_link))
        return items