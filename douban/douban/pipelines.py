# -*- coding: utf-8 -*-
import pymysql
from douban.settings import sql_host,sql_db_name,sql_password,sql_sheetname,sql_user
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


class DoubanPipeline(object):
    def __init__(self):
        host = sql_host
        dbname = sql_db_name
        user = sql_user
        password = sql_password
        self.sheetname = sql_sheetname
        self.conn = pymysql.connect(host=host, user=user, password=password, db=dbname, charset='utf8')
        # 创建一个游标
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        serial_number = item['serial_number']
        movie_name = item['movie_name']
        introduce = item['introduce']
        star = item['star']
        evaluate = item['evaluate']
        describes = item['describe']

        sql ="insert into douban_movie(serial_number,movie_name,introduce,star,evaluate,describes) VALUES(%s,%s,%s,%s,%s,%s)"
        self.cursor.execute(sql, (serial_number, movie_name, introduce, star, evaluate, describes))
        self.conn.commit()
        return item
    def close_spider(self,spider):
        self.cursor.close()
        self.conn.close()

