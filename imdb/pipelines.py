# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
import csv
class ImdbPipeline(object):
    def __init__(self):
        # 连接数据库
        self.connect = pymysql.connect(
            host='114.115.151.219',  # 数据库地址
            port=3308,  # 数据库端口
            db='Moviedb',  # 数据库名
            user='Moviedb',  # 数据库用户名
            passwd='zr85GFRpw66W8pf4',  # 数据库密码
            charset='utf8',  # 编码方式
            use_unicode=True)
        # 通过cursor执行增删查改
        self.cursor = self.connect.cursor();
        self.csv_file = open("movies.csv", 'w', encoding='utf-8-sig',newline='')
        # 定义一个列表，用于整合所有的信息
        self.csv_items = []

    def process_item(self, item, spider):
        self.cursor.execute(
            """insert into imdb_info(title,rating,name,alias,director,actor,length,language,year,type,color,area,voice,summary,url)
            value (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s)""",
            (item['video_title'],  # item里面定义的字段和表字段对应
             item['video_rating'],
             item['video_name'],
             item['video_alias'],
             item['video_director'],
             item['video_actor'],
             item['video_length'],  # item里面定义的字段和表字段对应
             item['video_language'],
             item['video_year'],
             item['video_type'],
             item['video_color'],
             item['video_area'],
             item['video_voice'],
             item['video_summary'],
             item['video_url']
             ))
        # 提交sql语句
        self.connect.commit()

        #data_1ist = [(('title'),('rating'),('name'),('alias'),('director'),('actor'),('length'),('language'),('year'),('type'),('color'),('area'),('voice'),('summary'),('url'))]  # 第一信息行
        item_csv = []
        item_csv = (item['video_title'],  # item里面定义的字段和表字段对应
             item['video_rating'],
             item['video_name'],
             item['video_alias'],
             item['video_director'],
             item['video_actor'],
             item['video_length'],  # item里面定义的字段和表字段对应
             item['video_language'],
             item['video_year'],
             item['video_type'],
             item['video_color'],
             item['video_area'],
             item['video_voice'],
             item['video_summary'],
             item['video_url'])
        print(item_csv)
        print('sss')
        self.csv_items.append(item_csv)
        return item  # 必须实现返回

    def close_spider(self,spider):
        writer = csv.writer(self.csv_file)
        writer.writerow(['title','rating','name','alias','director','actor','length','language','year','type','color','area','voice','summary','url'])  # 第一信息行
        writer.writerows(self.csv_items)
        self.csv_file.close()
