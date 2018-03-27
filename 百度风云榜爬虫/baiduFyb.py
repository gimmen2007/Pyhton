import requests
import datetime
import cx_Oracle
from lxml import etree
import os
import configparser
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
## 配置oacle连接信息：
conf = configparser.ConfigParser()  实例化对象
# 读取配置文件
conf.read("config_ora.ini") #读取配置文件config_ora.ini
username = conf.get("db",'username')
password = conf.get("db",'password')
db_url = conf.get("db",'db_url')

## 定义爬取的urls
urls = {'综艺':'http://top.baidu.com/buzz?b=19&c=3&fr=topcategory_c3','电视剧':'http://top.baidu.com/buzz?b=4&c=2&fr=topcategory_c2',
      '电影':'http://top.baidu.com/buzz?b=26&c=1&fr=topcategory_c1','动漫':'http://top.baidu.com/buzz?b=23&c=5&fr=topcategory_c5'}
## 定义爬取时间
nowtime=datetime.datetime.now()
now=nowtime.strftime("%Y%m%d")

## 定义header
headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'}

##定义爬虫函数：
def get_info(url,key):
### 爬取数据：
    data=requests.get(url,headers=headers)
    req=etree.HTML(data.content.decode('gbk','ignore'))
    names=req.xpath('//a[starts-with(@class,"list-title")]/text()')
### 定义空列表：
    parms=[]
### 循环处理，将爬取的内容写入列表
    for i in range(0,50):
        parms.append((now,i+1,names[i],key))
### 连接数据库，并对数据进入插入数据库处理，对异常校验：
    try:
        db=cx_Oracle.connect(username, password, db_url)
        cusor=db.cursor()
        cusor.execute("delete from dev.t_baidu_yule_day where statis_date='%s' and type='%s'" % (now,key))
        sql="insert into dev.t_baidu_yule_day(statis_date,id,name,type) values(:1,:2,:3,:4)"
        cusor.executemany(sql,parms)
#### 输出成功日志：
        print("日期：%s,百度风月榜%s页面爬取数据成功。" %(now,key))
#### 获取所有异常信息：
    except Exception as err:
        db.rollback()
### 输出失败日志：
        print("日期：%s,百度风月榜%s页面爬取数据异常,异常信息：%s" %(now,key,err))
    finally:
        db.commit()
        cusor.close()
        db.close()

if __name__=='__main__':
for key,url in urls.items():
    	get_info(url,key)