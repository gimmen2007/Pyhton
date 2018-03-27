import requests, random, datetime
from lxml import etree
import configparser
import cx_Oracle
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# 数据库实例化配置
conf = configparser.ConfigParser()
# 读取配置文件（配置文件自行重新配置即可，以下为模板，由于涉及系统安全，不做展示）
conf.read("/home/crawler/pyobjects/baiduZongyi/config_ora.ini")
username = conf.get("db", 'username')
password = conf.get("db", 'password')
db_url = conf.get("db", 'db_url')

# headers配置
headers = {
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) \
Chrome/64.0.3282.186 Safari/537.36'
}
# 定义一些列表和常量
nowtime = datetime.datetime.now()
now = nowtime.strftime("%Y%m%d")
# 咪咕音乐id
mgid = '577FF1A0197D256CE0533120A8C0DF8C'
mgname = '咪咕音乐'
# 歌手名列表
names = []
# 输出列表集合
params = []


# 定义爬虫函数：
def getMiguInfo(url):
    try:
        datas = requests.get(url, headers=headers)
        req = etree.HTML(datas.content.decode('utf-8', 'igonre'))
        names1 = req.xpath('//div[@class="artist-desc"]/span/text()')
        names2 = req.xpath(
            '//div[starts-with(@class,"artist-item artist-custom")]/a/text()')
        names.extend(names1)
        names.extend(names2)
    except Exception as e:
        pass


# 定义数据库操作函数：
def insertOracle(params):
    db = cx_Oracle.connect(username, password, db_url)
    cusor = db.cursor()
    try:
        cusor.execute(
            "delete from  ulams.T_PC_HS_HOT_SINGER where statis_date='%s' and platform_id='%s'"
            % (now, mgid))
        sql = "insert into ulams.T_PC_HS_HOT_SINGER(platform_id,singer_id,singer_name,statis_date,seq) values(:1,:2,:3,:4,:5)"
        cusor.executemany(sql, params)
        print("成功")
    except Exception as err:
        db.rollback()
        print("失败")
    finally:
        db.commit()
        cusor.close()
        db.close()


# 爬取目标网页
urls = [
    'http://music.migu.cn/v2/music/billboard?listId=57F61F0C3B1C40ACE050190A4BF5028F&nodeId=17580&page={}'.
    format(num) for num in range(1, 10)
]

if __name__ == '__main__':
    for url in urls:
        getMiguInfo(url)

    for i in range(0, len(names)):
        params.append((mgid, random.randint(1001, 9999), names[i], now, i + 1))

    insertOracle(params)
