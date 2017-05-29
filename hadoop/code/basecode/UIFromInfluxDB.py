#! /usr/bin/env python
#-*- coding:utf-8 -*-
from time import ctime,sleep
import requests
from influxdb import InfluxDBClient


def loadvalue(data, d):
    d1 = data['metrics']
    for k in d1:
        d2 = k
        c = 0
        for ksmall in d2:
            if str(ksmall) == 'metric':
                key = str(d2[ksmall])
                c += 1
            if str(ksmall) == 'value':
                value = d2[ksmall]
                c += 1

            if c == 2:
                d[key] = value


'''
功能：访问API,返回json格式的数据
return data
'''

def getdata():
    cs_url = 'http://192.168.1.50:8080/ganglia/api/v2/metrics'
    cs_user = 'dbcluster'
    cs_psw = '1'
    r = requests.get(cs_url, auth=(cs_user, cs_psw))
    data = r.json()
    return data
def writeToTSDB(d):

    json_body = [
        {
            "measurement": "ganglia",
            "fields": d
        }
    ]

    client = InfluxDBClient('localhost', 8086, 'root', '', 'testdb')  #初始化（指定要操作的数据库）
    client.write_points(json_body)  #写入数据


d = {}
while True:
        print "fetching at %s" %ctime()
        data = getdata()

        loadvalue(data,d)

        writeToTSDB(d)
        sleep(15)

'''
json_body = [
    {
        "measurement": "ganglia",

        "fields": {
            "waring":1
        }
    }
]
#自定义度量，用于警报，已经设定了阈值为0.6，也可以做成打分器
'''
