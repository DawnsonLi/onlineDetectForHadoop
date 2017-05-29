#-*- coding:utf-8 -*-
from influxdb import DataFrameClient
import time
def sampleDecay(sampletime,alpha,samplesize):

    k = len(sampletime)
    T = sampletime[-1]#取出最晚的时间
    temp = 0.0
    for i in range(0,k):
        temp += 1.0/(1.0+alpha*(T-sampletime[i]))
    z = 1.0/temp#正则化因子，保证各个样本权重总和为1
    print z
    weight = []
    for i in range(0,k):
        weight.append(z/(1.0+alpha*(T-sampletime[i])))
    return  weight

def sampleWithDecay(client):
    query = 'select * from ganglia where w_fs >0 and w_namenode>0 and w_rpc >0 ;'  # 显示数据库中的表
    result = client.query(query, chunked=False)
    data = result['ganglia']
    dbtime = data.index
    keylist = []
    for i in range(0,len(dbtime)):
        t = dbtime[i]
        temp = str(t).split('.')
        smallt= temp[0]
        a = time.mktime(time.strptime(str(smallt), '%Y-%m-%d %H:%M:%S'))
        keylist.append(a)
    weight = sampleDecay(keylist,0.0001,100)
    return  data.sample(n=30, weights=weight)
client = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='testdb')
print sampleWithDecay(client)