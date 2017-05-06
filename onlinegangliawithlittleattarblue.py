#-*- coding:utf-8 -*-
import threading
from time import ctime,sleep
import requests
import json
import threading
from time import ctime,sleep
import pandas as pd
'''
功能：取到API中的metrics和值，取得metricid,和value
输入：保存键值对的字典d,和getdata()返回的json数据data
输出：返回更新的字典,由于函数参数可以修改d,不用返回
'''
'''
输入：黑名单字典
功能：用于创建黑名单
'''
def blackdir(dblack):
    
    idlist,namelist = loadFormalName()
    name = idlist
    dname ={}#保存id 和name之间的关系
    for i in range(0,len(idlist)):
        dname[idlist[i]] = namelist[i]
    
    #过滤掉方差过小的数据
    df = pd.read_csv('ganglia.csv',names = name)
    s1 = df.std(axis = 0)
    ob = s1[ s1 < 1.0]
    l = list(ob.index)
    df = df.drop(l,axis = 1)
    remaincolumns =  df.columns

    for i in idlist:
        if i not in remaincolumns:
            dblack[i] = 1
    namelist = []
    idlist = []
    
    for i in name:
        if i not in dblack:
            idlist.append(i)
            namelist.append(dname[i])
    blacklist = ['all.unspecified.dbcluster.master.system.os_release','all.unspecified.dbcluster.master.system.os_name','all.unspecified.dbcluster.master.system.machine_type','all.unspecified.dbcluster.master.core.gexec']
    for i in blacklist:
        dblack[i] = 1
    
    return idlist,namelist
    
def loadvalue(data,d,blackd):
    #blacklist用于过滤掉用户不关心的变量，这里给出的是固定不变的量，比如os版本
    #blacklist = ['all.unspecified.dbcluster.master.system.os_release','all.unspecified.dbcluster.master.system.os_name','all.unspecified.dbcluster.master.system.machine_type','all.unspecified.dbcluster.master.core.gexec']
    #解析json文件
    d1 =  data['metrics']
    for k in d1:
        d2 = k
        for ksmall in d2:
            if str(ksmall) == 'id':
                key = str(d2[ksmall])
            if str(ksmall) =='value':
                value = d2[ksmall]
        if key not in blackd:     
            d[key] = value
'''
功能：通过metricsid得到一组metrics name用于生成最后的指定格式的数据通过这种方式，遍历idlist,取出metrics id ,从而将值按照顺序写入二维表中
要求：open的文件必须代表所有属性的特点
返回：namelist,包含所有metrics name的列表，idlist,包含所有metricsid的类别
return idlist,namelist
'''
def loadFormalName():
    with open('addspark.json') as json_file:
        data = json.load(json_file)
    namelist = []
    idlist = []
    d1 =  data['metrics']
    blacklist = ['all.unspecified.dbcluster.master.system.os_release','all.unspecified.dbcluster.master.system.os_name','all.unspecified.dbcluster.master.system.machine_type','all.unspecified.dbcluster.master.core.gexec']
    
    for k in d1:
        d2 = k
        for ksmall in d2:
            if str(ksmall) == 'metric': 
                metricname = d2[ksmall]
            if str(ksmall) == 'id':
                idname = d2[ksmall]
        if idname not in blacklist:
            idlist.append(idname)
            namelist.append(metricname)
    return idlist,namelist

'''
功能：访问API,返回json格式的数据
return data
'''
def getdata():
    cs_url = 'http://192.168.1.102:8080/ganglia/api/v2/metrics'
    cs_user = 'dbcluster'
    cs_psw  = '1'        
    r = requests.get(cs_url, auth=(cs_user, cs_psw))
    data = r.json()
    return data
'''
输入：字典d,d中保留了metricsid 和对应的瞬时值;d2就是保存metricname对应metricid
输出：固定格式的一组数据，使用列表存储,从而达到buffer maxsize进行溢出
'''
def extract(d,idlist):
    outcome = []
    for i in idlist:
        outcome.append(float(d[i]))############################
    return outcome
'''
功能：将buf中的数据写入文件
'''
def savefile(buf):
    defaultFileName = 'gangliafile.csv'
    output = open(defaultFileName,'a')
    attrnum = len(buf[0])
    recordnum = len(buf)
    for i in range(0,recordnum):
        for j in range(0,attrnum-1):
            output.write(str(buf[i][j]))
            output.write(",")
        output.write(str(buf[i][j]))
        output.write("\n")
    output.close()
    print "csv finish"
'''
功能：将extract到的数据加入到buf中，一方面与异常探测结合，另一方面，当数据达到一定大小，就溢出到文件中
'''
def addBuf(outcome,buf):
    buf.append(outcome)
    if len(buf) >= 50:
        savefile(buf)
        return True
    return False
def savename(namelist,idlist):
    defaultFileName = 'ganglianame.csv'
    output = open(defaultFileName,'w')
    for i in range(0,len(namelist)-1):
        output.write(str(namelist[i]))
        output.write(",")
    output.write(str(namelist[i]))
    output.write('\n')
    
    for i in range(0,len(idlist)-1):
        output.write(str(idlist[i]))
        output.write(",")
    output.write(str(idlist[i]))
    output.write('\n')
    
    
    print "loading name information is over"


