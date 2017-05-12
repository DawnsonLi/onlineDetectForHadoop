#-*- coding:utf-8 -*-
'''
该版本是固定窗口的版本，采用缓冲区满进行淘汰,不使用可变窗口技巧，以求得更高的更新速度
只关注异常的探测，不进行原因分析和解释
'''
import pandas as pd
from sklearn.cluster import Birch
from sklearn.ensemble import IsolationForest
from sklearn import tree
import pydotplus 
import time
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.feature_selection import SelectFromModel
from time import ctime,sleep
import requests
import json


import numpy as np

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
要求：open的文件必须代表所有属性的特点，至少有1条拥有全部属性的记录
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
        outcome.append(float(d[i]))
    return outcome

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
'''
功能：判断模型是否需要更新
更新条件：缓存达到了最大值
输入：buf,缓冲区大小，miu用户定义错误率阈值，analomyNum:当前窗口累计的异常数目
'''
def detectUpdate(buf, miu, maxContainSize,analomyNum):
    if len(buf) >= maxContainSize:
        return True
    #if float(analomyNum)/maxContainSize >miu:
    #   return True
    return False

'''
这里采用知网里给的一种方法，原始论文（2013）中使用数据块作为每一个窗口，而这里再更新时，如果缓存数目达到最大值，则使用缓存中数据进行更新，否则使用原来窗口加上缓存中的数据进行更新
输入：window当前窗口，buf缓冲区，maxContainSize 最大数目
输出：新窗口，新探测隔离树
其他：这里要判断更新窗口的情况，有两种
'''    
def updateWindow(window,buf,maxContainSize):
    if len(buf) >= maxContainSize:#使用buf更新
        print "buffer full "
        for i in buf:
            window.append(i)
        ilf = IsolationForest(n_estimators=100,contamination=0.01)
        ilf.fit(window)
        print "isolation update finished"
    
    else:                       #使用原来窗口和buf进行更新
        print "higher than threads"
        for i in buf:
            window.append(i)
        ilf = IsolationForest(n_estimators=100,contamination=0.01)
        ilf.fit(window)
        print "isolation update finished"
    return window,ilf
'''
功能：判断是否有连续的异常点产生，因为只有单个孤立的异常点，很有可能是噪声带来的，而当有大片的产生时，需要判定原因
输入：缓冲区buf里的预测的lable,连续异常报警个数 阈值k
判断是否有连续的k个异常，如果有，返回true,发出警告
'''
def warn(buf,lable,k):
    if lable[-1] == 1:
        return False #当前并没有异常点，无需处理
    if len(lable) <= k or len(buf)<=k: #没有探测的必要，缓存中没有那么多数据
        return False
    #回溯，假如有连续的k-1个-1，报警
    index = -2
    for i in range(0,k):
        if lable[index] ==1:
            return False
        index -= 1
    
    #处理一下
    print "Warning for Continuous outliers have been detected"
    return True

'''
功能：模拟实际的数据流运行    
'''
def init(idlist,d,dblack,outcome,winsize=200,sleeptime = 5):
    #创建窗口
    window =  []
    while True:
        print "fetching at %s" %ctime()
        data = getdata()
        loadvalue(data, d,dblack)
        outvalue = extract(d,idlist)
        window.append(outvalue)
        if len(window) > winsize:
            break
        sleep(sleeptime)
    #创建探测器
    ilf = IsolationForest(n_estimators=100,contamination=0.01)
    ilf.fit(window)
    print ilf.predict(window)
    for i in ilf.predict(window):
        outcome.append(i)
    #返回
    return ilf,window


def online_detect(sleeptime = 15):
   
    maxContainSize = 250
    analomyNum = 0
    allanalomy = 0
    
    outcome = []
    lable = []
    k = 6#警告因子
    
    d = {}
    buf = []
    dblack ={}
    
    idlist,namelist = blackdir(dblack)
    savename(namelist,idlist)
    ilf,window= init(idlist,d,dblack,outcome,50,sleeptime)
    print outcome
    print "initial finished"
    counter = 1
    
    while True:
        print "fetching at %s" %ctime()
        data = getdata()
        loadvalue(data, d,dblack)
        outvalue = extract(d,idlist)
        #添加到缓冲区
        buf.append(outvalue)
        #reshape从而编程1*x列向量用于预测
        reshapevalue = np.array(outvalue).reshape(1,-1)
        predictValue = ilf.predict(reshapevalue)
        #输出结果
        print "predict:",predictValue
        a = int(predictValue)
        outcome.append(a)
        lable.append(a)
        if a == -1:
            analomyNum += 1
            allanalomy += 1
        
        #判断是否需要警告
        if warn(buf,lable,k):
            lable[-1] = 1 #防止重复，连续的分析原因
            
        if detectUpdate(buf, 0.97, maxContainSize, analomyNum):#0.087
            del ilf
            window,ilf = updateWindow(window, buf, maxContainSize)
            analomyNum = 0
            del buf        
            buf = []
            
        counter += 1
        if counter %50 ==0:
            break
        sleep(sleeptime)
        
online_detect(5)