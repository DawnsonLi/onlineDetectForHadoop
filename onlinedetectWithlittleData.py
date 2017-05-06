#-*- coding:utf-8 -*-
'''
该版本是固定窗口的版本，采用缓冲区满和错误率高于阈值进行淘汰
'''
import pandas as pd
from pandas import DataFrame
from sklearn.cluster import Birch
from sklearn.ensemble import IsolationForest
from sklearn import tree
import pydotplus 
import time
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.feature_selection import SelectFromModel

from onlineganglia import *

import numpy as np
'''
功能：判断模型是否需要更新
更新条件：在缓存中的判定的异常样本的比例大于阈值miu ||缓存达到了最大值
输入：buf,缓冲区大小，miu用户定义错误率阈值，analomyNum:当前窗口累计的异常数目
'''
def detectUpdate(buf, miu, maxContainSize,analomyNum):
    if len(buf) >= maxContainSize:
        return True
    if float(analomyNum)/maxContainSize >miu:
        return True
    return False
'''
这里对窗口window进行聚类分析，留下窗口中的大部分，而不是全部舍弃
'''
'''
返回最大个数的类别标号
'''
def chooseMax(num0 ,num1, num2):
    if num0 > num1:
        if num0 > num2:
            return 0
    else:
        if num1 > num2:
            return 1
        else:
            return 2
def clusteringReminMost(window):
    brc = Birch(branching_factor=50, n_clusters=3, threshold=0.5,compute_labels=True)
    brc.fit(window)
    Class = brc.predict(window)
    #统计各个类别的信息，找出个数最多的类别，取出这些数据，从而强化历史数据
    num0 = 0
    num1 = 0
    num2 = 0
    
    for i in Class :
        if i == 0:
            num0 += 1
        elif i ==1:
            num1 +=1
        else:
            num2 +=1
    lable = chooseMax(num0, num1, num2)
    newwindow = window[0:1]
    for i in range(1,len(Class)):
        if Class[i] == lable:#属于目标类别，则进行添加
            newwindow = newwindow.append(window[i-1:i])#都为pandas数据结果
    return newwindow
'''
这里采用知网里给的一种方法，原始论文（2013）中使用数据块作为每一个窗口，而这里再更新时，如果缓存数目达到最大值，则使用缓存中数据进行更新，否则使用原来窗口加上缓存中的数据进行更新
输入：window当前窗口，buf缓冲区，maxContainSize 最大数目
输出：新窗口，新探测隔离树
其他：这里要判断更新窗口的情况，有两种
'''    
def updateWindow(window,buf,maxContainSize):
    if len(buf) >= maxContainSize:#使用buf更新
        print "buffer full "
        window = clusteringReminMost(window)
        print "window size after clustering without adding buffer :",len(window)
        for i in buf:
            window = window.append(i)
        ilf = IsolationForest(n_estimators=100,verbose=2,)
        ilf.fit(window)
        print "isolation update finished"
    
    else:                       #使用原来窗口和buf进行更新
        print "higher than threads"
        for i in buf:
            window = window.append(i)
        ilf = IsolationForest(n_estimators=100,verbose=2,)
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
    pass
    return True
'''
功能：分析异常产生的原因，创建训练样本
对buf中的k个异常点和buf中其余的正常点,通过lable进行控制，相当于训练样本的lable已知的分类问题
局限性：只针对了当前的窗口，其实可以做成历史窗口版本，也就是缓存有历史的正常数据，以后做
输入：name 属性名称
'''
def analyseWarn(buf,lable,k,name,topk=5):
    
    bufReminLen = len(buf) - k
    if bufReminLen <= k:
        print "NormalData remained is little,can not analyse"
        return False
    #构建训练样本
    anamolySample = DataFrame(buf[-1])
   # print anamolySample 
    
    index = -2
    for i in range(1,k):
        anamolySample = anamolySample.append(buf[index])
        index -= 1
    first = True
    while bufReminLen > 0:
        if lable[index] == 1:
            if first:
                normalSample = buf[index]
                first =False
            else:
                normalSample = normalSample.append(buf[index])
        else:
            anamolySample = anamolySample.append(buf[index])
        index -= 1
        bufReminLen -=1
    if len(normalSample) < len(anamolySample)/4:
        print "NormalData remained is little,can not analyse"
        print len(anamolySample)
        print len(normalSample),"***********************************"
        return False
    
    ###################分析原因了
    print "begin to analyse the reason"
    print len(anamolySample)
    print len(normalSample),"***********************************"
    #analyseReasonWithDecisonTree(anamolySample,normalSample)
    analyseReasonWithTreeBaesd(anamolySample, normalSample,name)
    return True
    '''
    bug 日记，k=3,第一次更新完毕，因为出现连续3个-1，之后又连续3个-1
    if len(normalSample) < len(anamolySample)/4:
    UnboundLocalError: local variable 'normalSample' referenced before assignment
    '''

'''
分析异常产生的原因。当前使用DecisonTree进行分析
'''
def analyseReasonWithDecisonTree(anamolySample,normalSample,name):
    data = anamolySample
    target = []
    for i in range(0,len(anamolySample)):
        target.append(1)
    data = data.append(normalSample)
    for i in range(0,len(normalSample)):
        target.append(0)
    print len(data)
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(data, target)
    
    dot_data = tree.export_graphviz(clf, out_file=None,feature_names=name,filled = True,special_characters=True) 
    graph = pydotplus.graph_from_dot_data(dot_data) 
    s = str(time.time())
    graph.write_pdf(s+"DT.pdf")
'''
分析异常产生的原因与哪几个变量关系最密切。当前使用卡方检验进行分析
'''

def analyseReasonWithXsqure(anamolySample,normalSample,topk,name):
    data = anamolySample
    target = []
    for i in range(0,len(anamolySample)):
        target.append(1)
    data = data.append(normalSample)
    for i in range(0,len(normalSample)):
        target.append(0)
    name = []
    for i in data.columns:
        name.append(i)
    X_new = SelectKBest(chi2, topk).fit(data, target)
    outcome = X_new.get_support()
    for i in range(0,len(name)):
        if outcome[i]:
            print name[i]

'''
基于树和集成学习，学习特征的重要性
'''
def analyseReasonWithTreeBaesd(anamolySample,normalSample,name):
    data = anamolySample
    target = []
    for i in range(0,len(anamolySample)):
        target.append(1)
    data = data.append(normalSample)
    for i in range(0,len(normalSample)):
        target.append(0)
   
   
    clf = ExtraTreesClassifier()
    clf = clf.fit(data,target)   
    model = SelectFromModel(clf,prefit=True) 
    outcome = model.get_support()
    for i in range(0,len(name)):
        if outcome[i]:
            print name[i]
'''
功能：模拟实际的数据流运行    
'''
def init(idlist,d,dblack,winsize=50):
    
    data = getdata()
    loadvalue(data, d,dblack)
    outvalue = extract(d,idlist)
    print len(outvalue)
    reshapevalue = np.array(outvalue).reshape(1,-1) 
    window =  DataFrame(reshapevalue)
    buf = []#################
    while True:
        print "fetching at %s" %ctime()
        data = getdata()
        loadvalue(data, d,dblack)
        outvalue = extract(d,idlist)
        reshapevalue = np.array(outvalue).reshape(1,-1) 
        window = window.append(DataFrame(reshapevalue))#加入dataframe对象，1row * xcolums
        buf.append(DataFrame(reshapevalue))
        print len(window)
        if len(window) > winsize:
            break
        sleep(5)
    ilf = IsolationForest(n_estimators=100,verbose=2,)
    ilf.fit(window)
    print ilf.predict(window)
    print "__________________"
    for i in buf:
        print ilf.predict(i)
    return ilf,window



def online_detect():
   
    maxContainSize = 500
    analomyNum = 0
    allanalomy = 0
    
    outcome = []
    lable = []
    k = 10#警告因子
    
    d = {}
    buf = []
    dblack ={}
    idlist,namelist = blackdir(dblack)
    
    savename(namelist,idlist)
    ilf,window= init(idlist,d,dblack,100)
    print window
    print "initial finished"
    counter = 1
    print idlist
    print len(idlist)
    print namelist
    print len(namelist)
    for i in dblack:
        print i
    
    while True:
        print "fetching at %s" %ctime()
        data = getdata()
        loadvalue(data, d,dblack)
        outvalue = extract(d,idlist)
        reshapevalue = np.array(outvalue).reshape(1,-1)
        predictValue = ilf.predict(reshapevalue)
        print "predict:",predictValue
        
        a = int(predictValue)
        outcome.append(a)
        lable.append(a)
        buf.append(DataFrame(reshapevalue))#加入dataframe对象，1row * xcolums
        
        if a == -1:
            analomyNum += 1
            allanalomy += 1
        
       
        #判断是否需要警告
        if warn(buf,lable,k):
            lable[-1] = 1 #防止重复，连续的分析原因
            analyseWarn(buf,outcome,k,namelist)#就进行原因分析
            updateWindow(window, buf, maxContainSize)
            
        if detectUpdate(buf, 0.87, maxContainSize, analomyNum):#0.087
            del ilf
            window,ilf = updateWindow(window, buf, maxContainSize)
            analomyNum = 0
            del buf        
            buf = []
            
        
        counter += 1
        if counter %5000 ==0:
            break
        sleep(5)
        
online_detect()