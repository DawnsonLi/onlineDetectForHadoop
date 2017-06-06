# -*- coding:utf-8 -*-
'''
该版本为支持模拟数据的模拟程序,与v1.3架构相同
'''
import pandas as pd
from sklearn.ensemble import IsolationForest
from time import ctime, sleep
import requests
import numpy as np
from sklearn import tree
import time
from datetime import datetime
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.feature_selection import SelectFromModel
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
'''
功能：分析当前是否需要原因分析
原理：触发一定条件就要进行原因分析
实现：当预测异常点的个数达到warnfactor时报警
输入：label,某个模块中的label,这里为计数变量；warnfactor，警告因子
返回：true/false
'''
def warn_analyse(label, warnfactor):
    if label >= warnfactor:
        return True
    return False
'''
功能：直接从数据库中进行样本读取
输入：name属性名称，topk给出的原因个数,qname为查询词，如w_fs
调用：具体的分析方法
'''
def analyseWarn(name,qname,topk=6):
    client = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='testdb')
    query_positive = 'select * from simulate where '+qname+' >0 ORDER BY time DESC limit 10'
    query_negative = 'select * from simulate where '+qname+' <0 ORDER BY time DESC limit 5'

    data_p = client.query(query_positive, chunked=False)
    data_positive = data_p['simulate']
    normalSample = data_positive[name]
    data_n = client.query(query_negative, chunked=False)
    data_negative = data_n['simulate']
    anamolySample = data_negative[name]
    return  analyseReasonWithXsqure(anamolySample, normalSample, topk, name)
    #return analyseReasonWithTreeBaesd(anamolySample, normalSample, name)
def writeToTSDB(d):
    json_body = [
        {
            "measurement": "simulate",
            "fields": d
        }
    ]

    client = InfluxDBClient('localhost', 8086, 'root', '', 'testdb')  # 初始化（指定要操作的数据库）
    client.write_points(json_body)  # 写入数据
'''
分析异常产生的原因与哪几个变量关系最密切。当前使用卡方检验进行分析
'''
def analyseReasonWithXsqure(anamolySample, normalSample, topk, name):

    target = []
    for i in range(0, len(anamolySample)):
        target.append(1)
    data = pd.concat([anamolySample, normalSample])
    for i in range(0, len(normalSample)):
        target.append(0)

    X_new = SelectKBest(chi2, topk).fit(data, target)
    outcome = X_new.get_support()
    warnstr = ""
    for i in range(0, len(name)):
        if outcome[i]:
            warnstr += name[i]
            warnstr += "   ;   "
    print 'x2:',warnstr
    return warnstr

'''
基于树和集成学习，学习特征的重要性
'''
def analyseReasonWithTreeBaesd(anamolySample, normalSample, name):
    target = []
    for i in range(0, len(anamolySample)):
        target.append(1)
    data = pd.concat([anamolySample,normalSample])
    for i in range(0, len(normalSample)):
        target.append(0)

    clf = ExtraTreesClassifier()
    clf = clf.fit(data, target)
    model = SelectFromModel(clf, prefit=True)
    outcome = model.get_support()

    warnstr = ""
    for i in range(0, len(name)):
        if outcome[i]:
            warnstr += name[i]
            warnstr += "   ;   "
    print warnstr
    return warnstr

'''
功能：初始化白名单
返回：l_sys,l_namenode,l_FS,l_RPC模块参数列表
'''
def init_white():
    # 分别加载各个文件
    FS = 'hadoop/FS.csv'
    data = pd.read_csv(FS, delimiter=',')
    l_FS = []
    for i in data.columns:
        l_FS.append(i)

    namenode = 'hadoop/namenode.csv'
    data = pd.read_csv(namenode, delimiter=',')
    l_namenode = []
    for i in data.columns:
        l_namenode.append(i)

    RPC = 'hadoop/RPC.csv'
    data = pd.read_csv(RPC, delimiter=',')
    l_RPC = []
    for i in data.columns:
        l_RPC.append(i)

    sys = 'hadoop/system.csv'
    data = pd.read_csv(sys, delimiter=',')
    l_sys = []
    for i in data.columns:
        l_sys.append(i)

    return l_sys, l_namenode, l_FS, l_RPC
'''
构建白名单字典，便于快速过滤
'''
def dir_white(l_sys, l_namenode, l_FS, l_RPC):
    dwhite = {}
    for i in l_sys:
        dwhite[i] = 1
    for i in l_namenode:
        dwhite[i] = 1
    for i in l_FS:
        dwhite[i] = 1
    for i in l_RPC:
        dwhite[i] = 1
    return dwhite

'''
输入：字典d,d中保留了metrics和对应的瞬时值;五大名字列表
输出：返回五大模块收集的值的列表，o_sys,o_namenode,o_FS,o_RPC(o表示output)
'''

'''
功能：判断模型是否需要更新,未来可能变为基于反馈的更新机制
更新条件：缓存达到了最大值
输入：size,缓冲区大小
'''
def detectUpdate(size, maxContainSize):
    if size >= maxContainSize:
        return True
    return False
'''
功能：为取出的所有元素按照decay赋予权重
返回：各个样本的权重
'''
def sampleDecay(sampletime,alpha,samplesize):
    k = len(sampletime)
    T = sampletime[0]#取出最晚的时间
    temp = 0.0
    for i in range(0,k):
        temp += 1.0/(1.0+alpha*(T-sampletime[i]))
    z = 1.0/temp#正则化因子，保证各个样本权重总和为1
    weight = []
    for i in range(0,k):
        weight.append(z/(1.0+alpha*(T-sampletime[i])))
    return  weight
'''
功能：按照概览返回采样数据
返回：采样的数据
'''
def sampleWithDecay(client,samplesum ,q):
    result = client.query(q, chunked=False)
    data = result['ganglia']
    dbtime = data.index
    keylist = []
    for i in range(0,len(dbtime)):
        t = dbtime[i]
        temp = str(t).split('.')
        smallt= temp[0]
        a = time.mktime(time.strptime(str(smallt), '%Y-%m-%d %H:%M:%S'))
        keylist.append(a)
    weight = sampleDecay(keylist,0.0001,samplesum)
    return  data.sample(n = samplesum, weights = weight)

def updateWindow(l_sys, l_namenode, l_FS, l_RPC,cont,limit):
    ilf = IsolationForest(n_estimators=100, contamination=cont)
    client = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='testdb')
    #取数据
    data_sys = sampleWithDecay(client,limit,'select * from ganglia where w_system >0 ORDER BY time DESC')
    d_sys = data_sys[l_sys]

    data_fs = sampleWithDecay(client, limit, 'select * from ganglia where w_fs >0 ORDER BY time DESC')
    d_FS = data_fs[l_FS]

    data_namenode = sampleWithDecay(client, limit, 'select * from ganglia where w_namenode >0 ORDER BY time DESC')
    d_namenode = data_namenode[l_namenode]

    data_rpc = sampleWithDecay(client, limit, 'select * from ganglia where w_rpc >0 ORDER BY time DESC')
    d_RPC = data_rpc[l_RPC]

    ilf_sys = IsolationForest(n_estimators=100, contamination=cont)
    ilf_namenode = IsolationForest(n_estimators=100, contamination=cont)
    ilf_FS = IsolationForest(n_estimators=100, contamination=cont)
    ilf_RPC = IsolationForest(n_estimators=100, contamination=cont)
    #适应数据，训练模型
    ilf_sys.fit(d_sys)
    ilf_namenode.fit(d_namenode)
    ilf_FS.fit(d_FS)
    ilf_RPC.fit(d_RPC)

    print "update finished"
    return ilf_sys,ilf_namenode,ilf_FS,ilf_RPC
'''
功能：初始化
参数：cont为隔离森林中评估数据异常率的重要参数
'''
def init(l_sys, l_namenode, l_FS, l_RPC, sleeptime=15, cont=0.01,limit = 300):
    # 创建探测器
    ilf_sys = IsolationForest(n_estimators=100, contamination=cont)
    ilf_namenode = IsolationForest(n_estimators=100, contamination=cont)
    ilf_FS = IsolationForest(n_estimators=50, contamination=cont)
    ilf_RPC = IsolationForest(n_estimators=100, contamination=cont)
    #加入历史数据综合分析
    client = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='testdb')

    data_sys = sampleWithDecay(client, limit, 'select * from ganglia where w_system >0 ORDER BY time DESC')
    d_sys = data_sys[l_sys]

    data_fs = sampleWithDecay(client, limit, 'select * from ganglia where w_fs >0 ORDER BY time DESC')
    d_FS = data_fs[l_FS]

    data_namenode = sampleWithDecay(client, limit, 'select * from ganglia where w_namenode >0 ORDER BY time DESC')
    d_namenode = data_namenode[l_namenode]

    data_rpc = sampleWithDecay(client, limit, 'select * from ganglia where w_rpc >0 ORDER BY time DESC')
    d_RPC = data_rpc[l_RPC]


    print len(d_sys)
    print len(d_FS)
    print len(d_namenode)
    print len(d_RPC)
    # 分别fit
    ilf_sys.fit(d_sys)
    ilf_namenode.fit(d_namenode)
    ilf_FS.fit(d_FS)
    ilf_RPC.fit(d_RPC)

    print ilf_FS.predict(d_FS)

    return ilf_sys, ilf_namenode, ilf_FS, ilf_RPC


def  extract(dsys,dnamenode,dfs,drpc,i):
    print "extract:",i
    o_sys = dsys.ix[i]
    o_namenode = dnamenode.ix[i]
    o_FS = dfs.ix[i]
    o_RPC =drpc.ix[i]
    return  o_sys, o_namenode, o_FS, o_RPC
def falsedata(l_sys, l_namenode, l_FS, l_RPC):
    sys =pd.read_csv('system.csv')
    namenode = pd.read_csv('namenode.csv')
    fs = pd.read_csv('fs.csv')
    rpc = pd.read_csv('rpc.csv')

    print l_sys
    dsys = sys[l_sys]
    dnamenode = namenode[l_namenode]
    dfs = fs[l_FS]
    drpc = rpc[l_RPC]
    '''
    dsys = sys.ix[:,1:][l_sys]
    print dsys
    dnamenode = namenode.ix[:,1:][l_namenode]
    dfs = fs.ix[:,1:][l_FS]
    drpc = rpc.ix[:,1:][l_RPC]
    '''

    return  dsys, dnamenode, dfs, drpc
def score(score_sys,score_namenode,score_fs,score_rpc):
    if score_sys > 1.0:
        score_sys = 1.0
    if score_namenode >1.0:
        score_namenode = 1.0
    if score_fs > 1.0:
        score_fs = 1.0
    if score_rpc >1.0:
        score_rpc = 1.0
    #底线
    if score_sys < -1.0:
        score_sys = -1.0
    if score_rpc < -1.0:
        score_rpc = -1.0
    if score_fs < -1.0:
        score_fs = -1.0
    if score_namenode < -1.0:
        score_namenode = -1.0
    return score_sys,score_namenode,score_fs,score_rpc

def online_detect(sleeptime=15, winsize=100, cont=0.01, warnfactor=20,limit = 300):
    maxContainSize = 200#触发更新机制的常量
    d = {}#存储metric和相应值的字典
    size = 0
    #原因分析触发模块计数变量
    label_namenode = 0
    label_sys = 0
    label_FS = 0
    label_RPC = 0
    #初始化数值
    score_sys = 1.0
    score_namenode = 1.0
    score_fs = 1.0
    score_rpc = 1.0
    # 初始化
    l_sys, l_namenode, l_FS, l_RPC = init_white()#得到各个模块的名字列表
    dwhite = dir_white(l_sys, l_namenode, l_FS, l_RPC)#初始化白名单
    ilf_sys, ilf_namenode, ilf_FS, ilf_RPC = init(l_sys, l_namenode, l_FS, l_RPC, sleeptime, cont,limit)
    print "initial finished"
    counter = 0
    print l_FS
    dsys, dnamenode, dfs, dprc = falsedata(l_sys, l_namenode, l_FS, l_RPC)
    while True:
        size += 1#触发更新
        print "fetching at %s" % ctime()

        o_sys, o_namenode, o_FS, o_RPC = extract(dsys,dnamenode,dfs,dprc,counter)
        counter += 1
        print counter+1
        if counter %70 == 0:
            counter = 0
        # 预测 p_predict之意
        #print o_sys

        p_sys = ilf_sys.predict(o_sys.values.reshape(1, -1))
        p_namenode = ilf_namenode.predict(o_namenode.values.reshape(1, -1))
        p_FS = ilf_FS.predict(o_FS.values.reshape(1, -1))
        p_RPC = ilf_RPC.predict(o_RPC.values.reshape(1, -1))
        # 输出结果
        print "system :", p_sys
        print "namenode :", p_namenode
        print "hadoop file system :", p_FS
        print "hadoop remote process call :", p_RPC
        d['w_system'] = int(p_sys)
        d['w_namenode'] = int(p_namenode)
        d['w_fs'] = int(p_FS)
        d['w_rpc'] = int(p_RPC)
        if int(p_sys) == -1:
            label_sys += 1
        if int(p_RPC) == -1:
            label_RPC += 1
        if int(p_namenode) == -1:
            label_namenode += 1
        if int(p_FS) == -1:
            label_FS +=1
        #改变得分
        score_sys += int(p_sys)*0.2
        score_namenode += int(p_namenode)*0.2
        score_fs += int(p_FS)*0.2
        score_rpc += int(p_RPC)*0.2

        print score_sys, ' , ',score_namenode,  ' , ',score_fs, ' , ',score_rpc
        score_sys, score_namenode, score_fs, score_rpc= score(score_sys,score_namenode,score_fs,score_rpc)
        if warn_analyse(label_sys,warnfactor):
            print "system :"
            stemp = analyseWarn(l_sys,'w_system')
            if stemp != "":
                d['a_system'] = stemp
            label_sys = 0

        if warn_analyse(label_namenode,warnfactor):
            print "namenode :"
            stemp = analyseWarn(l_namenode,'w_namenode')
            if stemp != "":
                d['a_namenode'] = stemp
            label_namenode =0


        if warn_analyse(label_FS,warnfactor):
            print "hadoop file system :"
            stemp = analyseWarn(l_FS,'w_fs')
            if stemp != "":
                d['a_fs'] = stemp
            label_FS = 0

        if warn_analyse(label_RPC,warnfactor):
            print "hadoop remote process call :"
            stemp = analyseWarn(l_RPC,'w_rpc')
            if stemp != "":
                d['a_rpc'] = stemp
            label_RPC = 0

        d['flasedb'] = 1#标记是仿真合成的数据
        d['score_sys'] = score_sys
        d['score_namenode'] = score_namenode
        d['score_fs'] = score_fs
        d['score_rpc'] = score_rpc

        for i in l_sys:
            d[i] = o_sys[i]
        for i in l_RPC:
            d[i] = o_RPC[i]
        for i in l_namenode:
            d[i] = o_namenode[i]

        writeToTSDB(d)
        if 'a_system' in d:
            del d['a_system']
        if 'a_namenode' in d:
            del d['a_namenode']
        if 'a_fs' in d:
            del d['a_fs']
        if 'a_rpc' in d:
            del d['a_rpc']
        # 判断更新
        if detectUpdate(size, maxContainSize):
            del ilf_sys
            del ilf_namenode
            del ilf_FS
            del ilf_RPC
            # 更新迭代器
            ilf_sys ,ilf_namenode,ilf_FS,ilf_RPC= updateWindow(l_sys, l_namenode, l_FS, l_RPC,cont,limit)
            size = 0#触发机制
            label_namenode = 0
            label_sys = 0
            label_FS = 0
            label_RPC = 0
        sleep(sleeptime)


if __name__ == "__main__":
    #online_detect(sleeptime=15, winsize=100, cont=0.01, warnfactor=20,limit = 300)
    online_detect(2, 10, 0.12, 5)

#online_detect(sleeptime=15, winsize=100, cont=0.01, warnfactor=20,limit = 300):
