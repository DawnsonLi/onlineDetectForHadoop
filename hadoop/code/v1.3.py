# -*- coding:utf-8 -*-
'''
版本V1.3
该版本完全抛弃滑动窗口的概念，更新采用一定的反馈机制，从而更大限度的减小内存缓冲区的开辟
数据库存储采用时间序列数据库influxDB,web UI采用grafana
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
def analyseWarn(name,qname,topk=5):
    client = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='testdb')
    query_positive = 'select * from ganglia where '+qname+' >0 ORDER BY time DESC limit 10'
    query_negative = 'select * from ganglia where '+qname+' <0 ORDER BY time DESC limit 5'

    data_p = client.query(query_positive, chunked=False)
    data_positive = data_p['ganglia']
    normalSample = data_positive[name]
    data_n = client.query(query_negative, chunked=False)
    data_negative = data_n['ganglia']
    anamolySample = data_negative[name]
    return analyseReasonWithTreeBaesd(anamolySample, normalSample, name)
def writeToTSDB(d):
    json_body = [
        {
            "measurement": "ganglia",
            "fields": d
        }
    ]

    client = InfluxDBClient('localhost', 8086, 'root', '', 'testdb')  # 初始化（指定要操作的数据库）
    client.write_points(json_body)  # 写入数据
'''
分析异常产生的原因与哪几个变量关系最密切。当前使用卡方检验进行分析
'''
def analyseReasonWithXsqure(anamolySample, normalSample, topk, name):
    data = anamolySample
    target = []
    for i in range(0, len(anamolySample)):
        target.append(1)
    data.extend(normalSample)
    for i in range(0, len(normalSample)):
        target.append(0)

    X_new = SelectKBest(chi2, topk).fit(data, target)
    outcome = X_new.get_support()
    for i in range(0, len(name)):
        if outcome[i]:
            print name[i]
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
def loadvalue(data, d, dwhite):
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
            if str(ksmall) == 'host':
                if d2[ksmall] == "slave04" or d2[ksmall] == "localhost":
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
'''
输入：字典d,d中保留了metrics和对应的瞬时值;五大名字列表
输出：返回五大模块收集的值的列表，o_sys,o_namenode,o_FS,o_RPC(o表示output)
'''
def extract(d, l_sys, l_namenode, l_FS, l_RPC):
    # 返回的各个结果值列表
    o_sys = []
    o_namenode = []
    o_FS = []
    o_RPC = []

    for i in l_sys:
        o_sys.append(float(d[i]))
    for i in l_namenode:
        o_namenode.append(float(d[i]))
    for i in l_FS:
        o_FS.append(float(d[i]))
    for i in l_RPC:
        o_RPC.append(float(d[i]))
    return o_sys, o_namenode, o_FS, o_RPC
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
    print z
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
def init(l_sys, l_namenode, l_FS, l_RPC, d, dwhite, winsize=200, sleeptime=15, cont=0.01,limit = 300):
    win_sys = []
    win_namenode = []
    win_FS = []
    win_RPC = []
    while True:
        print "fetching at %s" % ctime()
        data = getdata()
        loadvalue(data, d, dwhite)
        o_sys, o_namenode, o_FS, o_RPC = extract(d, l_sys, l_namenode, l_FS, l_RPC)
        # 分别加入到各自的窗口
        win_sys.append(o_sys)
        win_namenode.append(o_namenode)
        win_FS.append(o_FS)
        win_RPC.append(o_RPC)
        if len(win_sys) > winsize:  # 因为每个窗口的大小都相同
            break
        sleep(sleeptime)
    # 创建探测器
    ilf_sys = IsolationForest(n_estimators=100, contamination=cont)
    ilf_namenode = IsolationForest(n_estimators=100, contamination=cont)
    ilf_FS = IsolationForest(n_estimators=100, contamination=cont)
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

    #合并当前在线数据
    append_sys = pd.DataFrame(win_sys,columns=l_sys)
    append_namenode = pd.DataFrame(win_namenode, columns=l_namenode)
    append_FS = pd.DataFrame(win_FS, columns=l_FS)
    append_RPC = pd.DataFrame(win_RPC, columns=l_RPC)

    out_sys = pd.concat([d_sys,append_sys])
    out_namenode = pd.concat([d_namenode,append_namenode])
    out_FS = pd.concat([d_FS,append_FS])
    out_RPC = pd.concat([d_RPC,append_RPC])
    # 分别fit
    ilf_sys.fit(out_sys)
    ilf_namenode.fit(out_namenode)
    ilf_FS.fit(out_FS)
    ilf_RPC.fit(out_RPC)

    print ilf_sys.predict(win_sys)
    print ilf_namenode.predict(win_namenode)
    print ilf_FS.predict(win_FS)
    print ilf_RPC.predict(win_RPC)

    return ilf_sys, ilf_namenode, ilf_FS, ilf_RPC
def online_detect(sleeptime=15, winsize=100, cont=0.01, warnfactor=20,limit = 300):
    maxContainSize = 100#触发更新机制的常量
    d = {}#存储metric和相应值的字典
    size = 0
    #原因分析触发模块计数变量
    label_namenode = 0
    label_sys = 0
    label_FS = 0
    label_RPC = 0
    # 初始化
    l_sys, l_namenode, l_FS, l_RPC = init_white()#得到各个模块的名字列表
    dwhite = dir_white(l_sys, l_namenode, l_FS, l_RPC)#初始化白名单
    ilf_sys, ilf_namenode, ilf_FS, ilf_RPC = init(l_sys, l_namenode, l_FS, l_RPC, d, dwhite, winsize, sleeptime, cont,limit)
    print "initial finished"

    while True:
        size += 1#触发更新
        print "fetching at %s" % ctime()
        data = getdata()
        loadvalue(data, d, dwhite)
        o_sys, o_namenode, o_FS, o_RPC = extract(d, l_sys, l_namenode, l_FS, l_RPC)
        # reshape从而编程1*x列向量用于预测
        v_sys = np.array(o_sys).reshape(1, -1)
        v_namenode = np.array(o_namenode).reshape(1, -1)
        v_FS = np.array(o_FS).reshape(1, -1)
        v_RPC = np.array(o_RPC).reshape(1, -1)
        # 预测 p_predict之意
        p_sys = ilf_sys.predict(v_sys)
        p_namenode = ilf_namenode.predict(v_namenode)
        p_FS = ilf_FS.predict(v_FS)
        p_RPC = ilf_RPC.predict(v_RPC)
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
    online_detect(5, 10, 0.00000012, 5)

