# -*- coding:utf-8 -*-
'''
版本V0.4
该版本延续了V0.3
该版本形成了独立且拥有完整架构的版本，去除了v0.3中部分展示功能，关注于异常发现与原因解释
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
功能：分析当前是否需要警告并分析原因
实现：当数组中-1的个数达到warnfactor时报警
输入：label,某个模块中的label列表；warnfactor，警告因子，达到个数后进行原因分析,ratio,全部样本和警告因子的比例，用于控制正负样本的平衡
返回：true/false
'''


def warn_analyse(label, warnfactor, ratio=2):
    if len(label) / ratio < warnfactor:
        return False
    if label.count(-1) >= warnfactor:
        return True
    return False


'''
功能：根据标签label,和当前缓存buf,创建训练样本,使用逆序遍历，是为了利用时间的局部性
输入：name属性名称，topk给出的原因个数
调用：具体的分析方法
'''


def analyseWarn(buf, lable, name, topk=5):
    # 构建足够的训练样例，以防正样例不足
    train_data = buf  # 如果window size过大，只需使用window[700:]这样的切片
    # 构建训练样本,此处要注意样本不平衡问题
    anamolySample = []
    normalSample = []
    index = -1
    bufReminLen = len(train_data)
    while bufReminLen > 0:
        if lable[index] == 1:
            normalSample.append(train_data[index])
        else:
            anamolySample.append(train_data[index])
        index -= 1
        bufReminLen -= 1
    if len(anamolySample) < 1 or len(normalSample) < 1:
        print "sample is not enough"
        return
    # 分析原因
    print "begin to analyse the reason,with ", len(anamolySample), " negative samples and ", len(
        normalSample), " normal sample"
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
    data = anamolySample
    target = []
    for i in range(0, len(anamolySample)):
        target.append(1)
    data.extend(normalSample)
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
功能：判断模型是否需要更新
更新条件：缓存达到了最大值
输入：buf,缓冲区大小
'''


def detectUpdate(buf, maxContainSize):
    if len(buf) >= maxContainSize:
        return True
    return False


'''
输入：新窗口，也就是缓冲区
输出：新探测隔离树

'''


def updateWindow(l_sys, l_namenode, l_FS, l_RPC,cont):
    ilf = IsolationForest(n_estimators=100, contamination=cont)
    query = 'select * from ganglia where w_fs >0 and w_namenode>0 and w_rpc >0 limit 1024;'  # 筛选条件 可设置
    client = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='testdb')
    result = client.query(query, chunked=False)
    data = result['ganglia']
    d_sys = data[l_sys]
    d_namenode = data[l_namenode]
    d_FS = data[l_FS]
    d_RPC = data[l_RPC]

    ilf_sys = IsolationForest(n_estimators=100, contamination=cont)
    ilf_namenode = IsolationForest(n_estimators=100, contamination=cont)
    ilf_FS = IsolationForest(n_estimators=100, contamination=cont)
    ilf_RPC = IsolationForest(n_estimators=100, contamination=cont)

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


def init(l_sys, l_namenode, l_FS, l_RPC, d, dwhite, winsize=200, sleeptime=15, cont=0.01):
    # 创建窗口
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
    query = 'select * from ganglia where w_fs >0 and w_namenode>0 and w_rpc >0 limit 256;' #筛选条件 可设置
    client = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='testdb')
    result = client.query(query, chunked=False)
    data = result['ganglia']
    d_sys = data[l_sys]
    d_namenode = data[l_namenode]
    d_FS = data[l_FS]
    d_RPC = data[l_RPC]
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

    #print out_sys

    print ilf_sys.predict(win_sys)
    print ilf_namenode.predict(win_namenode)
    print ilf_FS.predict(win_FS)
    print ilf_RPC.predict(win_RPC)

    # 返回探测器，此时不用返回窗口
    return ilf_sys, ilf_namenode, ilf_FS, ilf_RPC


def online_detect(sleeptime=15, winsize=100, cont=0.01, warnfactor=20):
    maxContainSize = winsize
    # 存储metric和相应值的字典
    d = {}
    # 5个模块的缓存
    buf_sys = []
    buf_namenode = []
    buf_FS = []
    buf_RPC = []
    # 实时预测的buf中的结果
    label_sys = []
    label_namenode = []
    label_FS = []
    label_RPC = []
    # 为每一个label列表创建一个warnfactor,从而使得一次报警之后，再达到warnfactor个才能报警
    w_sys = warnfactor
    w_namenode = warnfactor
    w_FS = warnfactor
    w_RPC = warnfactor
    # 初始化
    l_sys, l_namenode, l_FS, l_RPC = init_white()
    dwhite = dir_white(l_sys, l_namenode, l_FS, l_RPC)
    ilf_sys, ilf_namenode, ilf_FS, ilf_RPC = init(l_sys, l_namenode, l_FS, l_RPC, d, dwhite, winsize, sleeptime, cont)
    print "initial finished"

    while True:
        print "fetching at %s" % ctime()
        data = getdata()
        loadvalue(data, d, dwhite)
        o_sys, o_namenode, o_FS, o_RPC = extract(d, l_sys, l_namenode, l_FS, l_RPC)
        # print len(o_sys),len(o_namenode),len(o_FS),len(o_RPC),len(o_queue)
        # 分别添加到缓冲区
        buf_sys.append(o_sys)
        buf_namenode.append(o_namenode)
        buf_FS.append(o_FS)
        buf_RPC.append(o_RPC)

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

        # 保留结果
        label_sys.append(int(p_sys))
        label_namenode.append(int(p_namenode))
        label_FS.append(int(p_FS))
        label_RPC.append(int(p_RPC))

        # 如果有-1出现，显示当前队列拥挤程度信息，v0.3功能
        # if int(p_sys)== -1 or int(p_namenode) == -1 or int(p_FS) == -1 or int(p_RPC) == -1 or int(P_queue)==-1:
        #   QueueInfo(d)
        # 输出结果
        print "system :", p_sys
        print "namenode :", p_namenode
        print "hadoop file system :", p_FS
        print "hadoop remote process call :", p_RPC
        d['w_system'] = int(p_sys)
        d['w_namenode'] = int(p_namenode)
        d['w_fs'] = int(p_FS)
        d['w_rpc'] = int(p_RPC)

        # 判断原因分析
        # warn_analyse(label,warnfactor,ratio=0.5):
        if warn_analyse(label_sys, w_sys):
            print "system :"
            stemp = analyseWarn(buf_sys, label_sys, l_sys)
            if stemp != "":
                d['a_system'] = stemp
            w_sys += warnfactor

        if warn_analyse(label_namenode, w_namenode):
            print "namenode :"
            stemp = analyseWarn(buf_namenode, label_namenode, l_namenode)
            if stemp != "":
                d['a_namenode'] = stemp
            w_namenode += warnfactor

        if warn_analyse(label_FS, w_FS):
            print "hadoop file system :"
            stemp = analyseWarn(buf_FS, label_FS, l_FS)
            if stemp != "":
                d['a_fs'] = stemp
            w_FS += warnfactor

        if warn_analyse(label_RPC, w_RPC):
            print "hadoop remote process call :"
            stemp = analyseWarn(buf_RPC, label_RPC, l_RPC)
            if stemp != "":
                d['a_rpc'] = stemp
            w_RPC += warnfactor

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
        if detectUpdate(buf_FS, maxContainSize):

            del ilf_sys
            del ilf_namenode
            del ilf_FS
            del ilf_RPC

            # 更新迭代器
            ilf_sys ,ilf_namenode,ilf_FS,ilf_RPC= updateWindow(l_sys, l_namenode, l_FS, l_RPC,cont)
            # 清空操作
            del buf_FS
            del buf_sys
            del buf_RPC
            del buf_namenode

            buf_sys = []
            buf_namenode = []
            buf_FS = []
            buf_RPC = []

            # 更新label
            label_sys = []
            label_namenode = []
            label_FS = []
            label_RPC = []

            # 更新 warning factor
            w_sys = warnfactor
            w_namenode = warnfactor
            w_FS = warnfactor
            w_RPC = warnfactor

        sleep(sleeptime)


if __name__ == "__main__":
    online_detect(15, 10, 0.00000012, 3)

