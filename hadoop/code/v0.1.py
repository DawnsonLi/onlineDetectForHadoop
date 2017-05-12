#-*- coding:utf-8 -*-
'''
版本V0.1
该版本延续了V0.0：采用固定大小的窗口，缓冲区满进行淘汰,不使用可变窗口技巧，只关注异常的探测，不进行原因分析和解释
独特功能：
（1）参数白名单：
        分别从几个文件中读取初始的参数，将v0.1中对于运行的依赖消除
(2)五大模块分别预警，参数个数减小，也有利于针对不同模块进行预警
V0.0区别：
不再使用冗余的id,去除了重名的Metrics，直接用metrics作为id，不再需要黑名单
'''
import pandas as pd
from sklearn.ensemble import IsolationForest
from time import ctime,sleep
import requests
import numpy as np
    
'''
功能：初始化白名单
返回：l_sys,l_namenode,l_FS,l_RPC,l_queue 五大模块参数列表
'''  
def init_white():
    #分别加载各个文件
    queue = '../hadoop/queue.csv' 
    data = pd.read_csv(queue, delimiter=',')
    l_queue = []
    for i in  data.columns:
        l_queue.append(i)
        
    FS ='../hadoop/FS.csv'
    data = pd.read_csv(FS, delimiter=',')
    l_FS = []
    for i in  data.columns:
        l_FS.append(i)
    
    namenode ='../hadoop/namenode.csv'
    data = pd.read_csv(namenode, delimiter=',')
    l_namenode = []
    for i in  data.columns:
        l_namenode.append(i)
    
    RPC ='../hadoop/RPC.csv'
    data = pd.read_csv(RPC, delimiter=',')
    l_RPC = []
    for i in  data.columns:
        l_RPC.append(i)
        
    sys ='../hadoop/system.csv'
    data = pd.read_csv(sys, delimiter=',')
    l_sys = []
    for i in  data.columns:
        l_sys.append(i)
        
    return l_sys,l_namenode,l_FS,l_RPC,l_queue
   
'''
构建白名单字典，便于快速过滤
'''
def dir_white(l_sys,l_namenode,l_FS,l_RPC,l_queue):
    dwhite = {}
    for i in l_sys:
        dwhite[i] = 1
    for i in l_namenode:
        dwhite[i] = 1
    for i in l_FS:
        dwhite[i] = 1
    for i in l_RPC:
        dwhite[i] = 1
    for i in l_queue:
        dwhite[i] = 1
    return dwhite
    
def loadvalue(data,d,dwhite):
    d1 =  data['metrics']
    for k in d1:
        d2 = k
        for ksmall in d2:
            if str(ksmall) == 'metric':
                key = str(d2[ksmall])
            if str(ksmall) =='value':
                value = d2[ksmall]
        if key in dwhite:     
            d[key] = value
   
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
输入：字典d,d中保留了metrics和对应的瞬时值;五大名字列表
输出：返回五大模块收集的值的列表，o_sys,o_namenode,o_FS,o_RPC,o_queue (o表示output)
'''
def extract(d,l_sys,l_namenode,l_FS,l_RPC,l_queue):
    #返回的各个结果值列表
    o_sys = []
    o_namenode = []
    o_FS = []
    o_RPC = []
    o_queue = []
    
    for i in l_sys:
        o_sys.append(float(d[i]))
    for i in l_namenode:
        o_namenode.append(float(d[i]))
    for i in l_FS:
        o_FS.append(float(d[i]))
    for i in l_RPC:
        o_RPC.append(float(d[i]))
    for i in l_queue:
        o_queue.append(float(d[i]))   
    return o_sys,o_namenode,o_FS,o_RPC,o_queue    
        
    
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
def updateWindow(buf,cont):

    ilf = IsolationForest(n_estimators=100,contamination=cont)
    ilf.fit(buf)#新的buf就是新的窗口
    print "isolation update finished"
    return ilf

'''
功能：初始化
参数：cont为隔离森林中评估数据异常率的重要参数
'''
def init(l_sys,l_namenode,l_FS,l_RPC,l_queue,d,dwhite,winsize=200,sleeptime = 15,cont=0.01):
    #创建窗口
    win_sys =  []
    win_namenode = []
    win_FS = []
    win_RPC =[]
    win_queue = []
    
    while True:
        print "fetching at %s" %ctime()
        data = getdata()
        loadvalue(data, d,dwhite)
        o_sys,o_namenode,o_FS,o_RPC,o_queue  = extract(d,l_sys,l_namenode,l_FS,l_RPC,l_queue)
        #分别加入到各自的窗口
        win_sys.append(o_sys)
        win_namenode.append(o_namenode)
        win_FS.append(o_FS)
        win_RPC.append(o_RPC)
        win_queue.append(o_queue)
        
        if len(win_sys) > winsize:#因为每个窗口的大小都相同
            break
        sleep(sleeptime)
    #创建探测器
    ilf_sys = IsolationForest(n_estimators=100,contamination=cont)
    ilf_namenode = IsolationForest(n_estimators=100,contamination=cont)
    ilf_FS = IsolationForest(n_estimators=100,contamination=cont)
    ilf_RPC = IsolationForest(n_estimators=100,contamination=cont)
    ilf_queue = IsolationForest(n_estimators=100,contamination=cont)
    #分别fit
    ilf_sys.fit(win_sys)
    ilf_namenode.fit(win_namenode)
    ilf_FS.fit(win_FS)
    ilf_RPC.fit(win_RPC)
    ilf_queue.fit(win_queue)
    #返回探测器，此时不用返回窗口
    return ilf_sys,ilf_namenode,ilf_FS,ilf_queue,ilf_RPC


def online_detect(sleeptime = 15,winsize=100,cont = 0.01):
   
    maxContainSize = winsize
    
    d = {}
    buf_sys = []
    buf_namenode = []
    buf_FS = []
    buf_RPC =[]
    buf_queue = []
    l_sys,l_namenode,l_FS,l_RPC,l_queue = init_white()
    dwhite = dir_white(l_sys, l_namenode, l_FS, l_RPC, l_queue)
    ilf_sys,ilf_namenode,ilf_FS,ilf_queue,ilf_RPC = init(l_sys,l_namenode,l_FS,l_RPC,l_queue,d,dwhite,winsize, sleeptime,cont)
    print "initial finished"
    
    
    while True:
        print "fetching at %s" %ctime()
        data = getdata()
        loadvalue(data, d,dwhite)
        o_sys,o_namenode,o_FS,o_RPC,o_queue  = extract(d,l_sys,l_namenode,l_FS,l_RPC,l_queue)
        #print len(o_sys),len(o_namenode),len(o_FS),len(o_RPC),len(o_queue)
        #分别添加到缓冲区
        buf_sys.append(o_sys)
        buf_namenode.append(o_namenode)
        buf_FS.append(o_FS)
        buf_RPC.append(o_RPC)
        buf_queue.append(o_queue)
        
        #reshape从而编程1*x列向量用于预测
        v_sys = np.array(o_sys).reshape(1,-1)
        v_namenode = np.array(o_namenode).reshape(1,-1)
        v_FS =  np.array(o_FS).reshape(1,-1)
        v_RPC = np.array(o_RPC).reshape(1,-1)
        v_queue =np.array(o_queue).reshape(1,-1)
        
        #预测
        p_sys = ilf_sys.predict(v_sys)
        p_namenode = ilf_namenode.predict(v_namenode)
        p_FS = ilf_FS.predict(v_FS)
        p_RPC = ilf_RPC.predict(v_RPC)
        P_queue = ilf_queue.predict(v_queue)
        #输出结果
        print "system :",p_sys
        print "namenode :",p_namenode
        print "hadoop file system :",p_FS
        print "hadoop remote process call :",p_RPC
        print "hadoop queue metrics :",P_queue
        
        #判断更新     
        if detectUpdate(buf_FS, maxContainSize):
            del ilf_sys
            del ilf_namenode
            del ilf_FS
            del ilf_queue
            del ilf_RPC
            
           
            ilf_sys = updateWindow(buf_sys,cont)
            ilf_namenode = updateWindow(buf_namenode,cont)
            ilf_FS =updateWindow(buf_FS,cont)
            ilf_queue = updateWindow(buf_queue,cont)
            ilf_RPC = updateWindow(buf_RPC,cont)
            
            #清空操作
            del buf_FS
            del buf_sys
            del buf_RPC
            del buf_queue
            del buf_namenode
            
            buf_sys = []
            buf_namenode = []
            buf_FS = []
            buf_RPC =[]
            buf_queue = []
            
       
        sleep(sleeptime)
        
online_detect(15,5,0.01)


