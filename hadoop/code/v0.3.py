#-*- coding:utf-8 -*-
'''
版本V0.3
该版本延续了V0.2
增加功能：
(1)增加了系统异常快照框架
        记录异常点时系统各个参数的值，资源的状态信息,使用全部metrcis
(2）系统繁忙程度概览
        显示在异常点附近的作业状态，帮助用户分析系统繁忙程度，内部使用Queue metrics
(3) 近期完成作业状态
        显示近期时间段（用户定义）完成作业的情况，协助用户分析，内容使用history server API
'''
import pandas as pd
from sklearn.ensemble import IsolationForest
from time import ctime,sleep
import requests
import numpy as np
from sklearn import tree
import pydotplus 
import time
from datetime import datetime
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.feature_selection import SelectFromModel

'''
功能：分析当前是否需要警告并分析原因
实现：当数组中-1的个数达到warnfactor时报警
输入：label,某个模块中的label列表；warnfactor，警告因子，达到个数后进行原因分析,ratio,全部样本和警告因子的比例，用于控制正负样本的平衡
返回：true/false
'''
def  warn_analyse(label,warnfactor,ratio = 2):
    if len(label)/ratio < warnfactor:
        return False
    if label.count(-1) >= warnfactor :
        return True
    return False
'''
功能：根据标签label,和当前缓存buf,创建训练样本,使用逆序遍历，是为了利用时间的局部性
输入：name属性名称，topk给出的原因个数
调用：具体的分析方法
'''
def analyseWarn(buf,lable,name,topk=5):
    
    #构建足够的训练样例，以防正样例不足
    train_data = buf#如果window size过大，只需使用window[700:]这样的切片
    #构建训练样本,此处要注意样本不平衡问题
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
        bufReminLen -=1
    if len(anamolySample) <1 or len(normalSample)< 1:
        print "sample is not enough"
        return
    #分析原因
    print "begin to analyse the reason,with ",len(anamolySample)," negative samples and ",len(normalSample)," normal sample"
    #analyseReasonWithDecisonTree(anamolySample,normalSample,name)
    analyseReasonWithTreeBaesd(anamolySample, normalSample,name)
   

'''
分析异常产生的原因。当前使用DecisonTree进行分析
'''
def analyseReasonWithDecisonTree(anamolySample,normalSample,name):
    data = anamolySample
    target = []
    for i in range(0,len(anamolySample)):
        target.append(1)
    data.extend(normalSample)
    for i in range(0,len(normalSample)):
        target.append(0)
        
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(data,target)
    
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
    data.extend(normalSample)
    for i in range(0,len(normalSample)):
        target.append(0)
        
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
    data.extend(normalSample)
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

'''
v3.0 系统异常快照框架
在此版本中只给出框架，依赖未来实现，比如后端数据库可供支持
'''
def snapshot():
   pass#在此只是实现一个框架，这里可以拓展的东西很多

'''
v3.0 系统排队程度概览
输入：字典d
'''
def QueueInfo(d):
    print "recent APP info :"
    print "________________"
    print "Active Applications:",d['yarn.QueueMetrics.ActiveApplications']
    print "AppsCompleted:",d['yarn.QueueMetrics.AppsCompleted']
    print "AppsFailed:",d['yarn.QueueMetrics.AppsFailed']
    print "AppsKilled:",d['yarn.QueueMetrics.AppsKilled']
    print "AppsRunning",d['yarn.QueueMetrics.AppsRunning']
    print "AppsPending",d['yarn.QueueMetrics.AppsPending']
    print "________________"
    print "ActiveUsers:",d['yarn.QueueMetrics.ActiveUsers']
    print "________________"
    print "Current number of running applications info: "
    print "Current number of running applications whose elapsed time are less than 60 minutes",d['yarn.QueueMetrics.running_0']
    print "Current number of running applications whose elapsed time are between 60 and 300 minutes",d['yarn.QueueMetrics.running_60']
    print "Current number of running applications whose elapsed time are between 300 and 1440 minutes",d['yarn.QueueMetrics.running_300']
    print "Current number of running applications elapsed time are more than 1440 minutes",d['yarn.QueueMetrics.running_1440']
    print "________________"

'''
v0.3 近期完成作业状态
显示近期时间段（用户定义）完成作业的情况，协助用户分析，内容使用history server API
这是一个与其他模块无关的报表功能
输入：用户指定时间起点 startpoint
返回：每个在起始时间在 指定时间点startpoint之后的Job的详细信息
'''
def recentHistory(startpoint):
    cs_url = 'http://192.168.1.102:19888/ws/v1/history/mapreduce/jobs?startedTimeBegin'+str(startpoint)
    cs_user = 'dbcluster'
    cs_psw  = '1'        
    r = requests.get(cs_url, auth=(cs_user, cs_psw))
    data = r.json()
    jobid_list = []#存储jobid的列表
    for key in data:
        jobs = key
        djobs = data[jobs]
        for job in djobs:
            djob = djobs[job]
            for k in djob:
                jobid = k['id']
                jobid_list.append(jobid)
    print jobid_list
    
    url = 'http://192.168.1.102:19888/ws/v1/history/mapreduce/jobs/'
    for i in jobid_list:
        url += i
        r = requests.get(cs_url, auth=(cs_user, cs_psw))
        data = r.json()
        #print data
        djob = data['jobs']
        jobinfo = djob['job'][0]
        
        for i in jobinfo:
            print i," : ",jobinfo[i]
        print "___________________"
        '''
                    使用参考
        t = time.time()
        t = '1491983546294'
        recentHistory(int(t)-100000)，当前时间段以前的一段时间
        '''
  
def online_detect(sleeptime = 15,winsize=100,cont = 0.01,warnfactor = 20):
   
    maxContainSize = winsize
    
    #存储metric和相应值的字典
    d = {}
    #5个模块的缓存
    buf_sys = []
    buf_namenode = []
    buf_FS = []
    buf_RPC =[]
    buf_queue = []
    #实时预测的buf中的结果
    label_sys = []
    label_namenode = []
    label_FS = []
    label_RPC = []
    label_queue = []
    #为每一个label列表创建一个warnfactor,从而使得一次报警之后，再达到warnfactor个才能报警
    w_sys = warnfactor
    w_namenode = warnfactor
    w_FS = warnfactor
    w_RPC =warnfactor
    w_queue =warnfactor
    #初始化
    l_sys,l_namenode,l_FS,l_RPC,l_queue = init_white()
    dwhite = dir_white(l_sys, l_namenode, l_FS, l_RPC, l_queue)
    ilf_sys,ilf_namenode,ilf_FS,ilf_queue,ilf_RPC = init(l_sys,l_namenode,l_FS,l_RPC,l_queue,d,dwhite,winsize, sleeptime,cont)
    print "initial finished"
    
    
    while True:
        print "fetching at %s" %ctime()
        now = datetime.now()#存储的时间对象
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
        
        #预测 p_predict之意
        p_sys = ilf_sys.predict(v_sys)
        p_namenode = ilf_namenode.predict(v_namenode)
        p_FS = ilf_FS.predict(v_FS)
        p_RPC = ilf_RPC.predict(v_RPC)
        P_queue = ilf_queue.predict(v_queue)
        #保留结果
        label_sys.append(int(p_sys))
        label_namenode.append(int(p_namenode))
        label_FS.append(int(p_FS))
        label_RPC.append(int(p_RPC))
        label_queue.append(int(P_queue))
        
        #如果有-1出现，显示当前队列拥挤程度信息，v0.3功能
        if int(p_sys)== -1 or int(p_namenode) == -1 or int(p_FS) == -1 or int(p_RPC) == -1 or int(P_queue)==-1:
            QueueInfo(d)
        #输出结果
        print "system :",p_sys
        print "namenode :",p_namenode
        print "hadoop file system :",p_FS
        print "hadoop remote process call :",p_RPC
        print "hadoop queue metrics :",P_queue
        
        #判断原因分析
        #warn_analyse(label,warnfactor,ratio=0.5):
        if warn_analyse(label_sys,w_sys):
            print "system :"
            analyseWarn(buf_sys,label_sys,l_sys)
            w_sys += warnfactor
            
        if warn_analyse(label_namenode,w_namenode):
            print "namenode :"
            analyseWarn(buf_namenode,label_namenode,l_namenode)
            w_namenode += warnfactor
            
        if warn_analyse(label_FS,w_FS):
            print "hadoop file system :"
            analyseWarn(buf_FS,label_FS,l_FS)
            w_FS += warnfactor
            
        if warn_analyse(label_RPC,w_RPC):
            print "hadoop remote process call :"
            analyseWarn(buf_RPC,label_RPC,l_RPC)
            w_RPC += warnfactor
            
        if warn_analyse(label_queue,w_queue):
            print "hadoop queue :"
            analyseWarn(buf_queue,label_queue,l_queue)
            w_queue += warnfactor
        #判断更新     
        if detectUpdate(buf_FS, maxContainSize):
            del ilf_sys
            del ilf_namenode
            del ilf_FS
            del ilf_queue
            del ilf_RPC
            
            #更新迭代器
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
            #更新label
            label_sys = []
            label_namenode = []
            label_FS = []
            label_RPC = []
            label_queue = []
            #更新 warning factor
            w_sys = warnfactor
            w_namenode = warnfactor
            w_FS = warnfactor
            w_RPC =warnfactor
            w_queue =warnfactor
            
        sleep(sleeptime)

if __name__ == "__main__":
    online_detect(15,15,0.01,5)


