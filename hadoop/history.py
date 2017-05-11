# -*- coding: utf-8 -*-
"""
Created on Thu May 11 21:16:40 2017

@author: dawnson
用来获取历史job的信息，整合之后可以提供给用户参考
"""

import requests
import json


cs_url = 'http://192.168.1.102:19888/ws/v1/history/mapreduce/jobs'
cs_user = 'dbcluster'
cs_psw  = '1'        
r = requests.get(cs_url, auth=(cs_user, cs_psw))
data = r.json()
for key in data:
    jobs = key
    djobs = data[jobs]
    for job in djobs:
        djob = djobs[job]
        for k in djob:
            print k

'''
example returns:
{u'finishTime': 1491966519178L, u'reducesCompleted': 1, u'name': u'word count', u'reducesTotal': 1, u'queue': u'default', u'submitTime': 1491966510921L, u'state': u'SUCCEEDED', u'mapsTotal': 0, u'user': u'dbcluster', u'startTime': 1491966514132L, u'id': u'job_1491918345048_0009', u'mapsCompleted': 0}
{u'finishTime': 1491973656387L, u'reducesCompleted': 2, u'name': u'word count', u'reducesTotal': 2, u'queue': u'default', u'submitTime': 1491973647239L, u'state': u'SUCCEEDED', u'mapsTotal': 0, u'user': u'dbcluster', u'startTime': 1491973650265L, u'id': u'job_1491918345048_0010', u'mapsCompleted': 0}
{u'finishTime': 1491973687981L, u'reducesCompleted': 1, u'name': u'word count', u'reducesTotal': 1, u'queue': u'default', u'submitTime': 1491973679565L, u'state': u'SUCCEEDED', u'mapsTotal': 0, u'user': u'dbcluster', u'startTime': 1491973682941L, u'id': u'job_1491918345048_0011', u'mapsCompleted': 0}
{u'finishTime': 1491983554565L, u'reducesCompleted': 1, u'name': u'word count', u'reducesTotal': 1, u'queue': u'default', u'submitTime': 1491983542705L, u'state': u'SUCCEEDED', u'mapsTotal': 1, u'user': u'dbcluster', u'startTime': 1491983546294L, u'id': u'job_1491982959684_0004', u'mapsCompleted': 1}
{u'finishTime': 1492051563567L, u'reducesCompleted': 1, u'name': u'word count', u'reducesTotal': 1, u'queue': u'default', u'submitTime': 1492051552756L, u'state': u'SUCCEEDED', u'mapsTotal': 1, u'user': u'dbcluster', u'startTime': 1492051555461L, u'id': u'job_1491982959684_0011', u'mapsCompleted': 1}
{u'finishTime': 1494404661115L, u'reducesCompleted': 1, u'name': u'word count', u'reducesTotal': 1, u'queue': u'default', u'submitTime': 1494404565606L, u'state': u'SUCCEEDED', u'mapsTotal': 11, u'user': u'dbcluster', u'startTime': 1494404573523L, u'id': u'job_1493719642412_0002', u'mapsCompleted': 11}
'''