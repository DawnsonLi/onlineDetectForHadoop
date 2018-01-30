# onlineDetectForHadoop
## hadoop metrics<br>
基于hadoop metrics,我们可以实时的获取包含hadoop系统上各个组件的metrics信息，这些metrics用于监控，表现调优和debug,并可用于troubleshooting<br>系统中异常变量的含义均保留使用hadoop metrics的名称<br>详情参见hadoop官方文档http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Metrics.html<br>
## 项目简介 <br>
 针对源源不断的metrics流，包括hadoop metrics和机器 metrics,我提出了一个新颖的方法用于hadoop系统的监控。v0.4版本之前的特点为：<br>(1)与基于阈值的监控方法相互结合，使用机器学习的方法<br>(2)在数据流模型上建立基于滑动窗口的探测模型，探测模型是无监督的，当前版本使用隔离森林作为探测器<br>(3)使用探测模型对数据的标签，用于进一步的原因分析（使用机器学习和统计模型）<br>(4)利用hadoop REST API体现hadoop Queue,history job的状态，协助分析<br>(5)对hadoop系统的不同组件，和使用ganglia收集而来的系统指标分别探测，定位更加精准<br>v0.4版本到1.x版本则发挥了数据库的优势，不再简单使用滑动窗口，结合数据库中的历史数据进行分析<br>
## 项目使用工具 <br>
使用ganglia API获取时间序列数据，并存储到时间序列数据库InfluxDB中，使用grafana进行可视化,使用Logstash和elasticsearch实现日志实时监控
## 项目延伸意义 <br>
给出了难以对数据进行实时异常分类问题的解决思路，提出基于时间序列进行异常探测及原因解释的整体框架，具有很强的延伸价值，比如数据安全等
## 致谢 <br>
感谢github开源工具ganglia API、InfluxDB、grafana等<br>
## 参考文献 <br>
1 Yin Zhang, Zihui Ge, Albert Greenberg, and Matthew Roughan. Network anomography. In Proceedings of the 5th ACM SIGCOMM Conference on Internet Measurement, IMC ’05, pages 30–30, Berkeley, CA, USA, 2005. USENIX Association <br>
2 Balachander Krishnamurthy, Subhabrata Sen, Yin Zhang, and Yan Chen. Sketch-based change detection: methods, evaluation, and applications. In Proceedings of the 3rd ACM SIGCOMM conference on Internet measurement, pages 234–247. ACM, 2003.   <br>
3 He Yan, Ashley Flavel, Zihui Ge, Alexandre Gerber, Daniel Massey, Christos Papadopoulos, Hiren Shah, and Jennifer Yates. Argus: End-to-end service anomaly detection and localization from an isp’s point of view. In INFOCOM, 2012 Proceedings IEEE, pages 2756–2760. IEEE, 2012.  <br>
4  Liu, Fei Tony, Ting, Kai Ming and Zhou, Zhi-Hua. “Isolation forest.” Data Mining, 2008. ICDM‘08. Eighth IEEE International Conference on. <br>
5 Rousseeuw, P.J., Van Driessen, K. “A fast algorithm for the minimum covariance determinant estimator” Technometrics 41(3), 212 (1999) <br>
6 Breunig, Kriegel, Ng, and Sander (2000) LOF: identifying density-based local outliers. Proc. ACM SIGMOD <br>
7 Perdisci R, Gu G, Lee W. Using an Ensemble of One-Class SVM Classifiers to Harden Payload-based Anomaly Detection Systems[C]// International Conference on Data Mining. IEEE, 2007:488-498. <br>
8 https://www.elastic.co/guide/en/logstash/current/plugins-outputs-elasticsearch.html <br>
9 http://ganglia.info/ <br>


