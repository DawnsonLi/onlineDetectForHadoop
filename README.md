# onlineDetectForHadoop
  基于hadoop metrics,我们可以实时的获取包含hadoop系统上各个组件的metrics信息，这些metrics用于监控，表现调优和debug,并可用于troubleshooting。
  针对源源不断的metrics流，我提出了一个新颖的方法用于hadoop系统的监控，这个方法的特点如下：
  （1）与基于阈值的监控方法相互结合，使用机器学习的方法用于监控
  （2）在数据流模型上建立基于滑动窗口的探测模型，探测模型是无监督的，当前版本使用隔离森林作为探测器
  （3）使用探测模型对数据的标签，用于进一步的原因分析（使用机器学习和统计模型）
  （4）利用hadoop REST API体现hadoop Queue,history job的状态，协助分析
  


