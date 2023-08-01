# NRPEst
NRPEst: Random Partitioning based Approach for Efficient Graph Property Estimation

## 一、数据预处理
首先执行TransToLabeledAdjacencyList程序，生成标签邻接表。数据集输入格式为边列表，跳过以#开头的注释行，如下所示。  
\# This is a comment  
1 2  
4 1  
1 3  

然后执行GenerateNRP程序，输入标签邻接表，将会生成NRPs和V，分别为NRP数据块和顶点索引。

## 二、NRPEst算法
DistributedDegreeEstimation程序是基于度的属性估计；  
DistributedClusteringCoefficientEstimation程序是基于聚类系数的属性估计；  
DistributedJointDegreeEstimation程序是基于联合度的属性估计；  
DistributedShorestPath程序是基于最短路径的属性估计。  

## 三、程序运行方式
1.本地IDE运行，需要在sparkConf加上.setMaster("local[*]")方可运行；  
2.在Spark集群上运行，先用maven打jar包，上传到服务器，通过spark-submit的方式提交执行。
