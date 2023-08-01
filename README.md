# NRPEst
NRPEst: Random Partitioning based Approach for Efficient Graph Property Estimation

## 1.Data pre-processing
First, execute the "TransToLabeledAdjacencyList" program to generate the Labeled Adjacency List. The format of input dataset is a list of edges, and the comment line which begins with "#" will be skipped.There is an example as below.  
\# This is a comment  
1 2  
4 1  
1 3  

Then,the "GenerateNRP" program is executed and the input Labeled Adjacency List will change into NRPs and V, which are NRP data blocks and vertex indexes, respectively.  

## 2.The NRPEst algorithm  
"DistributedDegreeEstimation" is a program for degree-based properties estimation.   
"DistributedClusteringCoefficientEstimation" is a program for clustering coefficient-based properties estimation.   
"DistributedJointDegreeEstimation" is a program for joint degree-based properties estimation.   
"DistributedShorestPath" is a program for shortest path length-based properties estimation.   

## 3.How the program works  
(1)On local IDE: you need to add ".setMaster("local[*]")" to  "sparkConf"  to run.   
(2)On Spark cluster: first use maven to pack up the jar package, then upload to the server, through the spark-submit way to submit the execution.  
