package calculateDistribution

import Utils.{GraphType, Util, GetStorageLevel}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DistributedClusteringCoefficientEstimation {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("DistributedClusteringCoefficientEstimation")
    val ss = new SparkSession.Builder().config(sparkConf).getOrCreate()
    val sc = ss.sparkContext
    //Parameters
    val filePath = args(0)
    val VPath = args(1)
    val outputPath = args(2)
    val samplingRatio = args(3).toDouble
    val numPartitionsMultiplier = args(4).toInt
    val storageLevel = GetStorageLevel.getStorageLevel(args(5).toInt)
    //Sample NRPs
    val AFile: RDD[String] = Util.sampleNRPs(sc, filePath, samplingRatio)
    //Processing the linked list of V_s
    val V1: RDD[(Long, (Int, Array[Long]))] = AFile.map(line => {
      val L = line.split(":")
      val neighbors = L(1).split(",").map(_.split("_")(0).toLong)
      (L(0).toLong, (1, neighbors))
    })
    val VA: RDD[Long] = V1.map(_._1)
    var ES: RDD[(Long, Long)] = V1.flatMap(line => {
      val neighbors = line._2._2
      val edges = neighbors.map(e => (line._1, e))
      edges
    })
    //E_s de-duplication
    ES = Util.edgesDistinct(ES)
    //Removing vertices in V_s from V_{neighbor}
    val VB: RDD[Long] = ES.flatMap(d => Array(d._1, d._2)).distinct().subtract(VA)
    var V2: RDD[(Long, (Int, Array[Long]))] = sc.emptyRDD
    if (VB.count() != 0) {
      val block: RDD[String] = Util.findVaddData(ss, VB, filePath, VPath)
      val blockData: RDD[(Long, Array[Long])] = block.map(line => {
        val L = line.split(":")
        val neighbors = L(1).split(",").map(_.split("_")(0).toLong)
        (L(0).toLong, neighbors)
      })
      V2 = VB.map(d => (d, 2)).leftOuterJoin(blockData).map(d => (d._1, (d._2._1, d._2._2.head)))
    }

    //Vα和Vβ的邻接链的并集
    val V: RDD[(Long, (Int, Array[Long]))] = V1.union(V2)
    val VRDD: RDD[(Long, (Int, Array[Long]))] = V.repartition(V.getNumPartitions * numPartitionsMultiplier)
    val E: RDD[Edge[None.type]] = ES.map(d => new Edge(d._1, d._2, None))
    val ERDD: RDD[Edge[None.type]] = E.repartition(E.getNumPartitions * numPartitionsMultiplier)
    //用GraphX构建图
    val newGraph: Graph[(Int, Array[Long]), None.type] = Graph.apply(VRDD, ERDD, (2, Array(-1)), storageLevel, storageLevel)
    //通过每条边计算Vα每个顶点的三角形数量和度
    val counters: RDD[(Long, (Long, Long))] = Util.calTriangle(newGraph, GraphType.UNDIRECTED)
    //计算聚类系数
    val cc: RDD[(Long, Double)] = counters.map {
      case (id, (tri, d)) =>
        if (d >= 2) (id, 1.0 * tri / d / (d - 1)) //这里不用×2，因为tri（三角数量）已经是实际数量的2倍
        else  (id, 0.0)
    }
    //计算聚类系数分布
    val ccd: RDD[(Double, Int)] = cc.map(d => (d._2, 1)).reduceByKey(_ + _)
    //保存聚类系数分布
    ccd.saveAsTextFile(outputPath + "/ccd")
    //计算平均聚类系数
    val numsOfNode = cc.count()
    val ccSum = cc.map(_._2).reduce(_ + _)
    val acc = ccSum / numsOfNode
    println("acc:" + acc)
    //计算全局聚类系数
    val numerator = counters.map(_._2._1).sum() / 2
    val denominator = counters.map(d => d._2._2 * (d._2._2 - 1) / 2.0).sum()
    val gcc = {
      if(denominator == 0)  0
      else  numerator / denominator
    }
    println("gcc:", gcc)

    ss.stop()
  }
}
