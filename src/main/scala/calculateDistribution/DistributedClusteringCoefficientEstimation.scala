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
    val V: RDD[(Long, (Int, Array[Long]))] = V1.union(V2)
    val VRDD: RDD[(Long, (Int, Array[Long]))] = V.repartition(V.getNumPartitions * numPartitionsMultiplier)
    val E: RDD[Edge[None.type]] = ES.map(d => new Edge(d._1, d._2, None))
    val ERDD: RDD[Edge[None.type]] = E.repartition(E.getNumPartitions * numPartitionsMultiplier)
    //Building graphs using GraphX
    val newGraph: Graph[(Int, Array[Long]), None.type] = Graph.apply(VRDD, ERDD, (2, Array(-1)), storageLevel, storageLevel)
    //Calculate the number and degree and triangles for each vertex in V_s.
    val counters: RDD[(Long, (Long, Long))] = Util.calTriangle(newGraph, GraphType.UNDIRECTED)
    //Calculate the clustering coefficients.
    val cc: RDD[(Long, Double)] = counters.map {
      case (id, (tri, d)) =>
        if (d >= 2) (id, 1.0 * tri / d / (d - 1)) //There is not need to times 2, because the tri(number of triangles) is twice the actual number.
        else  (id, 0.0)
    }
    //Calculate the clustering coefficient distribution.
    val ccd: RDD[(Double, Int)] = cc.map(d => (d._2, 1)).reduceByKey(_ + _)
    ccd.saveAsTextFile(outputPath + "/ccd")
    //Calculate the average clustering coefficient.
    val numsOfNode = cc.count()
    val ccSum = cc.map(_._2).reduce(_ + _)
    val acc = ccSum / numsOfNode
    println("acc:" + acc)
    //Calculate the local clustering coefficient.
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
