package calculateDistribution

import Utils.Util
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

object DistributedJointDegreeEstimation {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DistributedJointDegreeEstimation")
    val ss = new SparkSession.Builder().config(sparkConf).getOrCreate()
    val sc = ss.sparkContext
    //Parameters.
    val filePath = args(0)
    val VPath = args(1)
    val N = args(2).toLong
    val outputPath = args(3)
    val samplingRatio = args(4).toDouble

    //Sample NRPs.
    val AFile: RDD[String] = Util.sampleNRPs(sc, filePath, samplingRatio)
    val Vs: RDD[(Long, Array[Long])] = AFile.map(line => {
      val L = line.split(":")
      val neighbors = L(1).split(",").map(_.split("_")(0).toLong)
      (L(0).toLong, neighbors)
    })
    Vs.cache()
    val VsCount = Vs.count()
    val Es: RDD[(Long, Long, Double)] = Vs.map {
      case (v, neighbors) =>
        val neighbor = neighbors(Random.nextInt(neighbors.length))
        (Math.min(v, neighbor), Math.max(v, neighbor), 1.0 * VsCount / N / neighbors.length)
    }
    Es.cache()
    Es.collect()
    //Calculate joint degrees.
    val VB: RDD[Long] = Es.flatMap(e => Array(e._1, e._2)).distinct().subtract(Vs.map(_._1))
    var VBList: RDD[(Long, Array[Long])] = sc.emptyRDD
    if (VB.count() != 0) {
      val block: RDD[String] = Util.findVaddData(ss, VB, filePath, VPath)
      val blockData: RDD[(Long, Array[Long])] = block.map(line => {
        val L = line.split(":")
        val neighbors = L(1).split(",").map(_.split("_")(0).toLong)
        (L(0).toLong, neighbors)
      })
      VBList = VB.map(d => (d, 2)).leftOuterJoin(blockData).map(d => (d._1, d._2._2.head))
    }
    val VList: RDD[(Long, Array[Long])] = Vs.union(VBList)
    val jointDegree: RDD[((Int, Int), Double)] = Es.map(e => (e._1, (e._2, e._3)))
      .leftOuterJoin(VList)
      .map(d => (d._2._1._1, (d._1, d._2._2.head.length, d._2._1._2)))
      .leftOuterJoin(VList)
      .map(d => (d._2._1._2, d._2._2.head.length, d._2._1._3))
      .map(d => ((Math.min(d._1, d._2), Math.max(d._1, d._2)), 1 / d._3))
    //Calculate joint degree distribution.
    val zxy: RDD[((Int, Int), Double)] = jointDegree.reduceByKey(_ + _)
    val z = zxy.map(_._2).reduce(_ + _)
    val jdd: RDD[((Int, Int), Double)] = zxy.map { case (e, d) => (e, d / z) }
    jdd.saveAsTextFile(outputPath)
    //Calculate assortativity.
    val degAvg = Vs.map(d => d._2.length).reduce(_ + _).toDouble / Vs.count()
    val left = zxy.map { case (e, d) => d * (e._1 - degAvg) * (e._1 - degAvg) }.sum()
    val right = zxy.map { case (e, d) => d * (e._2 - degAvg) * (e._2 - degAvg) }.sum()
    val denominator = Math.pow(left, 1.0 / 2) * Math.pow(right, 1.0 / 2)
    val numerator = zxy.map { case (e, d) => d * (e._1 - degAvg) * (e._2 - degAvg) }.reduce(_ + _)
    //assortativity
    val ass = numerator / denominator
    println(ass)

    ss.stop()
  }
}
