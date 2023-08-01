package calculateDistribution

import Utils.Util
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DistributedJointDegreeEstimation {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DistributedJointDegreeEstimation")
    val ss = new SparkSession.Builder().config(sparkConf).getOrCreate()
    val sc = ss.sparkContext
    import ss.implicits._
    //Parameters.
    val filePath = args(0)
    val outputPath = args(1)
    val samplingRatio = args(2).toDouble
    //Sample NRPs.
    val AFile: RDD[String] = Util.sampleNRPs(sc, filePath, samplingRatio)
    val Vs: DataFrame = AFile.map(line => {
      val L = line.split(":")
      (L(0).toLong, L(1).split(",").length)
    }).toDF("vid", "neighbor")
    val edges: DataFrame = AFile.flatMap(line => {
      val L = line.split(":")
      val node = L(0).toLong
      val neighbors = L(1).split(",").map(d => d.split("_")(0).toLong)
      neighbors.map(d => (Math.min(node, d), Math.max(node, d)))
    }).distinct().toDF("leftNode", "rightNode")
    //Attach degree values to the 2 endpoints of an edge, while being able to remove non-conforming edges.
    var k = Vs.join(edges, Vs("vid") === edges("leftNode"))
    k = k.withColumnRenamed("neighbor", "leftNeighbor").drop("vid")
    var kk = Vs.join(k, Vs("vid") === k("rightNode"))
    kk = kk.withColumnRenamed("neighbor", "rightNeighbor").drop("vid")
    val nodePairs: DataFrame = kk.select("leftNode", "rightNode", "leftNeighbor", "rightNeighbor")
    //Calculate joint degrees.
    val jointDeg: RDD[(Int, Int)] = nodePairs.rdd.map(d => (Math.min(d.getInt(2), d.getInt(3)), Math.max(d.getInt(2), d.getInt(3))))
    //Calculate joint degree distribution.
    val jointDegNums = jointDeg.count()
    val jointDegDistribution: RDD[((Int, Int), Int)] = jointDeg.map((_, 1)).reduceByKey(_ + _)
    jointDegDistribution.saveAsTextFile(outputPath)
    //Calculate assortativity.
    val deg: RDD[Long] = jointDeg.flatMap{ case (leftDeg, rightDeg) => Array(leftDeg.toLong, rightDeg.toLong)}
    val avgDegree = deg.reduce(_ + _).toDouble / jointDegNums / 2
    val denominator = deg.map(d => Math.pow(d - avgDegree, 2)).reduce(_ + _)
    val numerator = jointDeg.map{ case (leftDeg, rightDeg) => (leftDeg - avgDegree) * (rightDeg - avgDegree)}.reduce(_ + _) * 2
    val ass = numerator / denominator
    println(ass)

    ss.stop()
  }
}
