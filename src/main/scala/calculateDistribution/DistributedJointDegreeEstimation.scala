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
    //参数
    val filePath = args(0)
    val outputPath = args(1)
    val samplingRatio = args(2).toDouble
    //抽样NRPs
    val AFile: RDD[String] = Util.sampleNRPs(sc, filePath, samplingRatio)
    //处理VA的邻接链
    val Vs: DataFrame = AFile.map(line => {
      val L = line.split(":")
      (L(0).toLong, L(1).split(",").length)
    }).toDF("vid", "neighbor")
    //每个点的相关边
    val edges: DataFrame = AFile.flatMap(line => {
      val L = line.split(":")
      val node = L(0).toLong
      val neighbors = L(1).split(",").map(d => d.split("_")(0).toLong)
      neighbors.map(d => (Math.min(node, d), Math.max(node, d)))
    }).distinct().toDF("leftNode", "rightNode")
    //利用join给边的2个端点附上邻接链表，同时能够去掉不在抽样点的边
    var k = Vs.join(edges, Vs("vid") === edges("leftNode"))
    k = k.withColumnRenamed("neighbor", "leftNeighbor").drop("vid")
    var kk = Vs.join(k, Vs("vid") === k("rightNode"))
    kk = kk.withColumnRenamed("neighbor", "rightNeighbor").drop("vid")
    val nodePairs: DataFrame = kk.select("leftNode", "rightNode", "leftNeighbor", "rightNeighbor")
    //转化成联合度
    val jointDeg: RDD[(Int, Int)] = nodePairs.rdd.map(d => (Math.min(d.getInt(2), d.getInt(3)), Math.max(d.getInt(2), d.getInt(3))))
    //计算联合度分布
    val jointDegNums = jointDeg.count()
    val jointDegDistribution: RDD[((Int, Int), Int)] = jointDeg.map((_, 1)).reduceByKey(_ + _)
    //保存
    jointDegDistribution.saveAsTextFile(outputPath)
    //计算assortativity
    val deg: RDD[Long] = jointDeg.flatMap{ case (leftDeg, rightDeg) => Array(leftDeg.toLong, rightDeg.toLong)}
    val avgDegree = deg.reduce(_ + _).toDouble / jointDegNums / 2
    val denominator = deg.map(d => Math.pow(d - avgDegree, 2)).reduce(_ + _)
    val numerator = jointDeg.map{ case (leftDeg, rightDeg) => (leftDeg - avgDegree) * (rightDeg - avgDegree)}.reduce(_ + _) * 2
    val ass = numerator / denominator
    println(ass)

    ss.stop()
  }
}
