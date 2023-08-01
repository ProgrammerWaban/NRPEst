package calculateDistribution

import Utils.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DistributedDegreeEstimation {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DistributedDegreeEstimation")
    val sc = new SparkContext(sparkConf)
    //参数
    val filePath = args(0)
    val outputPath = args(1)
    val samplingRatio = args(2).toDouble
    val N = args(3).toLong
    //读取文件夹里的文件列表
    val NRPs: List[String] = Util.getFiles(sc, filePath)
    //抽样块
    val NRPSample: List[String] = Util.sample(NRPs, samplingRatio)
    val Gs: RDD[String] = sc.textFile(NRPSample.mkString(","))
    //抽样块的点的数量
    val Ns: Long = Gs.count()
    //计算每个点的度
    val Fdeg: RDD[(Int, Int)] = Gs.map(line => {
      val L = line.split(",")
      (L.length, 1)
    })
    //计算度分布
    Fdeg.reduceByKey(_ + _).sortBy(_._1).saveAsTextFile(outputPath + "/deg")
    //计算平均度
    val sumDegree = Fdeg.map(_._1).reduce(_ + _)
    val avgDegree = sumDegree.toDouble / Ns
    println("avgDegree=" + avgDegree)
    //计算图密度
    val graphDensity = avgDegree / (N - 1)
    println("graphDensity=" + graphDensity)

    sc.stop()
  }
}
