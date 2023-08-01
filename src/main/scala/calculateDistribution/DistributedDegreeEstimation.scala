package calculateDistribution

import Utils.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DistributedDegreeEstimation {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DistributedDegreeEstimation")
    val sc = new SparkContext(sparkConf)
    //Parameters.
    val filePath = args(0)
    val outputPath = args(1)
    val samplingRatio = args(2).toDouble
    val N = args(3).toLong
    //Load data.
    val NRPs: List[String] = Util.getFiles(sc, filePath)
    //Sample NRPs.
    val NRPSample: List[String] = Util.sample(NRPs, samplingRatio)
    val Gs: RDD[String] = sc.textFile(NRPSample.mkString(","))
    //Calculate the number of vertices.
    val Ns: Long = Gs.count()
    //Calculate the degree of each vertices.
    val Fdeg: RDD[(Int, Int)] = Gs.map(line => {
      val L = line.split(",")
      (L.length, 1)
    })
    //Calculate the degree distribution.
    Fdeg.reduceByKey(_ + _).sortBy(_._1).saveAsTextFile(outputPath + "/deg")
    //Calculate the average degree.
    val sumDegree = Fdeg.map(_._1).reduce(_ + _)
    val avgDegree = sumDegree.toDouble / Ns
    println("avgDegree=" + avgDegree)
    //Calculate the graph density.
    val graphDensity = avgDegree / (N - 1)
    println("graphDensity=" + graphDensity)
    //Calculate the power law exponent.
    val minDeg = Fdeg.map(_._1).filter(_ != 0).min()
    val avgLogDeg = Fdeg.map(d => Math.log(d._1)).mean()
    val ple = 1 / (avgLogDeg - Math.log(minDeg)) + 1
    println("PowerLawExponent=" + ple)

    sc.stop()
  }
}
