package calculateDistribution

import Utils.{GetStorageLevel, Util}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object DistributedShorestPath {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DistributedShorestPath")
    val ss = new SparkSession.Builder().config(sparkConf).getOrCreate()
    val sc = ss.sparkContext
    import ss.implicits._

    //参数
    val filePath = args(0)
    val VPath = args(1)
    val samplingRatio = args(2).toDouble
    val numPartitionsMultiplier = args(3).toInt
    val storageLevel = GetStorageLevel.getStorageLevel(args(4).toInt)

    //抽样NRPs
    //读取文件夹里的文件列表
    val NRPs: List[String] = Util.getFiles(sc, filePath)
    //抽样NRPs的文件列表
    val NRPSample: List[String] = Util.sample(NRPs, samplingRatio)

    var T: RDD[(Int, Long)] = sc.emptyRDD

    //对每个NRP子图计算最短路径
    for (i <- NRPSample.indices) {
      //读取一个NRP
      val A: RDD[String] = sc.textFile(NRPSample(i))
      //点集V
      val V: RDD[Long] = A.map(_.split(":")(0).toLong)
      val VGlobal: Array[Long] = V.collect()
      //m = VGlobal.length
      //点集Vneighbor
      val Vneighbor: RDD[Long] = A.flatMap(line => {
        val s = line.split(":")
        val Vall = s(1).split(",").map(_.split("_")(0).toLong)
        Vall
      }).distinct().subtract(V)
      //V的关联边E
      val E: RDD[(Long, Long)] = A.flatMap(line => {
        val s = line.split(":")
        val node = s(0).toLong
        s(1).split(",").map(d => (Math.min(node, d.split("_")(0).toLong), Math.max(node, d.split("_")(0).toLong)))
      }).distinct()
      //找Vneighbor之间的边
      var VneighborEdges: RDD[(Long, Long)] = sc.emptyRDD
      if (Vneighbor.count() > 0) {
        val block: RDD[String] = Util.findVaddData(ss, Vneighbor, filePath, VPath)
        val blockData: RDD[(Long, String)] = block.map(line => (line.split(":")(0).toLong, line.split(":")(1)))
        val VneighborList: RDD[(Long, String)] = Vneighbor.map((_, 2)).leftOuterJoin(blockData).map(d => (d._1, d._2._2.head))
        val VneighborGlobal: Array[Long] = Vneighbor.collect()
        val Eneighbor: RDD[(Long, Long)] = VneighborList.flatMap {
          case (node, str) =>
            val nList = str.split(",").map(_.split("_")(0).toLong)
            val n = nList.filter(VneighborGlobal.contains(_))
            n.map(d => (Math.min(node, d), Math.max(node, d)))
        }.distinct()
        val EneighborList: RDD[(Long, Long, String, String)] = Eneighbor.leftOuterJoin(blockData)
          .map(d => (d._2._1, (d._1, d._2._2.head)))
          .leftOuterJoin(blockData)
          .map(d => (d._2._1._1, d._1, d._2._1._2, d._2._2.head))
        VneighborEdges = EneighborList.filter{
          case (l, l1, str, str1) =>
            val n1 = str.split(",").map(_.split("_")(0).toLong).filter(VGlobal.contains(_))
            val n2 = str1.split(",").map(_.split("_")(0).toLong).filter(VGlobal.contains(_))
            if (n1.length == 1 && n2.length == 1 && n1(0) == n2(0)) false
            else  true
        }.map(d => (d._1, d._2))
      }
      //构成图去计算最短路径
      //点
      val newV = V.union(Vneighbor).map((_, 1))
      val newVRDD = newV.repartition(newV.getNumPartitions * numPartitionsMultiplier)
      //边
      val newE = E.union(VneighborEdges).flatMap(d => Array(new Edge(d._1, d._2, None), new Edge(d._2, d._1, None)))
      val newERDD = newE.repartition(newE.getNumPartitions * numPartitionsMultiplier)
      //图
      val graph: Graph[Int, None.type] = Graph.apply(newVRDD, newERDD, 1, storageLevel, storageLevel)
      //找最大的连通分量
      val ccGraph: Graph[VertexId, None.type] = graph.connectedComponents()
      val largestComponent: (Int, VertexId) = ccGraph.vertices.map(v => (v._2, 1)).reduceByKey(_ + _).map(_.swap).max()
      val largeCCGraph: Graph[VertexId, None.type] = ccGraph.subgraph(vpred = (v, d) => d == largestComponent._2)
      val ccVGlobal: Array[VertexId] = largeCCGraph.vertices.filter(v => VGlobal.contains(v._1)).map(_._1).collect()
      //计算最短路径
      val sp: Graph[SPMap, None.type] = ShortestPaths.run(largeCCGraph, ccVGlobal)
      val Ti: RDD[(Int, Long)] = sp.vertices.filter(d => VGlobal.contains(d._1)).flatMap { case (id, map) => map.toList.map(d => (d._2, 1L)) }.filter(_._1 > 0)
      //并入T
      T = T.union(Ti)
    }


    val Tavg: RDD[(Int, Double)] = T.reduceByKey(_ + _).mapValues(_.toDouble / NRPSample.length)

    val TavgMap: Map[Int, Double] = Tavg.collect().toMap
    println("TavgMap", TavgMap)

    val list: List[(Int, Double)] = TavgMap.toList

    //计算平均最短路径
    var num = 0d
    var len = 0d
    for (i <- list.indices) {
      num = num + list(i)._2
      len = len + list(i)._1 * list(i)._2
    }
    println("avgShortestPathLength", len/num)

    //计算90%直径
    val spList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    for (i <- list.indices) {
      val k = list(i)._2.toInt
      for (j <- 0 until k) {
        spList.append(list(i)._1)
      }
    }
    val sorted: ArrayBuffer[Int] = spList.sorted
    val position = (0.9 * sorted.length).toInt - 1
    if (sorted.isEmpty || position < 0) {
      println("90%Diameter", 0)
    } else {
      println("90%Diameter", sorted(position))
    }

    ss.stop()
  }
}
