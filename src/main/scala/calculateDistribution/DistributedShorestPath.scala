package calculateDistribution

import Utils.{GetStorageLevel, Util}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DistributedShorestPath {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DistributedShorestPath")
    val ss = new SparkSession.Builder().config(sparkConf).getOrCreate()
    val sc = ss.sparkContext

    //Parameters.
    val filePath = args(0)
    val VPath = args(1)
    val samplingRatio = args(2).toDouble
    val numPartitionsMultiplier = args(3).toInt
    val storageLevel = GetStorageLevel.getStorageLevel(args(4).toInt)
    val iterNum = 3

    val startTime = System.currentTimeMillis()

    //Sample NRPs.
    //Load data.
    val NRPs: List[String] = Util.getFiles(sc, filePath)
    val NRPSample: List[String] = Util.sample(NRPs, samplingRatio)

    var T: RDD[(Int, Double)] = sc.emptyRDD

    //Compute shortest paths for subgraphs
    for (i <- NRPSample.indices) {
      //Load an NRP.
      val A: RDD[String] = sc.textFile(NRPSample(i))
      val V: RDD[Long] = A.map(_.split(":")(0).toLong)
      val VGlobal: Array[Long] = V.collect()
      val Vneighbor: RDD[Long] = A.flatMap(line => {
        val s = line.split(":")
        val Vall = s(1).split(",").map(_.split("_")(0).toLong)
        Vall
      }).distinct().subtract(V)
      val VneighborGlobal: Map[Long, Int] = Vneighbor.collect().map((_, 1)).toMap
      val E: RDD[(Long, Long)] = A.flatMap(line => {
        val s = line.split(":")
        val node = s(0).toLong
        s(1).split(",").map(d => (Math.min(node, d.split("_")(0).toLong), Math.max(node, d.split("_")(0).toLong)))
      }).distinct()
      //Add edges between V_{neighbor}.
      var VneighborEdges: RDD[(Long, Long)] = sc.emptyRDD
      if (Vneighbor.count() > 0) {
        val block: RDD[String] = Util.findVaddData(ss, Vneighbor, filePath, VPath)
        val blockData: RDD[(Long, String)] = block.map(line => (line.split(":")(0).toLong, line.split(":")(1)))
        val VneighborList: RDD[(Long, String)] = Vneighbor.map((_, 2)).leftOuterJoin(blockData).map(d => (d._1, d._2._2.head))
        val Eneighbor: RDD[(Long, Long)] = VneighborList.flatMap {
          case (node, str) =>
            val nList = str.split(",").map(_.split("_")(0).toLong)
            val n = nList.filter(VneighborGlobal.contains)
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
      //Construct subgraph.
      val newV = V.union(Vneighbor).map((_, 1))
      val newVRDD = newV.repartition(newV.getNumPartitions * numPartitionsMultiplier)
      val newE = E.union(VneighborEdges).flatMap(d => Array(new Edge(d._1, d._2, None), new Edge(d._2, d._1, None)))
      val newERDD = newE.repartition(newE.getNumPartitions * numPartitionsMultiplier)
      val graph: Graph[Int, None.type] = Graph.apply(newVRDD, newERDD, 1, storageLevel, storageLevel)
      //Calculate the number of vertex pairs of path length 1, 2, 3
      var newGraph: Graph[Map[VertexId, Int], None.type] = graph.mapVertices((vid, n) => {
        if (VGlobal.contains(vid)) {
          Map[VertexId, Int](vid -> 0)
        } else {
          Map[VertexId, Int]()
        }
      })
      newGraph = newGraph.pregel(Map[VertexId, Int](), iterNum)(
        (vid, attr, msg) => {
          val keySet = attr.keySet ++ msg.keySet
          keySet.map(key => {
            val value = Math.min(attr.getOrElse(key, Int.MaxValue - 10), msg.getOrElse(key, Int.MaxValue - 10) + 1)
            key -> value
          }).toMap
        },
        triplet => {
          if (triplet.srcAttr.nonEmpty && triplet.dstAttr.nonEmpty) {
            Iterator((triplet.srcId, triplet.dstAttr), (triplet.dstId, triplet.srcAttr))
          } else if (triplet.srcAttr.isEmpty && triplet.dstAttr.nonEmpty) {
            Iterator((triplet.srcId, triplet.dstAttr))
          } else if (triplet.srcAttr.nonEmpty && triplet.dstAttr.isEmpty) {
            Iterator((triplet.dstId, triplet.srcAttr))
          } else {
            Iterator()
          }
        },
        (m1, m2) => {
          val keySet = m1.keySet ++ m2.keySet
          keySet.map(key => {
            val value = Math.min(m1.getOrElse(key, Int.MaxValue - 10), m2.getOrElse(key, Int.MaxValue - 10))
            key -> value
          }).toMap
        }
      )
      val pathLen: RDD[(Int, Long)] = newGraph.vertices.filter(d => VGlobal.contains(d._1)).flatMap {
        case (id, m) =>
          m.toList.map(d => (d._2, 1L))
      }.reduceByKey(_ + _)
      //Calculate the number of total vertex pairs
      val sumPairs = VGlobal.length.toDouble * (VGlobal.length.toDouble - 1) / 2

      val pathLenPercent: RDD[(Int, Double)] = pathLen.map(d => (d._1, d._2.toDouble / 2 / sumPairs))
      T = T.union(pathLenPercent)
    }

    val outcome: RDD[(Int, Double)] = T.filter(_._1 != 0).reduceByKey(_ + _).mapValues(_ / NRPSample.length)

    outcome.collect().sortBy(_._1).foreach(println)

    val endTime = System.currentTimeMillis()

    val totalTime = (endTime - startTime) / 1000
    println("time", totalTime)

    ss.stop()
  }
}
