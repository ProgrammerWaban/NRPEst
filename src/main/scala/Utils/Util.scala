package Utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, input_file_name, substring_index}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Random

object Util {

  def sample(filePathList: List[String], samplingRate: Double):List[String] = {
    val length: Int = filePathList.length
    val filePathSample: List[String] = Random.shuffle(filePathList).take((length * samplingRate).toInt)
    filePathSample
  }

  def getFiles(sc: SparkContext, inputPath: String): List[String] = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileStatuses = fs.listStatus(new Path(inputPath))
    val fileNames = fileStatuses.map(d => d.getPath.getName)
    val file = fileNames.filter(d => !d.equals("_SUCCESS"))
    file.map(inputPath + "/" + _).toList
  }

  def edgesDistinct(edges: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    //Convert each edge to smaller vertex on the left and the larger vertex on the right.
    val newEdges = edges.map {
      case (e1, e2) =>
        if (e1 > e2) (e2, e1)
        else (e1, e2)
    }
    //Remove duplicate edges.
    newEdges.distinct()
  }

  def calTriangle(graph: Graph[(Int, Array[Long]), None.type], graphType: Int): RDD[(Long, (Long, Long))] = {
    //Count the number of triangles of per vertex in Vs.
    val counters: VertexRDD[(Long, Long)] = graph.aggregateMessages[(Long, Long)](ctx => {
      val n1 = ctx.srcAttr._2
      val n2 = ctx.dstAttr._2
      var counter: Int = 0
      var i = 0
      var j = 0
      while (i < n1.length && j < n2.length) {
        if (n1(i) == n2(j)) {
          i += 1
          j += 1
          counter += 1
        } else if (n1(i) > n2(j)) {
          j += 1
        } else {
          i += 1
        }
      }
      if (ctx.srcAttr._1 == 1) ctx.sendToSrc(counter, 1)  //(Number of triangles, degree)
      if (ctx.dstAttr._1 == 1) ctx.sendToDst(counter, 1)
    }, (attr1, attr2) => (attr1._1 + attr2._1, attr1._2 + attr2._2))
    counters
  }


  def findVaddData(ss: SparkSession, VB: RDD[Long], filePath: String, VPath: String): RDD[String] = {
    import ss.implicits._
    //Find vertices in V_{neighbors} from NRPs not loaded.
    //1.Read the data block contained vertices in V_{neighbors}.
    val blockNo: Column = substring_index(input_file_name(), "/", -1)
    val VNRPs: DataFrame = ss.read.text(VPath).withColumn("blockNo", blockNo)
    val frame: DataFrame = VB.toDF("id").join(VNRPs, col("id").cast("long") === col("value").cast("long"))
    val blockNames: Array[String] = frame.select("blockNo").distinct().rdd.map(filePath + "/" + _.getString(0)).collect()
    //2.From the read NRP, find the linked lists of vertices in V_{neighbors}.
    val block: RDD[String] = ss.sparkContext.textFile(blockNames.mkString(","))
    block
  }

  def sampleNRPs(sc: SparkContext, filePath: String, samplingRatio: Double): RDD[String] = {
    //Randomly select NRP.
    val NRPs: List[String] = Util.getFiles(sc, filePath)
    val NRPSample: List[String] = Util.sample(NRPs, samplingRatio)
    val Gs: RDD[String] = sc.textFile(NRPSample.mkString(","))
    Gs
  }

}
