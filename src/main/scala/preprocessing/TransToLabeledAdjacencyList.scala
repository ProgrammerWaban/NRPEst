package preprocessing

import entity.Vertex
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object TransToLabeledAdjacencyList {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TransToLabeledAdjacencyList")
    val sc = new SparkContext(sparkConf)
    //参数
    val inputPath = args(0)
    val outputPath = args(1)
    val isDirectGraph = args(2).toInt match {
      case 0 => false
      case 1 => true
    }
    val numsOfList = args(3).toInt
    //读取文件
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, inputPath, false)

    if(isDirectGraph){
      //计算out邻居
      val outNeighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)
      //计算in邻居
      val inNeighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
      //将out邻居和in邻居合并起来
      val allNeighbor = outNeighbor.join(inNeighbor).sortBy(_._1).map{
        case (id, (out, in)) =>
          val outN = out.map(Vertex(_, 'o'))
          val inN = in.map(Vertex(_, 'i'))
          val n = Array.concat(outN, inN).sortBy(_.id)
          val strings = n.map(d => d.id + "_" + d.direction)
          id + ":" + strings.mkString(",")
      }
      //保存
      //allNeighbor.repartition(numPartitions).saveAsTextFile(outputPath)
      allNeighbor.zipWithIndex()
        .mapValues(_ / numsOfList)
        .map(_.swap)
        .partitionBy(new HashPartitioner(Math.ceil(allNeighbor.count().toDouble / numsOfList).toInt))
        .map(_._2)
        .saveAsTextFile(outputPath)
    }else{
      //计算邻居
      val neighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)
      //转换
      val allNeighbor: RDD[String] = neighbor.sortBy(_._1).map {
        case (id, nei) =>
          val strings = nei.sorted(Ordering.Long).map(_ + "_u")
          id + ":" + strings.mkString(",")
      }
      //保存
      //allNeighbor.repartition(numPartitions).saveAsTextFile(outputPath)
      allNeighbor.zipWithIndex()
        .mapValues(_ / numsOfList)
        .map(_.swap)
        .partitionBy(new HashPartitioner(Math.ceil(allNeighbor.count().toDouble / numsOfList).toInt))
        .map(_._2)
        .saveAsTextFile(outputPath)
    }

    sc.stop
  }
}
