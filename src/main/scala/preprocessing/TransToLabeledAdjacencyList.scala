package preprocessing

import entity.Vertex
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object TransToLabeledAdjacencyList {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TransToLabeledAdjacencyList")
    val sc = new SparkContext(sparkConf)
    //Parameters.
    val inputPath = args(0)
    val outputPath = args(1)
    val isDirectGraph = args(2).toInt match {
      case 0 => false
      case 1 => true
    }
    val numsOfList = args(3).toInt
    //Load files.
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, inputPath, false)

    //Converts an edge pair file into a labeled adjacency list.
    if(isDirectGraph){
      val outNeighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)
      val inNeighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
      val allNeighbor = outNeighbor.join(inNeighbor).sortBy(_._1).map{
        case (id, (out, in)) =>
          val outN = out.map(Vertex(_, 'o'))
          val inN = in.map(Vertex(_, 'i'))
          val n = Array.concat(outN, inN).sortBy(_.id)
          val strings = n.map(d => d.id + "_" + d.direction)
          id + ":" + strings.mkString(",")
      }
      allNeighbor.zipWithIndex()
        .mapValues(_ / numsOfList)
        .map(_.swap)
        .partitionBy(new HashPartitioner(Math.ceil(allNeighbor.count().toDouble / numsOfList).toInt))
        .map(_._2)
        .saveAsTextFile(outputPath)
    }else{
      val neighbor: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)
      val allNeighbor: RDD[String] = neighbor.sortBy(_._1).map {
        case (id, nei) =>
          val strings = nei.sorted(Ordering.Long).map(_ + "_u")
          id + ":" + strings.mkString(",")
      }
   
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
