package preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, input_file_name, rand, rank, round, substring_index, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object GenerateNRP {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GenerateNRP")
    val ss = new SparkSession.Builder().config(sparkConf).getOrCreate()
    //参数
    val graphFilePath = args(0)
    val outputPath = args(1)
    val P = args(2).toInt
    //读取文件
    var A: DataFrame = ss.read.text(graphFilePath)
    //给每条数据附上对应的块号(文件名
    val blockNo: Column = substring_index(input_file_name(), "/", -1)
    A = A.withColumn("blockNo", blockNo)
    //给每条数据附上随机数
    A = A.withColumn("randomNo", rand())
    //根据块号(文件名)分区，并且分区内按随机数的大小排序(相当于乱序)，排序后的顺序当成下标j
    A = A.withColumn("j", rank().over(Window.partitionBy("blockNo").orderBy("randomNo")) - 1)
    //计算每个分区的数据量
    A = A.withColumn("n", count("*").over(Window.partitionBy("blockNo")))
    //计算每个分区内子块的大小
    val subBlockSize: Column = round(col("n") / P)
    A = A.withColumn("subBlockSize", when(subBlockSize > 0, subBlockSize).otherwise(1))
    //计算每个分区的每条数据属于哪一个子块
    val subBlockNo: Column = (col("j") / col("subBlockSize")).cast("int")
    A = A.withColumn("subBlockNo", when(subBlockNo >= P, P - 1).otherwise(subBlockNo))
    //留下有用的列
    A = A.select(col("value"), col("subBlockNo"))
    //转化成RDD并按块号分区
    val NRPs: RDD[(Int, String)] = A.rdd.map(d => (d.getInt(1), d.getString(0))).partitionBy(new HashPartitioner(P))
    //保存
    NRPs.map(_._2).saveAsTextFile(outputPath + "/NRPs")
    //只保存链表的头节点
    NRPs.map(_._2.split(":")(0).toLong).saveAsTextFile(outputPath + "/V")

    ss.stop()
  }
}
