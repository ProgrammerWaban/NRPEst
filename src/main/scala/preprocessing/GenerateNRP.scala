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
    //Parameters.
    val graphFilePath = args(0)
    val outputPath = args(1)
    val P = args(2).toInt
    //Load file.
    var A: DataFrame = ss.read.text(graphFilePath)
    //Attach a block number to each linked list.
    val blockNo: Column = substring_index(input_file_name(), "/", -1)
    A = A.withColumn("blockNo", blockNo)
    //linked lists are randomly partitioned.
    A = A.withColumn("randomNo", rand())
    A = A.withColumn("j", rank().over(Window.partitionBy("blockNo").orderBy("randomNo")) - 1)
    A = A.withColumn("n", count("*").over(Window.partitionBy("blockNo")))
    val subBlockSize: Column = round(col("n") / P)
    A = A.withColumn("subBlockSize", when(subBlockSize > 0, subBlockSize).otherwise(1))
    val subBlockNo: Column = (col("j") / col("subBlockSize")).cast("int")
    A = A.withColumn("subBlockNo", when(subBlockNo >= P, P - 1).otherwise(subBlockNo))
    A = A.select(col("value"), col("subBlockNo"))
    val NRPs: RDD[(Int, String)] = A.rdd.map(d => (d.getInt(1), d.getString(0))).partitionBy(new HashPartitioner(P))
    NRPs.map(_._2).saveAsTextFile(outputPath + "/NRPs")
    NRPs.map(_._2.split(":")(0).toLong).saveAsTextFile(outputPath + "/V")
    
    ss.stop()
  }
}
