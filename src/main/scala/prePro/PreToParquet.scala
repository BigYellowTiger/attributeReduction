package prePro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object PreToParquet {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.task.maxFailures","0")
      //.config("spark.network.timeout","3000")
      .config("spark.default.parallelism",300)
      //.config("spark.driver.maxResultSize", "1g")
      .config("spark.storage.memoryFraction","0.7")
      //.config("spark.shuffle.memoryFraction","0.3")
      .config("spark.shuffle.consolidateFiles","true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("kernel")
      .master("spark://master:7077")
      //.master("local")
      .getOrCreate()

    val fileName="HIGGS"
    val fileData = sparkSession
      .read.format("csv")
      .option("header","true")
      .load("hdfs://master:9000/bigdata/"+fileName+".csv")
    fileData.printSchema()
    println(fileData.count())
    //println("first:"+fileData.first())

    val result=fileData.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/bigdata/"+fileName+".parquet")
    val test=sparkSession.read.parquet("hdfs://master:9000/bigdata/"+fileName+".parquet")
    test.printSchema()
    test.show(1)
  }
}
