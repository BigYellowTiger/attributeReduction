package prePro

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object PreToInt {
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.task.maxFailures", "0")
      //.config("spark.network.timeout","3000")
      .config("spark.default.parallelism", 300)
      //.config("spark.driver.maxResultSize", "1g")
      .config("spark.storage.memoryFraction", "0.7")
      //.config("spark.shuffle.memoryFraction","0.3")
      .config("spark.shuffle.consolidateFiles", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("kernel")
      .master("spark://master:7077")
      //.master("local")
      .getOrCreate()
    val fileData=sparkSession.read.parquet("hdfs://master:9000/bigdata/solved_kdd.parquet")
    val length=fileData.first().length
    val schemaArr={
      var result=Seq[String]()
      for(i<-0 to length-1){
        result=result:+("att"+i)
      }
    }
    implicit val seqEncoder1 = org.apache.spark.sql.Encoders.kryo[Seq[String]]
    val result=fileData.map(x=>{
      x.toSeq.asInstanceOf[Seq[String]]
    }).rdd
  }
}
