package discretization

import Kernel_2019_6_11.allTheMapOperation.{MapToDoubleSeqIterator, MapToStringArrayIterator}
import Kernel_2019_6_8.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**将原数据表均匀离散化为10份*/
object Discre {
  println(9.9875156.toInt)
  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",160)
      .appName("kernel")
      .master("spark://master:7077")
      .getOrCreate()

    val inputTable = sparkSession.read.parquet("hdfs://master:9000/bigdata/discre_epsilon.parquet")

    inputTable.printSchema()
    inputTable.show(1)
    println(inputTable.count())


//    val inputTable=sparkSession.read.parquet("hdfs://master:9000/bigdata/epsilon2.parquet")
//    val realTable=inputTable.limit(10)
//    realTable.createGlobalTempView("findMax")
//    var attArr = Seq[String]()
//    var schemaString=""
//    //将表的schema即列名数组分别赋值给StringArr列表
//    for(i <- 0 to realTable.schema.length-1){
//      attArr = attArr :+ realTable.schema(i).name
//      schemaString+=realTable.schema(i).name+" "
//    }


    //离散份数
//    val dicreNum=10.0
//    //val maxNumArr=new Array[Double](attArr.size)
//    val minNumArr=new Array[Double](attArr.size)
//    val stepArr=new Array[Double](attArr.size)
//
//    //inputTable.createGlobalTempView("findMax")
//    var counter=0
//    while(counter<attArr.size){
//      val max=sparkSession.sql(s"SELECT MAX(${attArr(counter)}) FROM global_temp.findMax").collect()(0).getString(0).toDouble
//      //maxNumArr(counter)=max*1.001
//      val min=sparkSession.sql(s"SELECT MIN(${attArr(counter)}) FROM global_temp.findMax").collect()(0).getString(0).toDouble
//      minNumArr(counter)=min
//      stepArr(counter)=(max*1.001-min)/dicreNum
//      counter+=1
//    }
//    //stepArr.foreach(x=>println(x))
//    implicit val myEncoder=org.apache.spark.sql.Encoders.kryo[Seq[Double]]
//    //val seqRdd=inputTable.mapPartitions(it=>new ToSeqRddIterator(it).asInstanceOf[Iterator[Seq[Double]]]).rdd
//    val seqRdd=realTable.mapPartitions(it=>new ToSeqRddIterator(it).asInstanceOf[Iterator[Seq[Double]]]).rdd
//
//    //seqRdd.foreach(x=>println(x))
//    val resultTable=DiscreOb.discre(seqRdd,stepArr,minNumArr)
//
//    val resultDataFrame=Rdd2IntDf.rdd2df(schemaString,resultTable,sparkSession).repartition()
//    resultDataFrame.printSchema()
//    resultDataFrame.show(1)
//    resultDataFrame.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/bigdata/testDiscre.parquet")
  }
}
