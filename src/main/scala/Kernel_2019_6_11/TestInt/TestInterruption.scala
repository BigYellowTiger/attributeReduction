package Kernel_2019_6_11.TestInt

import java.util.concurrent._

import Kernel_2019_6_11.allTheMapOperation.{ColUtils, OneValIterator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

object TestInterruption {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder()
      .config("spark.task.maxFailures","0")
      .config("spark.default.parallelism",20)
      .config("spark.shuffle.memoryFraction","0.3")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("kernel")
      .master("spark://centos1:7077")
      //.master("local")
      .getOrCreate()

    val fileData = sparkSession
      .read.format("csv")
      .option("header","true")
      .load("hdfs://centos1:9000/data/allone.csv")
      //.load("C:\\Users\\qly\\Desktop\\allone.csv")

    var sourceSchema = Seq[String]()
    val attnum=fileData.schema.length
    var resultString=""
    //记录表的schema
    for(i <- 0 to attnum-1){
      sourceSchema = sourceSchema :+ fileData.schema(i).name
    }
    val sourceData=fileData.select(sourceSchema.map(name => fileData.col(name).cast(IntegerType)):_*)
    //val kvRdd=sourceData.rdd.mapPartitions(it=>new OneValIterator(it))
    //println(kvRdd.count()+"pair rdd created")

    val exec =new ThreadPoolExecutor(1,1,0
      ,TimeUnit.SECONDS,new LinkedBlockingQueue[Runnable]())
    var f:Future[Integer]=null
    class TestIntThread(index:Integer,lastResult:Integer) extends Callable[Integer]{
      override def call(): Integer = {
        try{
          val kvRdd=sourceData.rdd.mapPartitions(it=>new OneValIterator(it))
          println("kv lines"+kvRdd.count())
          val temp=kvRdd.reduceByKey(TestUtils.searchFun3(index)).count()
          println("total lines:"+temp)
        }
        catch {
          case e:Exception=>{
            println("find exception"+index)
            println("************************")
            return index
          }
        }
        println("no exception found："+index)
        println("************************")
        index
      }
    }

    var counter=1
    var firstCal=true
    while (counter<=40){
      //f = exec.submit(new CalThread(calIndex,false))
      if(firstCal){
        f = exec.submit(new TestIntThread(counter,0))
        firstCal=false
        counter+=1
      }
      else{
        f = exec.submit(new TestIntThread(counter,f.get()))
        counter+=1
      }
    }
    exec.shutdown()
    //等待线程池运算完毕

    //防止运算完后自动关闭webui
    while (true){}
  }
}
