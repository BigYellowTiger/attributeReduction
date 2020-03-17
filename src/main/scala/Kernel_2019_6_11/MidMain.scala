package Kernel_2019_6_11

import java.util.concurrent._

import Kernel_2019_6_11.allTheMapOperation.{ColUtils, OneValIterator}
import com.esotericsoftware.kryo.Kryo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.storage.StorageLevel


object MidMain {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().registerKryoClasses(Array(
      classOf[(Seq[Int],Int)]
      ,classOf[(Seq[Int],Seq[Int])]
      ,classOf[scala.collection.mutable.WrappedArray.ofRef[_]]
      ,classOf[Array[org.apache.spark.sql.types.StructType]]
      ,classOf[org.apache.spark.sql.types.StructType]
      ))
    val sparkSession = SparkSession
      .builder()
      .config("spark.task.maxFailures","0")
      .config("spark.default.parallelism",375)
      //.config("spark.driver.maxResultSize", "1g")
      //.config("spark.storage.memoryFraction","0.3")
      .config("spark.shuffle.memoryFraction","0.3")
      //.config("spark.shuffle.sort.bypassMergeThreshold","400")
      //.config("spark.shuffle.manager","hash")
      //.config("spark.shuffle.file.buffer","128k")
      //.config("spark.shuffle.consolidateFiles","true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryo.registrationRequired","true")
      .config(conf)
      .appName("kernel")
      //.master("spark://master:7077")
      .master("local")
      .getOrCreate()

    //val fileData=sparkSession.read.parquet("hdfs://master:9000/bigdata/DS4.parquet")
    //println(fileData.count())
    ////local test code
        val fileData = sparkSession
          .read.format("csv")
          .option("header","true")
          .load("C:\\Users\\qly\\Desktop\\testdata.csv")
    var sourceSchema = Seq[String]()
    val attnum=fileData.schema.length
    var resultString=""
    //记录表的schema
    for(i <- 0 to attnum-1){
      sourceSchema = sourceSchema :+ fileData.schema(i).name
    }

    //全表转为int型
    val sourceData=fileData.select(sourceSchema.map(name => fileData.col(name).cast(IntegerType)):_*)

    val kv1Rdd=sourceData.rdd.mapPartitions(it=>new OneValIterator(it))
    //去除冲突行
    val discreNum=10
    val seqRdd=kv1Rdd.reduceByKey((a,b)=>{
      if(a!=b)
        discreNum
      else
        a
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("cached rdd lineNum:"+seqRdd.count)

    //判断属性删除还是保留
    val deleteAtt=new Array[Boolean](attnum-1)
    var calIndex=attnum-2
    val mainLock = new CountDownLatch(attnum-1)
    var stopCutFlag=false

    /*******************test code****************************
    calIndex=50
    for(i<-50 to attnum-2){
      deleteAtt.update(i,true)
    }
      ***************************************/

    //println("start cal")

    class FindCoreException extends Exception
    class CalThread(i:Int,startFlag:Boolean) extends Callable[Boolean]{
      override def call(): Boolean = {
        try{
          //println("in-------------")
          val keyLength={
            var result = 0
            for(i<-0 to deleteAtt.length-1){
              if(!deleteAtt(i))
                result+=1
            }
            result
          }
          val calRdd=ColUtils.getCalRdd(seqRdd,deleteAtt,calIndex,keyLength,stopCutFlag)

          val reducedrdd=calRdd.reduceByKey((a,b)=>{
            if(a(0)!=b(0)){
              println(a(0)+","+b(0))
              throw new FindCoreException
              b
            }
            else
              a
          })
          println(reducedrdd.count+"times"+calIndex)
          //正常遍历完，说明不是核属性，且之前没有在切割，下一次要删除这一列
          if(stopCutFlag){
            deleteAtt.update(calIndex,true)
            mainLock.countDown()
            calIndex-=1
            false
          }
          //之前在切割，删除所有val列
          else {
            for(i<-keyLength/2 to keyLength-1){
              deleteAtt.update(i,true)
              mainLock.countDown()
            }
            calIndex=keyLength/2-1
            false
          }

        }catch {
          //捕捉异常，说明这一次计算属性是核属性,下一次计算要保留这一列
          case e:Exception=>{
            //上一步没有切割属性，保留这一列
            if(stopCutFlag){
              println("find core attribute "+sourceSchema(calIndex))
              resultString+=sourceSchema(calIndex)+" "
              mainLock.countDown()
              calIndex-=1
              true
            }
            //上一次在切割属性，停止切割，恢复正常运算
            else{
              stopCutFlag=true
              true
            }
          }
        }
      }
    }

    val exec =new ThreadPoolExecutor(1,1,0
      ,TimeUnit.SECONDS,new LinkedBlockingQueue[Runnable]())
    var f:Future[Boolean]=null

    //test code
    //for(i<-0 to 50){
    var firstCal=true
    while (calIndex>0){
      //f = exec.submit(new CalThread(calIndex,false))
      if(firstCal){
        f = exec.submit(new CalThread(calIndex,false))
        firstCal=false
      }
      else{
        f = exec.submit(new CalThread(calIndex,f.get()))
      }
    }
    exec.shutdown()
    //等待线程池运算完毕
    mainLock.await
    println("是核属性的属性有："+resultString)

    //防止运算完后自动关闭webui
    while (true){}
  }
}


