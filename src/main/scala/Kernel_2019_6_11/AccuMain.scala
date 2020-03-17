package Kernel_2019_6_11

import java.util.concurrent._

import Kernel_2019_6_11.allTheMapOperation.{ColUtils, OneValIterator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

object AccuMain {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .config("spark.task.maxFailures","0")
      .config("spark.default.parallelism",300)
      //.config("spark.driver.maxResultSize", "1g")
      //.config("spark.storage.memoryFraction","0.3")
      .config("spark.shuffle.memoryFraction","0.3")
      //.config("spark.shuffle.sort.bypassMergeThreshold","400")
      //.config("spark.shuffle.manager","hash")
      .config("spark.shuffle.file.buffer","128k")
      //.config("spark.shuffle.consolidateFiles","true")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("kernel")
      .master("spark://master:7077")
      //.master("local")
      .getOrCreate()

    val fileData=sparkSession.read.parquet("hdfs://master:9000/bigdata/higgs_discre.parquet")
    //println(fileData.count())
    //local test code
//        val fileData = sparkSession
//          .read.format("csv")
//          .option("header","true")
//          .load("C:\\Users\\Administrator\\Desktop\\testData\\tableSdis.csv")
    var sourceSchema = Seq[String]()
    val attnum=fileData.schema.length
    var resultString=""
    //记录表的schema
    for(i <- 0 to attnum-1){
      sourceSchema = sourceSchema :+ fileData.schema(i).name
    }

    //全表转为int型
    val sourceData=fileData.select(sourceSchema.map(name => fileData.col(name).cast(IntegerType)):_*)

    implicit val seqEncoder1 = org.apache.spark.sql.Encoders.kryo[(Seq[Int],Int)]
    implicit val seqEncoder2 = org.apache.spark.sql.Encoders.kryo[(Seq[Int],Seq[Int])]
    val kv1Rdd=sourceData.rdd.mapPartitions(it=>new OneValIterator(it))
    //去除冲突行
    val discreNum=10
    val seqRdd=kv1Rdd.reduceByKey((a,b)=>{
      if(a!=b)
        discreNum
      else
        a
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(seqRdd.count)

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
    val findCoreAccu=sparkSession.sparkContext.longAccumulator
    //sparkSession.sparkContext.register(findCoreAccu)
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
          //println(calRdd.count())
          //calRdd.foreach(x=>println(x))

          val reducedrdd=calRdd.reduceByKey(ColUtils.searchFun(findCoreAccu))
          println(reducedrdd.count+"times"+calIndex)
          //找到核属性
          //println("before: "+findCoreAccu.value)
          if(!findCoreAccu.isZero){
            findCoreAccu.reset()
            if(stopCutFlag){
              println("find core attribute "+sourceSchema(calIndex))
              resultString+=sourceSchema(calIndex)+" "
              mainLock.countDown()
              calIndex-=1
              return true
            }
            //上一次在切割属性，停止切割，恢复正常运算
            else{
              stopCutFlag=true
              return true
            }
          }
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
            e.printStackTrace()
            true
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
    while (true){}
  }
}
