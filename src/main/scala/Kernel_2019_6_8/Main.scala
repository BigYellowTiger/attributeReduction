package Kernel_2019_6_8

import java.util.concurrent._

import Kernel_2019_6_8.Utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArraySeq

/**
  * 快速核属性计算主程序，没有中断版*/
object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder().config("spark.task.maxFailures","0").config("spark.network.timeout","6000")
      .appName("kernel")
      .master("spark://master:7077")
      .getOrCreate()
    println("spark入口创建完毕")

    var insistTable = sparkSession
      .read.format("csv")
      .option("header","true")
      .load("hdfs://master:9000/bigdata/epsilon2.csv")

    println("数据加载完成")
    var StringArr = Seq[String]()

    //将表的schema即列名数组分别赋值给StringArr列表
    for(i <- 0 to insistTable.schema.length-1){
      StringArr = StringArr :+ insistTable.schema(i).name
    }
    Utils.StringArr=StringArr

    insistTable=Utils.resortAndRePro(Utils.StringArr,insistTable,sparkSession,true)
    insistTable=insistTable.orderBy(createCol(StringArr):_*)

    println("rdd转换为df完成")
    //insistTable.show(1)

    println("开始计算===================================================")
    val mainLock = new CountDownLatch(StringArr.length-1)
    //缓存用df和rdd数组，供各个线程使用
    var kvRdd:RDD[(ArraySeq[Double],ArraySeq[Double])]=null
    var f:Future[Boolean]=null
    var resultString = ""

    //内部类，对单个属性执行核属性判断计算
    class Calculator(val lastInterrupted:Boolean, var allindex:Int) extends Callable[Boolean]{
      override def call(): Boolean = {
        try{
          //如为第一次运算则不用进行任何处理，直接用初始升序表开始运算
          if(allindex==0){
            println("进入判断条件1:第一次运算")
            kvRdd=transferToCalRdd(Utils.StringArr,insistTable,sparkSession)
            println("第一次运算转换为键值对成功"+kvRdd.take(1).length)
          }else{
            if(lastInterrupted){
              println("进入判断条件2：上一次的计算列为核属性，将上次计算列放在第一列")
              Utils.StringArr = {
                var temp = Seq[String]()
                temp=temp:+Utils.StringArr(Utils.StringArr.length-2)
                for(i<-0 to Utils.StringArr.length-3){
                  temp=temp:+Utils.StringArr(i)
                }
                temp=temp:+Utils.StringArr(Utils.StringArr.length-1)
                temp
              }

              insistTable=resortAndRePro(Utils.StringArr,insistTable,sparkSession)
              //insistTable = insistTable.orderBy(createCol(Utils.StringArr):_*)
              kvRdd=transferToCalRdd(Utils.StringArr,insistTable,sparkSession)
              println("运算转换为键值对成功"+kvRdd.take(1).length)
            }else{
              println("进入判断条件3:上一次的计算列不是核属性，删除上一次的计算列")
              insistTable=insistTable.drop(Utils.StringArr(Utils.StringArr.length-2))
              Utils.StringArr={
                var temp = Seq[String]()
                for(i<-0 to Utils.StringArr.length-3)
                  temp=temp:+Utils.StringArr(i)
                temp=temp:+Utils.StringArr(Utils.StringArr.length-1)
                temp
              }

              insistTable=Utils.resortAndRePro(Utils.StringArr,insistTable,sparkSession)
              println("删除上一列")
              kvRdd=transferToCalRdd(Utils.StringArr,insistTable,sparkSession)
              println("运算转换为键值对成功"+kvRdd.take(1).length)
            }
          }
          println("进入运算"+allindex)

          kvRdd=kvRdd.reduceByKey((a,b)=>{
            if(a(0)!=b(0)&&a(1)==b(1)){
              a
            }else{
              println("匹配行1两属性："+a(0)+" "+a(1))
              println("匹配行2两属性："+b(0)+" "+b(1))
              //检测到匹配行，抛出异常提前结束本轮运算
              throw new ClassCastException
            }
          })

          //执行action触发计算操作
          println("总行数"+kvRdd.count())

          //没有被异常打断，正常执行完，说明当前属性不是核属性
          println(Utils.StringArr(Utils.StringArr.length-2)+" is not core attribute!!!!!")
          mainLock.countDown
          println("**************************************")
          //返回false，说明这次计算的结果不是核属性
          false
        }catch{
          case e:Exception=>
            println(Utils.StringArr(Utils.StringArr.length-2)+" is core attribute")
            resultString+=Utils.StringArr(Utils.StringArr.length-2)+" "
            //完成一次计算，门闩计数减少一
            mainLock.countDown
            println("**************************************")
            //返回为true，说明这一次计算属性是核属性
            true
        }
      }
    }

    val exec =new ThreadPoolExecutor(1,1,0
      ,TimeUnit.SECONDS,new LinkedBlockingQueue[Runnable]())

    //提交任务到线程池，通过Future.get方法的阻塞特性实现执行完一个提交一个
    //通过Future.get获取到上一次的计算结果
    for(i<-0 to StringArr.length-2){
      if(i==0){
        //开始第一次运算
        f = exec.submit(new Calculator(false,i))
        println("第一次计算完成")
      }else{
        f = exec.submit(new Calculator(f.get,i))
        println("一次计算完成")
      }
    }
    exec.shutdown()
    //等待线程池运算完毕
    mainLock.await
    //输出最终结果
    println("是核属性的属性有："+resultString)
  }

}
