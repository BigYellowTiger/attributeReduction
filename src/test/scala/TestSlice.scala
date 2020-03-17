import java.util
import java.util.{ArrayList, Date}
import java.util.concurrent._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TestSlice extends App{
  val num1=Seq[Int](1,2,3,4)
  val num2=Seq[Int](1,2,3,4)



  val startTime=new Date().getTime
  for(i<-0 to 400000)
    println(num1.hashCode()==num2.hashCode())
  val endTime=new Date().getTime
  println(endTime-startTime)

}
