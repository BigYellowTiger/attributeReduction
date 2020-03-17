package Kernel_2019_6_11.allTheMapOperation

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object ColUtils {
  implicit val seqEncoder1 = org.apache.spark.sql.Encoders.kryo[(Seq[Int],Seq[Int])]
  def getCalRdd(inputRdd:RDD[(Seq[Int],Int)],deleteAtt:Array[Boolean],calIndex:Int,keyLength:Int,stopCutFlag:Boolean)
  :RDD[(Seq[Int],Seq[Int])]={
    inputRdd.mapPartitions(it=>new ToCalIterator(deleteAtt,it,calIndex,keyLength,stopCutFlag))
  }

  def searchFun(accum: LongAccumulator):(Seq[Int],Seq[Int])=>Seq[Int]={
    (a,b)=>{
      if(a(0)!=b(0)){
        accum.add(1)
        b
      }
      else
        a
    }
  }
}
