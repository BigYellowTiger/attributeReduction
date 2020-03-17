package Kernel_2019_6_8

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable.ArraySeq

/**主程序中用于数据转换的方法*/
object Utils {
  implicit val seqEncoder1 = org.apache.spark.sql.Encoders.kryo[(ArraySeq[Double],Double)]
  implicit val seqEncoder2 = org.apache.spark.sql.Encoders.kryo[(ArraySeq[Double],ArraySeq[Double])]
  var StringArr:Seq[String]=null
  def transferToCalRdd(inputStringArr:Seq[String],oriTable:DataFrame,sparkSession: SparkSession):RDD[(ArraySeq[Double],ArraySeq[Double])]={
    val kvRdd=oriTable.mapPartitions(it=>{
      new CalMapIterator(it,inputStringArr).asInstanceOf[Iterator[(ArraySeq[Double],ArraySeq[Double])]]
    }).rdd
    kvRdd
  }

  def resortAndRePro(inputStringArr:Seq[String],oriTable:DataFrame,sparkSession: SparkSession,init_flag:Boolean=false):DataFrame={
    val kvRdd1 = oriTable.mapPartitions(it=>{
      new PreMapIterator(it,inputStringArr,init_flag).asInstanceOf[Iterator[(ArraySeq[Double],Double)]]
    }).rdd
    println("一般rdd转为键值对rdd完成")

    val solvedRdd = kvRdd1.reduceByKey{(a,b)=>
      if(a>=b)
        a+1
      else
        b+1
    }.mapPartitions(it=>{
      new PreRecoverIterator(it).asInstanceOf[Iterator[ArraySeq[Double]]]
    })
    println("预处理完成")

    //手动指定schema，来进行rdd到df的转换
    val schemaString = {
      //result为输出结果
      var result=""
      for(i<-0 to inputStringArr.length-1){
        result+=(inputStringArr(i))
        if(i!=inputStringArr.length-1)
          result+=" "
      }
      result
    }

    //预处理完成后rdd到df的转换
    Rdd2Df.rdd2df(schemaString,solvedRdd,sparkSession)
  }

  //将所有数据转为double类型
//  def row2Double(it:Iterator[Row]):Iterator[Seq[Double]]={
//    //专门用于初始化的引用
//    val nullList = List[Double]()
//    var temp:Row = null
//    var result=List[List[Double]]()
//    var line=List[Double]()
//    while (it.hasNext){
//      line = nullList
//      temp=it.next()
//      for(i<-0 to temp.length-1){
//        line=line:+temp(i).toString.toDouble
//      }
//      result=result:+line
//    }
//    result.iterator
//  }
//
//  //println("将数据转换为double类型完成")
//
//  //生成字段名数组的函数
  def createCol(arr:Seq[String]):Seq[Column]={
    var result = Seq[Column]()
    for(i <- 0 to arr.length-1){
      result = result :+ new Column(arr(i))
    }
    result
  }
//
//  //将初始表转换为键值对rdd，用于预处理的方法
//  def toKV1(it:Iterator[Row]):Iterator[(Seq[Double],Double)]={
//    //专门用于初始化的引用
//    val nullList = List[Double]()
//    var temp:Row=null
//    var keySeq=Seq[Double]()
//    var valSeq:Double=0
//    var result=Seq[(Seq[Double],Double)]()
//    while(it.hasNext){
//      //每次把缓存序列置空
//      keySeq=nullList
//      valSeq=0
//      temp=it.next()
//      //其他属性作为键
//      for(i<-0 to temp.length-2){
//        keySeq=keySeq:+temp(i).toString.toDouble
//      }
//
//      //决策属性作为值
//      valSeq=temp(temp.length-1).toString.toDouble
//
//      result=result:+(keySeq,valSeq)
//    }
//    //返回键值对的迭代器
//    result.iterator
//  }
//
//  //println("转换为键值对rdd完成")
//
//  //将预处理完的键值对rdd转为一般rdd的方法
//  def toNorm(it:Iterator[(Seq[Double],Double)]):Iterator[Seq[Double]]={
//    var temp:(Seq[Double],Double)=null
//    var result=Seq[Seq[Double]]()
//    while(it.hasNext){
//      temp=it.next()
//      //将键值对转换为列表
//      result=result:+(temp._1:+temp._2)
//    }
//    result.iterator
//  }
//
//  //println("预处理完的键值对rdd转换为一般rdd完成")
//  //根据字段名数组顺序将列表rdd转换为键值对的函数
//  //将升序表转换为键值对
//  def toKV(StringArr:Seq[String]):Iterator[Row]=>Iterator[(Seq[Double],Seq[Double])]={
//      it=>{
//        //专门用于初始化的引用
//        val nullList = List[Double]()
//        var temp:Row=null
//        var keySeq=Seq[Double]()
//        var valSeq=Seq[Double]()
//        var result=Seq[(Seq[Double],Seq[Double])]()
//        while(it.hasNext){
//          //每次把缓存序列置空
//          keySeq=nullList
//          valSeq=nullList
//          temp=it.next()
//          //前半部分作为键
//          for(i<-0 to StringArr.length-3){
//            keySeq=keySeq:+temp.getAs[Double](StringArr(i))
//          }
//
//          //后半部分作为值
//          for(i<-StringArr.length-2 to StringArr.length-1){
//            valSeq=valSeq:+temp.getAs[Double](StringArr(i))
//          }
//
//          result=result:+(keySeq,valSeq)
//        }
//        //返回键值对的迭代器
//        result.iterator
//      }
//    }
}
