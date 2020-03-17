package Kernel_2019_6_11

import org.apache.spark.sql.Row

import scala.collection.mutable.ArraySeq

class PreMapIterator(it:Iterator[Row], StringArr:Seq[String],init_flag:Boolean=false) extends Iterator[Object]{
  def hasNext(): Boolean ={
    it.hasNext
  }

  def next(): (ArraySeq[Double],Double) ={
    val temp=it.next();
    var keySeq=ArraySeq[Double]();
    var valSeq:Double=0;

    //其他属性作为键
    //前半部分作为键
    if(init_flag){
      for(i<-0 to StringArr.length-2){
        keySeq=keySeq:+temp.getAs[String](StringArr(i)).toDouble
      }

      //决策属性作为值
      valSeq=temp.getAs[String](StringArr(StringArr.length-1)).toDouble
    }
    else {
      for(i<-0 to StringArr.length-2){
        keySeq=keySeq:+temp.getAs[Double](StringArr(i))
      }

      //决策属性作为值
      valSeq=temp.getAs[Double](StringArr(StringArr.length-1))
    }


    (keySeq,valSeq)
  }
}
