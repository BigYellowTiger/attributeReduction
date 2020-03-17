package Kernel_2019_6_11

import org.apache.spark.sql.Row

import scala.collection.mutable.ArraySeq

class CalMapIterator(it:Iterator[Row], StringArr:Seq[String]) extends Iterator[Object] {
  def hasNext(): Boolean ={
    it.hasNext;
  }

  def next(): (ArraySeq[Double],ArraySeq[Double]) ={
    val temp=it.next()
    var keySeq=ArraySeq[Double]()
    var valSeq=ArraySeq[Double]()

    //前半部分作为键
    for(i<-0 to StringArr.length-3){
      keySeq=keySeq:+temp.getAs[Double](StringArr(i))
    }

    //后半部分作为值
    for(i<-StringArr.length-2 to StringArr.length-1){
      valSeq=valSeq:+temp.getAs[Double](StringArr(i))
    }

    (keySeq,valSeq)
  }
}
