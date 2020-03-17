package Kernel_2019_6_11

import scala.collection.mutable.ArraySeq

class PreRecoverIterator(it:Iterator[(ArraySeq[Double],Double)]) extends Iterator[Object]{
  def hasNext(): Boolean ={
    it.hasNext
  }

  def next():ArraySeq[Double]={
    val temp:(ArraySeq[Double],Double)=it.next()
    temp._1:+temp._2
  }
}
