package Kernel_2019_6_11.allTheMapOperation

import org.apache.spark.sql.Row

class TwoValIterator(it:Iterator[Row]) extends Iterator[(Seq[Int],Seq[Int])]{
  def hasNext():Boolean=it.hasNext

  def next():(Seq[Int],Seq[Int])={
    val temp=it.next().toSeq
    val length=temp.length
    (temp.slice(0,length-2).asInstanceOf[Seq[Int]],temp.slice(length-2,length).asInstanceOf[Seq[Int]])
  }

}
