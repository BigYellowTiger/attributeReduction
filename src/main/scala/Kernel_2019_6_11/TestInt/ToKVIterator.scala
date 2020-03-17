package Kernel_2019_6_11.TestInt

import org.apache.spark.sql.Row

class ToKVIterator(it:Iterator[Row]) extends Iterator[(Seq[Int],Int)] {
  def hasNext():Boolean=it.hasNext

  def next():(Seq[Int],Int)={
    val temp=it.next()
    val length=temp.length
    val keyArr=new Array[Int](length-1)
    var i=0
    while (i<length-1){
      keyArr.update(i,temp.getAs[Int](i))
      i+=1
    }
    (keyArr.toSeq,temp.getAs[Int](length-1))
  }
}
