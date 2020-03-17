package Kernel_2019_6_11.allTheMapOperation

import org.apache.spark.sql.Row

class ToArrayIterator(it:Iterator[Row]) extends Iterator[Array[Int]]{
  def hasNext():Boolean=it.hasNext

  def next():Array[Int]={
    val temp=it.next()
    val n = temp.length
    val values = new Array[Int](n)
    var i = 0
    while (i < n) {
      values.update(i, temp.getAs[String](i).toInt)
      i += 1
    }
    values
  }
}
