package discretization

import org.apache.spark.rdd.RDD

class DiscreIterator(it:Iterator[Seq[Double]],stepArr:Array[Double],minArr:Array[Double]) extends Iterator[Object]{
  def hasNext():Boolean=it.hasNext

  def next():Seq[Int]={
    val temp=it.next()
    var result=Seq[Int]()
    for(i<-0 to temp.size-1){
      result=result:+((temp(i)-minArr(i))/stepArr(i)).toInt
    }
    result
  }
}