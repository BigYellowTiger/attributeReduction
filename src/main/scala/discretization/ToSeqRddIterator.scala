package discretization

import org.apache.spark.sql.Row

class ToSeqRddIterator(it:Iterator[Row]) extends Iterator[Object]{
  def hasNext():Boolean=it.hasNext

  def next():Seq[Double]={
    val temp=it.next()
    var result=Seq[Double]()
    for(i<-0 to temp.size-1){
      result=result:+temp.getAs[Double](i)
    }
    result
  }
}
