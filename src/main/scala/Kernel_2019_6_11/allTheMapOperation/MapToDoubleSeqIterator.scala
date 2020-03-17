package Kernel_2019_6_11.allTheMapOperation

class MapToDoubleSeqIterator(it:Iterator[Array[String]]) extends Iterator[Object]{
  def hasNext():Boolean={
    it.hasNext
  }

  def next():Seq[Double]={
    var result=Seq[Double]()
    val temp=it.next()
    for(i<-0 to temp.length-1){
      result=result:+temp(i).toDouble
    }
    result
  }
}
