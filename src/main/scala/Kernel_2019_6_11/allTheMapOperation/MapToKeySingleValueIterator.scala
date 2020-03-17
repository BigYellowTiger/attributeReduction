package Kernel_2019_6_11.allTheMapOperation

class MapToKeySingleValueIterator(it:Iterator[Seq[Double]]) extends Iterator[Object]{
  def hasNext():Boolean={
    it.hasNext
  }

  def next():(Seq[Double],Double)={
    val temp=it.next()
    (temp.slice(0,temp.size-1),temp(temp.size-1))
  }
}
