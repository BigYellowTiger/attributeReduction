package Kernel_2019_6_11.allTheMapOperation

class MapToStringArrayIterator(it:Iterator[String]) extends Iterator[Object]{
  def hasNext():Boolean={
    it.hasNext
  }

  override def next(): Array[String] = {
    it.next().split(",")
  }
}
