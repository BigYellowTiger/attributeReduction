package Kernel_2019_6_11.allTheMapOperation

class ToCalIterator(deleteAtt:Array[Boolean],it:Iterator[(Seq[Int],Int)],calIndex:Int,keyLength:Int,stopCutFlag:Boolean)
  extends Iterator[(Seq[Int],Seq[Int])]{
  def hasNext():Boolean=it.hasNext

  def next():(Seq[Int],Seq[Int])={
    if(stopCutFlag){
      val temp=it.next()
      val n = deleteAtt.length
      val keyArr = new Array[Int](keyLength-1)
      val valSeq = Seq[Int](temp._2,temp._1(calIndex))
      var deleteIndex = 0
      var keyIndex = 0
      while (deleteIndex < n) {
        if(deleteAtt(deleteIndex)==false && deleteIndex!=calIndex){
          keyArr.update(keyIndex, temp._1(deleteIndex))
          keyIndex += 1
        }
        deleteIndex += 1
      }
      (keyArr.toSeq,valSeq)
    }else{
      val temp=it.next()
      val n = deleteAtt.length
      val keyArr = new Array[Int](keyLength/2)
      val valArr = new Array[Int](keyLength-keyLength/2+1)
      var deleteIndex = 0
      var keyIndex = 0
      var valIndex = 1
      valArr.update(0,temp._2)
      while (deleteIndex<keyLength){
        if(deleteAtt(deleteIndex)==false && deleteIndex<keyLength/2){
          keyArr.update(keyIndex,temp._1(deleteIndex))
          keyIndex+=1
        }
        if(deleteAtt(deleteIndex)==false && deleteIndex>=keyLength/2){
          valArr.update(valIndex,temp._1(deleteIndex))
          valIndex+=1
        }
        deleteIndex+=1
      }
      (keyArr.toSeq,valArr.toSeq)
    }

  }
}
