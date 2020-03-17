package discretization

import org.apache.spark.rdd.RDD

object DiscreOb {
  def discre(inputRdd:RDD[Seq[Double]],stepArr:Array[Double],minArr:Array[Double]):RDD[Seq[Int]]={
    inputRdd.mapPartitions(it=>new DiscreIterator(it,stepArr,minArr).asInstanceOf[Iterator[Seq[Int]]])
  }
}
