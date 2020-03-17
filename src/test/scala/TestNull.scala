import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TestNull extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession
    .builder()
    .appName("kernel")
    .master("local")
    .getOrCreate()
  val seq1=Seq((1,2,3),(4,5,6),(7,8,9))
  val rdd1=sparkSession.sparkContext.parallelize(seq1)
  rdd1.foreach(x=>println(x))
}
