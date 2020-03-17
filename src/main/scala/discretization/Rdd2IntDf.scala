package discretization
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArraySeq

object Rdd2IntDf {
  def rdd2df(schemaString:String, solvedRdd:RDD[Seq[Int]], sparkSession: SparkSession):DataFrame={
    //将schemaString按空格返回字符串数组，并对每一个数组封装成StructField对象，进而构成了Array[StructField]-fields
    //第二个参数为各个字段的数据类型，第三个参数为字段是否可为空
    val fields = schemaString.split(" ")
      .map(fieldName=>StructField(fieldName,IntegerType,false))

    //将fields强制转换为StructType对象，形成了真正可用于构建DataFrame对象的schema
    val schema = StructType(fields)

    class CutIterator(it:Iterator[Seq[Int]]) extends Iterator[Object]{
      def hasNext():Boolean={
        it.hasNext
      }

      def next():Row={
        val temp=it.next()
        Row(temp:_*)
      }
    }

    //将solvedRdd转为Rdd[Row]
    val rowRdd = solvedRdd
      .mapPartitions(it=>{
        new CutIterator(it).asInstanceOf[Iterator[Row]]
      })

    //将schema应用于Rdd[Row]上，完成到DataFrame的转换
    val solvedTable = sparkSession.createDataFrame(rowRdd,schema)
    solvedTable
  }

}
