import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.Test

import java.text.SimpleDateFormat
import java.util.Date

class TestUnit{
  @Test
  def dataset3(): Unit = {
    // 1. 创建 SparkSession
    val spark = new sql.SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 2. 导入隐式转换
    import spark.implicits._

    val timeStart = new Date().getTime
    val dataset: Dataset[Deal] = spark.createDataset(Seq(Deal("1","2",1,100),Deal("1","3",2,500),Deal("3","2",3,605),Deal("2","3",4,10)))
    val rdd: RDD[InternalRow] = dataset.queryExecution.toRdd
    println("用户总数为："+rdd.collect().length)
    val timeEnd = new Date().getTime
    val format = new SimpleDateFormat("mm:ss")
    println("此操作消耗时间为："+ format.format(timeEnd - timeStart))


  }
}
