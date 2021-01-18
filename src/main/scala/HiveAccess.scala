import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date

object HiveAccess {

  def main(args: Array[String]): Unit = {
    //    0. 创建 SparkSession
    //    1. 开启 Hive 支持
    //    2. 指定 Metastore 的位置
    //    3. 指定 Warehouse 的位置
    val spark = SparkSession.builder()
      .appName("hive access1")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://lmc3:9083")
      .config("spark.sql.warehouse.dir", "/dataset/hive")
      .getOrCreate()

    var timeStart = new Date().getTime
    spark.sql("use myhive")
    //获取用户总数
    //对所有的c_id进行去重
    val ds1 = spark.sql("select c_id from mytable")
    val rdd1 = ds1.queryExecution.toRdd
    println("交易总数为："+rdd1.collect().length)
    //对所有的c_friend_id进行去重
    val ds2 = spark.sql("select c_friend_id from mytable")
    //求并集
    val res = ds1.union(ds2).distinct()
    val rdd = res.queryExecution.toRdd
    println("用户总数为："+rdd.collect().length)
    var timeEnd = new Date().getTime
    val format = new SimpleDateFormat("mm:ss.SSS")
    println("此操作消耗时间为："+ format.format(timeEnd - timeStart))

    timeStart = new Date().getTime
    //println("总收入为：")
    spark.sql("select USM(c_amount) as total_income from mytable where c_type == 1 or c_type == 3").show()
    //println("总支出为：")
    spark.sql("select SUM(c_amount) as total_expend from mytable where c_type == 2 or c_type == 4").show()
    timeEnd = new Date().getTime
    println("此操作消耗时间为："+ format.format(timeEnd - timeStart))
  }
}
