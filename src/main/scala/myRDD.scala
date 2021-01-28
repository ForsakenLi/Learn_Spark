import org.apache.spark.rdd.RDD
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object myRDD {

  def main1(args: Array[String]): Unit = {
    //创建sc对象
    val conf = new SparkConf().setAppName("rdd_practice").setMaster("local[6]")
    val sc = new SparkContext(conf)

    val timeStart = new Date().getTime
    // textFile 算子作用是创建一个 HadoopRDD
    val textRDD = sc.textFile("ff/deal.csv")
    println("总交易数为：" + textRDD.count())

    //使用cache提高程序性能，因为splitRDD被多次重复使用了
    val splitRDD: RDD[Array[String]] = textRDD.map(_.split("\t")).cache()
    val cidRDD: RDD[String] = splitRDD.map(x => x(0))
    val cfidRDD: RDD[String] = splitRDD.map(x => x(1))
    val uniRDD: RDD[String] = cidRDD.union(cfidRDD).distinct()
    println("总用户数为：" + uniRDD.count())

    val income: Double = splitRDD.filter(x => x(2) == "1" || x(2) == "3").map(x => x(4).toDouble).sum()
    val expend: Double = splitRDD.filter(x => x(2) == "2" || x(2) == "4").map(x => x(4).toDouble).sum()
    println("总支出为：" + expend)
    println("总收入为：" + income)
    val timeEnd = new Date().getTime
    val format = new SimpleDateFormat("mm:ss.SSS")
    println("此操作消耗时间为："+ format.format(timeEnd - timeStart))

  }

}
