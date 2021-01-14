import junit.framework.Test
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  /* 这是我的第一个 Scala 程序
   * 以下程序将输出'Hello World!'
   */
  def main(args: Array[String]) {
    // 1. 创建 Spark Context
    val conf = new SparkConf().setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 2. 读取文件并计算词频
    val source: RDD[String] = sc.textFile("hdfs://lmc1:8020/dataset/wordcount.txt", 2)
    val words: RDD[String] = source.flatMap { line => line.split(" ") }
    val wordsTuple: RDD[(String, Int)] = words.map { word => (word, 1) }
    val wordsCount: RDD[(String, Int)] = wordsTuple.reduceByKey { (x, y) => x + y }

    // 3. 查看执行结果
    val result = wordsCount.collect()
    result.foreach(item => println(item))

  }
}