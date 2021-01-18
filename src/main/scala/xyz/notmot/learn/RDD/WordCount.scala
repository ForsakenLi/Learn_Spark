package xyz.notmot.learn.RDD


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class WordCount {

  def wc(args: Array[String]): Unit = {
    // 1. 创建SparkContext
    val conf = new SparkConf().setAppName("word_count")
    val sc = new SparkContext(conf)

    // 2. 加载文件
    //     1. 准备文件
    //     2. 读取文件

    // RDD 特点:
    // 1. RDD是数据集
    // 2. RDD是编程模型
    // 3. RDD相互之间有依赖关系
    // 4. RDD是可以分区的
    val rdd1: RDD[String] = sc.textFile("hdfs:///data/wordcount.txt")

    // 3. 处理
    //     1. 把整句话拆分为多个单词
    val rdd2: RDD[String] = rdd1.flatMap(item => item.split(" ") )
    //     2. 把每个单词指定一个词频1
    val rdd3: RDD[(String, Int)] = rdd2.map(item => (item, 1) )
    //     3. 聚合
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((curr, agg) => curr + agg )

    // 4. 得到结果
    val result: Array[(String, Int)] = rdd4.collect()
    result.foreach(item => println(item))
  }

  
  def sparkContext(): Unit = {
    // 1. Spark Context 如何编写
    //     1. 创建 SparkConf
    val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context")
    //     2. 创建 SparkContext
    val sc = new SparkContext(conf)

    // SparkContext身为大入口API, 应该能够创建 RDD, 并且设置参数, 设置Jar包...
    //    sc....

    // 2. 关闭 SparkContext, 释放集群资源
  }

  val conf = new SparkConf().setMaster("local[6]").setAppName("spark_context")
  val sc = new SparkContext(conf)

  // 从本地集合创建
  
  def rddCreationLocal(): Unit = {
    val seq = Seq(1, 2, 3)
    val rdd1: RDD[Int] = sc.parallelize(seq, 2)
    sc.parallelize(seq)
    val rdd2: RDD[Int] = sc.makeRDD(seq, 2)
  }

  // 从文件创建
  
  def rddCreationFiles(): Unit = {
    sc.textFile("file:///...")

    // 1. textFile 传入的是什么
    //    * 传入的是一个 路径, 读取路径
    //    * hdfs://  file://   /.../...(这种方式分为在集群中执行还是在本地执行, 如果在集群中, 读的是hdfs, 本地读的是文件系统)
    // 2. 是否支持分区?
    //    * 假如传入的path是 hdfs:///....
    //    * 分区是由HDFS中文件的block决定的
    // 3. 支持什么平台
    //    * 支持aws和阿里云
  }

  // 从RDD衍生
  
  def rddCreateFromRDD(): Unit = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3))
    // 通过在rdd上执行算子操作, 会生成新的 rdd
    // 原地计算
    // str.substr 返回新的字符串, 非原地计算
    // 和字符串中的方式很像, 字符串是可变的吗?
    // RDD可变吗?不可变
    val rdd2: RDD[Int] = rdd1.map(item => item)
  }

  
  def mapTest(): Unit = {
    // 1. 创建 RDD
    val rdd1 = sc.parallelize(Seq(1, 2, 3))
    // 2. 执行 map 操作
    val rdd2 = rdd1.map( item => item * 10 )
    // 3. 得到结果
    val result: Array[Int] = rdd2.collect()
    result.foreach(item => println(item))
  }

  
  def flatMapTest(): Unit = {
    // 1. 创建 RDD
    val rdd1 = sc.parallelize(Seq("Hello lily", "Hello lucy", "Hello tim"))
    // 2. 处理数据
    val rdd2 = rdd1.flatMap( item => item.split(" ") )
    // 3. 得到结果
    val result = rdd2.collect()
    result.foreach(item => println(item))
    // 4. 关闭sc
    sc.stop()
  }

  
  def reduceByKeyTest(): Unit = {
    // 1. 创建 RDD
    val rdd1 = sc.parallelize(Seq("Hello lily", "Hello lucy", "Hello tim"))
    // 2. 处理数据
    val rdd2 = rdd1.flatMap( item => item.split(" ") )
      .map( item => (item, 1) )
      .reduceByKey( (curr, agg) => curr + agg )
    // 这里的reduceByKey后面的函数式的含义是，当遇到两个具有相同key的元组时
    // 会将一个（当前汇总）和另一个的value加起来，后面的函数式指的是这两个value的操作，
    // 可以简记为_+_
    // https://www.jianshu.com/p/af175e66ce99

    // 3. 得到结果
    val result = rdd2.collect()
    result.foreach(item => println(item))
    // 4. 关闭sc
    sc.stop()
  }

  
  def combineByKeyTest(): Unit = {
    val rdd = sc.parallelize(Seq(
      ("zhangsan", 99.0),
      ("zhangsan", 96.0),
      ("lisi", 97.0),
      ("lisi", 98.0),
      ("zhangsan", 97.0))
    )

    val combineRdd = rdd.combineByKey(
      score => (score, 1),
      (scoreCount: (Double, Int),newScore) => (scoreCount._1 + newScore, scoreCount._2 + 1),
      (scoreCount1: (Double, Int), scoreCount2: (Double, Int)) =>
        (scoreCount1._1 + scoreCount2._1, scoreCount1._2 + scoreCount2._2)
    )

    val meanRdd = combineRdd.map(score => (score._1, score._2._1 / score._2._2))

    val res = meanRdd.collect()
    res.foreach(item => println(item))
  }

}
