import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ReadTopic {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("read_topic")
      .master("local[6]")
      .getOrCreate()

    val schemaType = new StructType()
      .add("type",StringType)
      .add("optional",BooleanType)

    val schema = new StructType()
      .add("schema", schemaType)
      .add("payload",StringType)

    import spark.implicits._

    //从 Kafka 读取数据, 生成源数据集
    //连接 Kafka 生成 DataFrame
    //从 DataFrame 中取出表示 Kafka 消息内容的 value 列并转为 String 类型
    val source = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "lmc1:9092,lmc2:9092,lmc3:9092")
      .option("subscribe", "connect-test")
      .option("failOnDataLoss","false")
      .option("auto.offset.reset","earliest")
      .option("startingOffsets", "earliest")
      .load() //此时还是DataFrame格式
      .selectExpr("CAST(value AS STRING) as value")
      .as[String] //此时还是JSON格式的DataSet[String]

    import org.apache.spark.sql.functions._

      //alias给划分结果重命名
      val res = source.select(from_json('value, schema).alias("parsed_value"))
        .selectExpr("parsed_value.payload") //这里是DataFrame
        .as[String].map {
      item =>
        val arr = item.split("\t")
        (arr(0), arr(1), arr(2).toInt, arr(4).toDouble)
    }
      .as[(String, String, Int, Double)]
      //.toDF("c_id", "c_friend_id", "c_type", "c_amount")
        .where(" _3 == 1 or _3 == 3").selectExpr("SUM(_4) as total_income")
//
//    val rdd1 = res.select('c_id).queryExecution.toRdd
//    val rdd2 = res.select('c_friend_id).queryExecution.toRdd
//    val rdd3 = rdd1.union(rdd2).distinct()
//    println("总用户数为：" + rdd3.count())

    //val income = res.where(" c_type == 1 or c_type == 3").selectExpr("SUM(c_amount) as total_income")

    //res.where(" c_type == 2 or c_type == 4").selectExpr("SUM(c_amount) as total_expand").show()

    //落地到本地文件
    res.writeStream
      .format("console") // 也可以是 "orc", "json", "csv" 等
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()

    //落地到Kafka
//    result.writeStream
//      .format("kafka")
//      .outputMode(OutputMode.Append())
//      .option("checkpointLocation","/Users/limiaochen/IdeaProjects/Learn_Spark/ff")
//      .option("kafka.bootstrap.servers", "lmc1:9092,lmc2:9092,lmc3:9092")
//      .option("topic", "first")
//      .start()
//      .awaitTermination()
  }

}
