import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.streaming.DataStreamReader

/**
  * Created by Sharon on 1/20/19.
  */
object FareSelector {
  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setMaster("spark://ec2-18-211-110-36.compute-1.amazonaws.com:7077")
    .setAppName("Flight to DB")
    .set("spark.kafka.brokers","ec2-18-211-110-36.compute-1.amazonaws.com:9092,ec2-34-234-235-148.compute-1.amazonaws.com:9092")
    .set("spark.redis.host", "ec2-54-227-20-247.compute-1.amazonaws.com")
    .set("spark.redis.port", "6379")
  val spark: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()
  val sc: SparkContext = spark.sparkContext


//  val kafkaParams = Map[String, Object](
//    "bootstrap.servers" -> "ec2-18-211-108-91.compute-1.amazonaws.com:9092,ec2-34-237-251-142.compute-1.amazonaws.com:9092",
//    "key.deserializer" -> classOf[StringDeserializer],
//    "value.deserializer" -> classOf[StringDeserializer],
//    "group.id" -> "use_a_separate_group_id_for_each_stream",
//    "auto.offset.reset" -> "latest",
//    "enable.auto.commit" -> (false: java.lang.Boolean)
//  )

  val kafka = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", sparkConf.get("spark.kafka.brokers"))
    .option("subscribe", "flights")
    .option("startingOffsets", "latest")
    .load()

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.{explode, split}


    val kafkaData = kafka
      .withColumn("Key", col("key").cast(StringType))
      .withColumn("Topic", col("topic").cast(StringType))
      .withColumn("Offset", col("offset").cast(LongType))
      .withColumn("Partition", col("partition").cast(IntegerType))
      .withColumn("Timestamp", col("timestamp").cast(TimestampType))
      .withColumn("Value", col("value").cast(StringType))
      .select(split(col("Value"), ",")(5).as("timestamp"),
        split(col("Value"), ",")(0).as("origin"),
        split(col("Value"), ",")(3).as("dest"),
        split(col("Value"), ",")(2).as("fare"))
      .withColumn("key",concat(col("origin"), col("dest"), col("timestamp")))
    //      .groupBy("key").agg(avg(col("fare")))

    //      .select(get_json_object(col("value").cast("string"), "$.ORIGIN").alias("origin"),
    //        get_json_object(col("value").cast("string"), "$.DEST").alias("dest"),
    //        get_json_object(col("value").cast("string"), "$.FARE").alias("fare"),
    //        get_json_object(col("value").cast("string"), "$.EPOCH").alias("epochTime"))
    //      .groupBy(col("origin"),col("dest"), window(col("epochTime").cast("timestamp"), "60 minute"))
    //      .count()

    kafkaData.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()


    //      .groupBy("key").agg(avg(col("fare")))

    //      .select(get_json_object(col("value").cast("string"), "$.ORIGIN").alias("origin"),
    //        get_json_object(col("value").cast("string"), "$.DEST").alias("dest"),
    //        get_json_object(col("value").cast("string"), "$.FARE").alias("fare"),
    //        get_json_object(col("value").cast("string"), "$.EPOCH").alias("epochTime"))
    //      .groupBy(col("origin"),col("dest"), window(col("epochTime").cast("timestamp"), "60 minute"))
    //      .count()

//    import com.redislabs.provider.redis._
//    val redisDB = ("127.0.0.1", 6379)
//    sc.toRedisZSET(kafkaData, "flights", 600)

    kafkaData
        .write
//        .writeStream
      .format("org.apache.spark.sql.redis")
      .option("table", "flights")
      .option("key.column", kafkaData.hashCode())
//      .start()
      .save()

//    val count = kafka.select(get_json_object(col("value").cast("string"), "$.FARE").alias("fare")).count()

//    val windowfun = kafka.select(get_json_object((col("value")).cast("string"), "$.DEST").alias("dest"), get_json_object((col("value")).cast("timestamp"), "$.timestamp").alias("timestamp"))
//      .groupBy(col("dest"), window(col("timestamp").cast("timestamp"), "10 minute", "5 minute", "2 minute"))
//      .count()
//
//    println(count)
//    println(windowfun)
  }
}
