import org.apache.spark.sql.streaming.Trigger
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
object FareSelector extends SparkSessionBuilder{
  val spark = buildSparkSession

//  val sparkConf = new SparkConf()
//    .setMaster("local[2]")
//    .setMaster("spark://ec2-18-211-110-36.compute-1.amazonaws.com:7077")
//    .setAppName("Flight to DB")
//    .set("spark.kafka.brokers","ec2-18-211-110-36.compute-1.amazonaws.com:9092,ec2-34-234-235-148.compute-1.amazonaws.com:9092")
//    .set("spark.redis.host", "ec2-3-86-129-28.compute-1.amazonaws.com")
//    .set("spark.redis.port", "6379")
//    .set("spark.worker.memory", "6g")
//    .set("spark.executor.memory", "3g")
//    .set("spark.executor.cores", "2")
//    .set("spark.worker.cores", "1")
//  val spark: SparkSession =
//    SparkSession.builder().config(sparkConf).getOrCreate()
//  val sc: SparkContext = spark.sparkContext


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
    .option("kafka.bootstrap.servers", "ec2-18-211-110-36.compute-1.amazonaws.com:9092,ec2-34-234-235-148.compute-1.amazonaws.com:9092")
    .option("subscribe", "april")
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
      .withColumn("write_time", col("timestamp").cast(TimestampType))
      .withColumn("Value", regexp_extract(col("value").cast(StringType), "\\[(.*)\\]", 1))
      .select(split(col("Value"), ",")(5).as("datetime"),
        split(col("Value"), ",")(0).as("origin"),
        split(col("Value"), ",")(3).as("dest"),
        split(col("Value"), ",")(2).as("fare"),
        unix_timestamp().as("write_time_ms"),
        col("write_time"))
      .withColumn("key",concat(col("origin"), lit("@"),col("dest"),  lit("@"),col("datetime"), lit("@"), col("write_time_ms")))
//       .groupBy(col("key")).agg(min(col("fare")).as("best_fare"), avg(col("fare")).as("avg_cost"), count(col("fare")).as("num_of_flights"))
      .select("key","fare")


//        kafkaData.writeStream.outputMode("append").format("console").option("truncate", false).trigger(Trigger.ProcessingTime("5 seconds")).start().awaitTermination()



    val sink = kafkaData
      .writeStream
//      .outputMode("complete")
      .foreach({

      new RedisSink
    })
        .queryName("Spark Struc Stream to Redis")
//      .format("org.apache.spark.sql.redis")
//      .option("table", "flights")
//      .option("key.column", kafkaData.hashCode())
      .start()


    sink.awaitTermination()
//    val count = kafka.select(get_json_object(col("value").cast("string"), "$.FARE").alias("fare")).count()

//    val windowfun = kafka.select(get_json_object((col("value")).cast("string"), "$.DEST").alias("dest"), get_json_object((col("value")).cast("timestamp"), "$.timestamp").alias("timestamp"))
//      .groupBy(col("dest"), window(col("timestamp").cast("timestamp"), "10 minute", "5 minute", "2 minute"))
//      .count()
//
//    println(count)
//    println(windowfun)
  }
}
