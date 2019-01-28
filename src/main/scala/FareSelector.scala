
import org.apache.spark.{SparkConf, SparkContext, streaming}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by Sharon on 1/20/19.
  */
object FareSelector {
  val sparkConf = new SparkConf()
//    .setMaster("local[2]")
    .setMaster("spark://ec2-18-211-110-36.compute-1.amazonaws.com:7077")
    .setAppName("Flight to DB")
    .set("spark.kafka.brokers","ec2-34-234-235-148.compute-1.amazonaws.com:9092")
    .set("spark.redis.host", "ec2-54-227-20-247.compute-1.amazonaws.com")
    .set("spark.redis.port", "6379")
    .set("spark.deploy.mode", "client")
    .set("spark.driver.cores", "1")
    .set("spark.executor.memory", "4g")
    .set("spark.driver.memory", "1g")

  val spark: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))
  val sqlContext: SQLContext = spark.sqlContext

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

//
    val kafkaData = kafka
      .withColumn("Key", col("key").cast(StringType))
      .withColumn("Topic", col("topic").cast(StringType))
      .withColumn("Offset", col("offset").cast(LongType))
      .withColumn("Partition", col("partition").cast(IntegerType))
      .withColumn("created_time", col("timestamp").cast(TimestampType))
      .withColumn("value", col("value").cast(StringType))
      .select(split(col("value"), ",")(5).as("departureTime"),
        split(col("value"), ",")(0).as("origin"),
        split(col("value"), ",")(3).as("dest"),
        split(col("value"), ",")(2).as("fare"),
        col("created_time"))
      .withColumn("key",concat(col("origin"), col("dest"), col("departureTime"), col("created_time")))
//          .groupBy("key").agg(min(col("fare")))

    //      .select(get_json_object(col("value").cast("string"), "$.ORIGIN").alias("origin"),
    //        get_json_object(col("value").cast("string"), "$.DEST").alias("dest"),
    //        get_json_object(col("value").cast("string"), "$.FARE").alias("fare"),
    //        get_json_object(col("value").cast("string"), "$.EPOCH").alias("epochTime"))
    //      .groupBy(col("origin"),col("dest"), window(col("epochTime").cast("timestamp"), "60 minute"))
    //      .count()

    kafkaData.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

      import scala.concurrent.duration._
//    kafkaData.writeStream.format("memory").queryName("flights").outputMode("complete").start

//    kafkaData
//        .write
//      .writeStream
//      .format("org.apache.spark.sql.redis")
//      .option("table", "flights")
//      .option("key.column", kafkaData.hashCode())
//          .save()
//          .start()

    println("done?")

//        .write
////      .foreach()
////        .writeStream
//      .format("org.apache.spark.sql.redis")
//      .option("table", "flights")
//      .option("key.column", kafkaData.hashCode())
////      .start()
//      .save()

//    val count = kafka.select(get_json_object(col("value").cast("string"), "$.FARE").alias("fare")).count()

//    val windowfun = kafka.select(get_json_object((col("value")).cast("string"), "$.DEST").alias("dest"), get_json_object((col("value")).cast("timestamp"), "$.timestamp").alias("timestamp"))
//      .groupBy(col("dest"), window(col("timestamp").cast("timestamp"), "10 minute", "5 minute", "2 minute"))
//      .count()
//
//    println(count)
//    println(windowfun)


  }
}
