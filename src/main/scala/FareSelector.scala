import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}

/**
  * Created by Sharon on 1/20/19.
  */
object FareSelector {
  val sparkConf = new SparkConf()
    .setMaster("spark://ec2-18-211-110-36.compute-1.amazonaws.com:7077")
    .setAppName("Flight to DB")
    .set("spark.kafka.brokers","ec2-18-211-110-36.compute-1.amazonaws.com:9092,ec2-34-234-235-148.compute-1.amazonaws.com:9092")
    .set("spark.redis.host", "ec2-3-86-129-28.compute-1.amazonaws.com")
    .set("spark.redis.port", "6379")
    .set("spark.worker.memory", "6g")
    .set("spark.executor.memory", "3g")
    .set("spark.executor.cores", "2")
    .set("spark.worker.cores", "1")
  val spark: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()
  val sc: SparkContext = spark.sparkContext

  val kafka = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "ec2-18-211-110-36.compute-1.amazonaws.com:9092,ec2-34-234-235-148.compute-1.amazonaws.com:9092")
    .option("subscribe", "april")
    .option("startingOffsets", "latest")
    .load()

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.{split}


    val kafkaData = kafka
      .withColumn("Key", col("key").cast(StringType))
      .withColumn("Topic", col("topic").cast(StringType))
      .withColumn("Offset", col("offset").cast(LongType))
      .withColumn("Partition", col("partition").cast(IntegerType))
      .withColumn("processingTime", col("timestamp").cast(TimestampType))
      .withColumn("Value", regexp_extract(col("value").cast(StringType), "\\[(.*)\\]", 1))
      .select(split(col("Value"), ",")(5).as("datetime"),
        split(col("Value"), ",")(0).as("origin"),
        split(col("Value"), ",")(3).as("dest"),
        split(col("Value"), ",")(2).as("fare"),
        unix_timestamp().as("processingTime_ms"),
      col("processingTime"))
      .withColumn("key",concat(col("origin"), lit("@"),col("dest"),  lit("@"),col("datetime"), lit("@"), col("processingTime_ms")))
//       .groupBy(col("key")).agg(min(col("fare")).as("best_fare"), avg(col("fare")).as("avg_cost"), count(col("fare")).as("num_of_flights"))
      .select("key","fare","processingTime")

//     kafkaData
//     .writeStream
//     .outputMode("append")
//     .format("console")
//     .option("truncate", false)
////     .trigger(Trigger.ProcessingTime("5 seconds"))
//     .start()
//     .awaitTermination()
//
    val sink = kafkaData
      .writeStream
//      .outputMode("complete")
      .foreach(new RedisSink)
      .queryName("Spark Struc Stream to Redis")
      .start()

    sink.awaitTermination()
  }
}
