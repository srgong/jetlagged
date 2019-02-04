import com.redis.{RedisClient, RedisClientPool}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.split

/**
  * Created by Sharon on 1/20/19.
  */
object FareSelector {
  val sparkConf = new SparkConf()
    .setAppName("Flight to DB")
    .setMaster("local[*]")
    .set("spark.kafka.brokers","ec2-18-211-110-36.compute-1.amazonaws.com:9092,ec2-18-211-107-25.compute-1.amazonaws.com:9092,ec2-34-234-235-148.compute-1.amazonaws.com:9092,ec2-54-227-20-247.compute-1.amazonaws.com:9092")
    .set("spark.kafka.topic", "client_h")
    .set("spark.kafka.startingOffsets","latest")
    .set("spark.redis.port","6379")
    .set("spark.redis.host","ec2-3-86-129-28.compute-1.amazonaws.com")
    .set("spark.driver.memory","2g")
    .set("spark.executor.memory","3g")
    .set("spark.executor.cores","2")

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()


    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sparkConf.get("spark.kafka.brokers"))
      .option("subscribe", sparkConf.get("spark.kafka.topic"))
      .option("startingOffsets", sparkConf.get("spark.kafka.startingOffsets"))
      .load()

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
      .withColumn("key",concat(col("origin"), lit("@"),col("dest"),  lit("@"),col("datetime")))
        .withColumn("field", concat(col("processingTime_ms")))
       .groupBy(col("key"),col("field")).agg(min(col("fare")).as("best_fare"), avg(col("fare")).as("avg_cost"), count(col("fare")).as("num_of_flights"))
      .select("key","best_fare")

//     kafkaData
//     .writeStream
//     .outputMode("append")
//     .format("console")
//     .option("truncate", false)
//     .trigger(Trigger.ProcessingTime("5 seconds"))
//     .start()
//     .awaitTermination()

    val sink = kafkaData
      .writeStream
      .outputMode("complete")
      .foreach({
          new RedisSink
      })
      .queryName("Spark Struc Stream to Redis")
      .start()

    sink.awaitTermination()
  }
}
