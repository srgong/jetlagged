package processing


import connector.RedisSink
import model.Flight

/**
  * Created by Sharon on 1/20/19.
  */
object FareSelector {
  val sparkConf = new SparkConf()
    .setAppName("Flight to DB")

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
      .withColumn("record", split(col("value"),","))
      .select(
        col("record")(0).as("date"),
        col("record")(1).as("time"),
        col("record")(2).as("from"),
        col("record")(3).as("to"),
        col("record")(4).as("last_to"),
        col("record")(5).as("last_time"),
        col("record")(6).as("fare"),
        col("record")(7).as("updated_ms"),
        unix_timestamp().as("processed_ms")).as[Flight]

    val sink = kafkaData
      .writeStream
      .outputMode("append")
      .foreach(new RedisSink)
      .queryName("Spark Struc Stream to Redis")
      .start()

    sink.awaitTermination()
  }
}
