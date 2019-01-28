import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by Sharon on 1/22/19.
  */
object FareStreamer {
  val sparkConf = new SparkConf()
    .setAppName("Fare Streamer")
//    .setMaster("spark://ec2-18-211-110-36.compute-1.amazonaws.com:7077")
//    .set("deploy-mode", "cluster")
//    .set("spark.kafka.broker","spark://ec2-18-211-110-36.compute-1.amazonaws.com:9092")
  val spark: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext: SQLContext = spark.sqlContext

  import org.apache.kafka.clients.producer.KafkaProducer
  import org.apache.kafka.clients.producer.ProducerRecord

  /**
    * Replicates same flight information (to/from/when)
    * @param df
    * @param n
    * @return
    */
  def replicate(df: DataFrame, n: Int) = {
    df.withColumn("dummy", explode(array((1 until n).map(lit): _*)))
      .drop("dummy")
  }

  def generateFare(df: DataFrame): DataFrame = {
    val MIN_FARE=35
    val MAX_FARE=543
    val MEAN=253
    val STDDEV=60

    val withFare = df.withColumn("RIGHTSKEWDISTR", round(randn(seed=10)*STDDEV+MEAN,2))
    withFare.withColumn("fare",
      when(col("RIGHTSKEWDISTR") < MIN_FARE, scala.util.Random.nextInt((75 - 35) + 1) + 35)
        .when(col("RIGHTSKEWDISTR") > MAX_FARE, MAX_FARE)
        .otherwise(col("RIGHTSKEWDISTR")))
  }

  /**
    * Creates normalized time columns - timestamp, epoch time
    * @param df
    * @return
    */
  def getEpoch(df: DataFrame): DataFrame = {
    val withTime = df.withColumn("time", concat(col("fl_date"),lit(" "),col("crs_dep_time")))
    val withEpoch = withTime.withColumn("epoch",unix_timestamp(col("time"),"yyyy-MM-dd HHmm"))
    withEpoch.withColumn("timestamp",to_timestamp(col("epoch")))
  }

  def main(args: Array[String]): Unit = {
    val broker = sparkConf.get("spark.kafka.broker") //"127.0.0.1:9092"

    case class KafkaProducerConfigs(brokerList: String = broker) {
      val properties = new Properties()
      properties.put("bootstrap.servers", brokerList)
      properties.put("key.serializer", classOf[StringSerializer])
      properties.put("value.serializer", classOf[StringSerializer])
      //    properties.put("serializer.class", classOf[StringDeserializer])
      //    properties.put("batch.size", 16384)
      //    properties.put("linger.ms", 1)
      //    properties.put("buffer.memory", 33554432)
    }

    val customSchema = StructType(Array(
      StructField("year", StringType, true),
      StructField("quarter", IntegerType, true),
      StructField("month", IntegerType, true),
      StructField("day_of_month", IntegerType, true),
      StructField("day_of_week", IntegerType, true),
      StructField("fl_date", StringType, true),
      StructField("op_unique_carrier", StringType, true),
      StructField("tail_num", StringType, true),
      StructField("op_carrier_fl_num", StringType, true),
      StructField("origin", StringType, true),
      StructField("origin_city_name", StringType, true),
      StructField("dest", StringType, true),
      StructField("dest_city_name", StringType, true),
      StructField("crs_dep_time", StringType, true),
      StructField("crs_arr_time", StringType, true),
      StructField("crs_elapsed_time", DoubleType, true),
      StructField("distance", DoubleType, true),
      StructField("distance_group", IntegerType, true)
    ))

//    val df = sqlContext.read.json("src/main/resources/json").rdd
//    val df = sqlContext.read.option("header", "true").csv("src/main/resources/csv")
    val df = sqlContext.read
      .option("header", "true")
      .option("inferSchema", "true")
    .schema(customSchema)
  .csv("hdfs://ec2-18-211-110-36.compute-1.amazonaws.com:9000/user/ubuntu/201712.csv")
    val withEpoch = getEpoch(df).filter("epoch is not null").select("epoch","timestamp","origin","dest")
    val withReplication = replicate(withEpoch,100)
    val withFare = generateFare(withReplication).rdd

    withFare.foreachPartition { eachPartition => {
      val kProducer = new KafkaProducer[String, String](KafkaProducerConfigs().properties)
      eachPartition.toList.foreach { eachElement => {
        val kMessage = new ProducerRecord[String, String]("flights", null, eachElement.toString())
        kProducer.send(kMessage)
      }}}
    }
  }

}
