import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by Sharon on 1/22/19.
  */
object FareStreamer {
  val sparkConf = new SparkConf()
    .setAppName("Fare Streamer")
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
    withFare.withColumn("FARE",
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
    val withTime = df.withColumn("TIME", concat(col("FL_DATE"),lit(" "),col("CRS_DEP_TIME")))
    val withEpoch = withTime.withColumn("EPOCH",unix_timestamp(col("time"),"yyyy-MM-dd HHmm"))
    withEpoch.withColumn("TIMESTAMP",to_timestamp(col("EPOCH")))
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

//    val df = sqlContext.read.json("src/main/resources/json").rdd
    val df = sqlContext.read.option("header", "true").csv("src/main/resources/csv")
    val withEpoch = getEpoch(df).filter("EPOCH is not null").select("EPOCH","TIMESTAMP","ORIGIN","DEST")
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
