import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by Sharon on 1/22/19.
  */
object FareStreamer {
  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Fare Streamer")
    .set("spark.kafka.producer","ec2-18-211-107-25.compute-1.amazonaws.com:9092")
  val spark: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext: SQLContext = spark.sqlContext

  import org.apache.kafka.clients.producer.KafkaProducer
  import org.apache.kafka.clients.producer.ProducerRecord

  def main(args: Array[String]): Unit = {
    val broker = sparkConf.get("spark.kafka.producer") //"127.0.0.1:9092"

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

    val df = sqlContext.read.json("src/main/resources/json").rdd

    df.foreachPartition { eachPartition => {
      val kProducer = new KafkaProducer[String, String](KafkaProducerConfigs().properties)
      eachPartition.toList.foreach { eachElement => {
        val kMessage = new ProducerRecord[String, String]("april", null, eachElement.toString())
        kProducer.send(kMessage)
      }}}
    }
  }

}
