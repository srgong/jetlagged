import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by Sharon on 1/22/19.
  */
object FareStreamer {
  val sparkConf = new SparkConf()
    .setAppName("Fare Streamer")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext: SQLContext = spark.sqlContext

    val broker = sparkConf.get("spark.kafka.producer")
    case class KafkaProducerConfigs(brokerList: String = broker) {
      val properties = new Properties()
      properties.put("bootstrap.servers", brokerList)
      properties.put("key.serializer", classOf[StringSerializer])
      properties.put("value.serializer", classOf[StringSerializer])
      //    properties.put("batch.size", 16384)
      //    properties.put("linger.ms", 1)
      //    properties.put("buffer.memory", 33554432)
    }

    val filepath = sparkConf.get("spark.hdfs.filepath")
    val df = sqlContext.read.json(filepath).rdd

    val topic = sparkConf.get("spark.kafka.topic")
    df.foreachPartition { eachPartition => {
      val kProducer = new KafkaProducer[String, String](KafkaProducerConfigs().properties)
      eachPartition.toList.foreach { eachElement => {
        val kMessage = new ProducerRecord[String, String](topic, null, eachElement.toString())
        kProducer.send(kMessage)
      }}
      kProducer.close()
    }}
  }

}
