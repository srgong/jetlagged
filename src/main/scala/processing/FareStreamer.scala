package processing

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Sharon on 1/22/19.
  */
object FareStreamer {
  val sparkConf = new SparkConf()
    .setAppName("Fare Streamer")


  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val broker = sparkConf.get("spark.kafka.producer")
    case class KafkaProducerConfigs(brokerList: String = broker) {
      val properties = new Properties()
      properties.put("bootstrap.servers", brokerList)
      properties.put("key.serializer", classOf[StringSerializer])
      properties.put("value.serializer", classOf[StringSerializer])
    }

    val topic = sparkConf.get("spark.kafka.topic")
    val ds = sc.textFile(sparkConf.get("spark.hdfs.in"))

    while(true) {
      ds.foreachPartition { eachPartition => {
        val kProducer = new KafkaProducer[String, String](KafkaProducerConfigs().properties)
        eachPartition.foreach { eachElement => {
          val pricing_time = (System.currentTimeMillis / 1000).toString
          val record = eachElement.substring(1, eachElement.length-1).concat(","+pricing_time)
          println(record)
          val kMessage = new ProducerRecord[String, String](topic, null, record)
          kProducer.send(kMessage)
        }}
        kProducer.close()
      }}
    }
  }

}
