import java.util.Properties

import FareFinder.sqlContext
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by Sharon on 1/22/19.
  */
object FareStreamer {

  import org.apache.kafka.clients.producer.KafkaProducer
  import org.apache.kafka.clients.producer.ProducerRecord

  def main(args: Array[String]): Unit = {
    val broker = args(0) //"127.0.0.1:9092"
    val partition = args(1)

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

    val df = sqlContext.read.format("avro").load("src/main/resources/out").rdd
    val kProducer = new KafkaProducer[String, String](KafkaProducerConfigs().properties)

    df.foreachPartition { eachPartition => {
      eachPartition.toList.foreach { eachElement => {
        println(eachElement.toString())
        val kMessage = new ProducerRecord[String, String]("flights", partition, eachElement.toString())
        kProducer.send(kMessage)
      }}}
    }
  }

}
