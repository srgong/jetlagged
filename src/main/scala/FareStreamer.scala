import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{SparkSession}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by Sharon on 1/22/19.
  */
object FareStreamer {
  val sparkConf = new SparkConf()
    .setAppName("Fare Streamer")
//    .setMaster("local[*]")
//    .set("spark.hdfs.flights","src/main/resources/json/sample/")
//    .set("spark.kafka.producer","ec2-18-211-110-36.compute-1.amazonaws.com:9092")
//    .set("spark.kafka.topic","local_e")


  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
//    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

    val broker = sparkConf.get("spark.kafka.producer")
    case class KafkaProducerConfigs(brokerList: String = broker) {
      val properties = new Properties()
      properties.put("bootstrap.servers", brokerList)
      properties.put("key.serializer", classOf[StringSerializer])
      properties.put("value.serializer", classOf[StringSerializer])
    }

    val topic = sparkConf.get("spark.kafka.topic")
//    val ds = sqlContext.read.json(sparkConf.get("spark.hdfs.flights"))
    val ds = sc.textFile(sparkConf.get("spark.hdfs.flights"))

    while(true) {
      ds.foreachPartition { eachPartition => {
        val kProducer = new KafkaProducer[String, String](KafkaProducerConfigs().properties)
        eachPartition.foreach { eachElement => {
          Thread.sleep(1000)
          val pricing_time = (System.currentTimeMillis / 1000).toString
          val record = eachElement.mkString(",").concat(","+pricing_time)
          println(record)
          val kMessage = new ProducerRecord[String, String](topic, null, record)
          kProducer.send(kMessage)
        }}
        kProducer.close()
      }}
    }
  }

}
