import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilderForKafkaStreams extends Serializable {
  // Build a spark session. Class is made serializable in order to access SparkSession in a driver and executors.
  // Note here the usage of @transient lazy val

  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setAppName("Kafka Streams")
      .setMaster("spark://ec2-18-211-110-36.compute-1.amazonaws.com:6066")
//      .setMaster("local[2]")
      .set("spark.kafka.broker","ec2-18-211-110-36.compute-1.amazonaws.com:9092")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "1g")
      .set("spark.deploy.mode","cluster")
    @transient lazy val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark
  }
}