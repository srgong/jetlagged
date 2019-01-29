import org.apache.spark.sql.SparkSession
import com.redislabs.provider.redis._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ideas to try : diff package just 2.3.1
  * deploy redis in cluster mode reread docs on how to. clusters shouldn't have the same name apparently?
  * Created by Sharon on 1/23/19.
  */
object RedisTester {
  case class Flight(id: String, fare: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("redis-df")
      .master("local[2]")
//      .master("spark://ec2-18-211-110-36.compute-1.amazonaws.com:7077")
//      .config("spark.redis.host", "ec2-54-227-20-247.compute-1.amazonaws.com") // spark
      .config("spark.redis.host", "ec2-18-211-110-36.compute-1.amazonaws.com") // sharon-db
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val flightSeq = Seq(Flight("MSP@MKE@2017-12-16T06:30:00.000-05:00", 224.96),
      Flight("MSP@MKE@2017-12-16T06:30:00.000-05:00@1548799951", 157.44),
      Flight("MSP@MKE@2017-12-16T06:30:00.000-05:00@1548799951", 310.04),
      Flight("MSP@MKE@2017-12-16T06:30:00.000-05:00@1548799951", 391.71),
      Flight("MSP@MKE@2017-12-16T06:30:00.000-05:00@1548799951", 232.96),
      Flight("LIH@LAX@2017-12-02T16:46:00.000-05:00@1548799951", 172.12),
      Flight("LIH@LAX@2017-12-02T16:46:00.000-05:00@1548799951", 378.52),
      Flight("LIH@LAX@2017-12-02T16:46:00.000-05:00@1548799951", 231.18),
      Flight("LIH@LAX@2017-12-02T16:46:00.000-05:00@1548799951", 219.96)
    )

    val df = spark.createDataFrame(flightSeq)

    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "april-connect")
      .option("key.column", "id")
      .save()
  }
}
