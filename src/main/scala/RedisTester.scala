import org.apache.spark.sql.SparkSession
import com.redislabs.provider.redis._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ideas to try : diff package just 2.3.1
  * deploy redis in cluster mode reread docs on how to. clusters shouldn't have the same name apparently?
  * Created by Sharon on 1/23/19.
  */
object RedisTester {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("redis-df")
      .master("local[2]")
      .config("spark.redis.host", "ec2-18-211-110-36.compute-1.amazonaws.com")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val personSeq = Seq(Person("John", 30), Person("Peter", 45))
    val df = spark.createDataFrame(personSeq)

    val redisServerDnsAddress = "ec2-18-211-110-36.compute-1.amazonaws.com"
    val redisPortNumber = 6379
    val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, null))

    println(redisConfig.hosts.length)

    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .option("key.column", "name")
      .save()
  }
}
