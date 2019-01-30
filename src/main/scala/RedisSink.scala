import com.redis.{RedisClient, RedisClientPool}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Created by Sharon on 1/25/19.
  */



class SparkSessionBuilder extends Serializable {
  // Build a spark session. Class is made serializable so to get access to SparkSession in a driver and executors.
  // Note here the usage of @transient lazy val
  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Structured Streaming from Kafka to Redis")
//        .set("spark.kafka.brokers","ec2-18-211-110-36.compute-1.amazonaws.com:9092,ec2-34-234-235-148.compute-1.amazonaws.com:9092")
        .set("spark.worker.memory", "6g")
        .set("spark.executor.memory", "3g")
        .set("spark.executor.cores", "2")
        .set("spark.worker.cores", "1")
    @transient lazy val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.redis.host", "ec2-3-86-129-28.compute-1.amazonaws.com")
//      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .getOrCreate()
//    @transient lazy val sqlContext: SQLContext = spark.sqlContext
    spark

  }

}


object RedisConnection extends Serializable {
  lazy val conn: RedisClient = new RedisClient("ec2-3-86-129-28.compute-1.amazonaws.com", 6379)
}

class RedisDriver extends SparkSessionBuilder {
  // This object will be used in CassandraSinkForeach to connect to Cassandra DB from an executor.
  // It extends SparkSessionBuilder so to use the same SparkSession on each node.
//  val spark = buildSparkSession
//  val connector = new RedisConfig(new RedisEndpoint(host="ec2-18-211-110-36.compute-1.amazonaws.com",port=6379))
//  val connector = new RedisEndpoint(spark.sparkContext.getConf)
  val connector = RedisConnection.conn
//  val client = new RedisClient("ec2-3-86-129-28.compute-1.amazonaws.com", 6379)
//  val clients = new RedisClientPool("ec2-3-91-113-70.compute-1.amazonaws.com", 6379)
//  val redisServerDnsAddress = "ec2-3-91-113-70.compute-1.amazonaws.com" // spark
//  val redisPortNumber = "6379" // sharon-db
//  val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber))


  //  val connector = RedisEndpoint(spark.sparkContext.getConf)
  // Define Cassandra's table which will be used as a sink
  /* For this app I used the following table:
       CREATE TABLE fx.spark_struct_stream_sink (
       fx_marker text,
       timestamp_ms timestamp,
       timestamp_dt date,
       primary key (fx_marker));
  */
  val table = "april"
  val foreachTableSink = "spark_struct_stream_sink"
}

class RedisSink extends ForeachWriter[org.apache.spark.sql.Row]
{
    /**
      * Called when starting to process one partition of new data in the executor. The `version` is
      * for data deduplication when there are failures. When recovering from a failure, some data may
      * be generated multiple times but they will always have the same version.
      *
      * If this method finds using the `partitionId` and `version` that this partition has already been
      * processed, it can return `false` to skip the further data processing. However, `close` still
      * will be called for cleaning up resources.
      *
      * @param partitionId the partition id.
      * @param version a unique id for data deduplication.
      * @return `true` if the corresponding partition and version id should be processed. `false`
      *         indicates the partition should be skipped.
      */


//  val redisDriver = new RedisDriver

//  case class Flight(id: String, fare: Double)
    def open(partitionId: Long, version: Long): Boolean = {
      println("Open Connection")
//      println(redisDriver.connector.hosts.length)
      true
    }
    /**
      * Called to process the data in the executor side. This method will be called only when `open`
      * returns `true`.
      */
    def process(record: org.apache.spark.sql.Row): Unit = {
      println(s"Process $record")

      RedisConnection.conn.lpush(record(0), record(1))


//      val flightSeq = Seq(Flight(record(0).toString,record(1).toString.toDouble))

//      val schema = StructType(Array(
//        StructField("key", StringType, nullable = false),
//          StructField("fare", DoubleType, nullable = false)))
//      val sqlContext = redisDriver.spark.sqlContext
//      import sqlContext.implicits._
//      sqlContext.createDataFrame(record, schema)

//      val df = redisDriver.spark.createDataFrame(flightSeq)

//      redisDriver.connector.
//      df.write
//        .format("org.apache.spark.sql.redis")
//        .option("table", "april-d")
//        .option("key.column", "id")
//        .save()
    }

    /**
      * Called when stopping to process one partition of new data in the executor side. This is
      * guaranteed to be called either `open` returns `true` or `false`. However,
      * `close` won't be called in the following cases:
      *  - JVM crashes without throwing a `Throwable`
      *  - `open` throws a `Throwable`.
      *
      * @param errorOrNull the error thrown during processing data or null if there was no error.
      */
    def close(errorOrNull: Throwable): Unit ={
      println(s"Close connection")
    }
}