import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Created by Sharon on 1/25/19.
  */

class SparkSessionBuilder extends Serializable {
  // Build a spark session. Class is made serializable so to get access to SparkSession in a driver and executors.
  // Note here the usage of @transient lazy val
  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setAppName("Structured Streaming from Kafka to Redis")
    @transient lazy val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.redis.host", "ec2-18-211-110-36.compute-1.amazonaws.com")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .getOrCreate()
    spark
  }

}

class RedisDriver extends SparkSessionBuilder {
  // This object will be used in CassandraSinkForeach to connect to Cassandra DB from an executor.
  // It extends SparkSessionBuilder so to use the same SparkSession on each node.
//  val spark = buildSparkSession
  val connector = new RedisConfig(new RedisEndpoint(host="ec2-18-211-110-36.compute-1.amazonaws.com",port=6379))

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

class RedisSink extends ForeachWriter[org.apache.spark.sql.DataFrame]
//class RedisSink extends ForeachWriter[org.apache.spark.sql.Row]
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

//    val redisServerDnsAddress = "ec2-54-227-20-247.compute-1.amazonaws.com" // spark
//      val redisServerDnsAddress = "ec2-18-211-110-36.compute-1.amazonaws.com" // sharon-db
//  val redisPortNumber = 6379
  val redisDriver = new RedisDriver()
    def open(partitionId: Long, version: Long): Boolean = {
      println("Open Connection")
//      val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, null))
      println(redisDriver.connector.hosts.length)
      true
    }

    /**
      * Called to process the data in the executor side. This method will be called only when `open`
      * returns `true`.
      */
//    def process(record: org.apache.spark.sql.Row): Unit = {
    def process(dataFrame: org.apache.spark.sql.DataFrame): Unit = {
      println(s"Process $dataFrame")
//      val schema = StructType(Array(
//        StructField("key", StringType, nullable = false),
//          StructField("fare", DoubleType, nullable = false)))
//      sqlContext.createDataFrame(rdd, schema)

      dataFrame.write
        .format("org.apache.spark.sql.redis")
        .option("table", "person")
        .option("key.column", "name")
        .save()
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