import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.spark.sql.ForeachWriter

/**
  * Created by Sharon on 1/25/19.
  */
class RedisSink(url: String, user:String, passwd: String)
//  extends ForeachWriter[org.apache.spark.sql.Row]
 {
//  /**
//    * Called when starting to process one partition of new data in the executor. The `version` is
//    * for data deduplication when there are failures. When recovering from a failure, some data may
//    * be generated multiple times but they will always have the same version.
//    *
//    * If this method finds using the `partitionId` and `version` that this partition has already been
//    * processed, it can return `false` to skip the further data processing. However, `close` still
//    * will be called for cleaning up resources.
//    *
//    * @param partitionId the partition id.
//    * @param version a unique id for data deduplication.
//    * @return `true` if the corresponding partition and version id should be processed. `false`
//    *         indicates the partition should be skipped.
//    */
//
//  val redisServerDnsAddress = "ec2-18-211-110-36.compute-1.amazonaws.com"
//  val redisPortNumber = 6379
//  val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, null))
//
//  println(redisConfig.hosts.length)
//
//  df.write
//    .format("org.apache.spark.sql.redis")
//    .option("table", "person")
//    .option("key.column", "name")
//    .save()
//  def open(partitionId: Long, version: Long): Boolean = {
//    println("Open Connection")
//    true
//  }
//
//  /**
//    * Called to process the data in the executor side. This method will be called only when `open`
//    * returns `true`.
//    */
//  def process(record: org.apache.spark.sql.Row): Unit = {
//    println(s"Process $record")
//
//    )
//  }
//
//  /**
//    * Called when stopping to process one partition of new data in the executor side. This is
//    * guaranteed to be called either `open` returns `true` or `false`. However,
//    * `close` won't be called in the following cases:
//    *  - JVM crashes without throwing a `Throwable`
//    *  - `open` throws a `Throwable`.
//    *
//    * @param errorOrNull the error thrown during processing data or null if there was no error.
//    */
//  def close(errorOrNull: Throwable): Unit ={
//
//  }
}
