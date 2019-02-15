package connector

import model.Flight

/**
  * Created by Sharon on 1/25/19.
  */

class RedisSink extends ForeachWriter[Flight]
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

    def open(partitionId: Long, version: Long): Boolean = {
      println("Open Connection")
      true
    }
    /**
      * Called to process the data in the executor side. This method will be called only when `open`
      * returns `true`.
      */
    def process(flight: Flight): Unit = {
      println(s"Process $flight")
      val redisClient: RedisClient =  new RedisClient("ec2-3-86-129-28.compute-1.amazonaws.com", 6379)
      val delim = "@"
      val key = "date="+flight.date + delim + "from="+flight.from + delim + "to="+flight.to
      val k = "last_leg="+flight.last_to
      val v = "fare="+flight.fare+delim+"processed_ms="+flight.updated_ms
      val field = Map(k -> v)
      redisClient.hmset(key, field)
      // set TTL to 24 hrs
      redisClient.expire(key, 86400)
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
    def close(errorOrNull: Throwable): Unit = {
      println(s"Close connection")
    }
}