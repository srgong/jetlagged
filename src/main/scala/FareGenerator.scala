/**
  * Created by Sharon on 1/20/19.
  */

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object FareGenerator {

  val sparkConf = new SparkConf()
    .setAppName("Fare Generator")
//    .setMaster("local[*]")
//    .set("spark.file.in", "src/main/resources/csv/201803.csv")
//    .set("spark.file.flights", "src/main/resources/json/")

  /**
    * Saves to hdfs as text file.
    * @param df
    * @param path
    */
  def save(df: DataFrame, path: String) = {
    df.rdd.saveAsTextFile(path)
//    df.repartition(255).write.mode(SaveMode.Overwrite).format("json").save(path)
  }

  /**
    * Replicates same flight information (to/from/when)
    * @param df
    * @param n
    * @return
    */
  def replicate(df: DataFrame, n: Int) = {
    df.cache()
    df.withColumn("dummy", explode(array((1 until n).map(lit): _*)))
      .drop("dummy")
  }

  /**
    * Appends different fare for each flight.
    *
    * Fare based on a right skewed distribution
    * Right Skew and number range is guaranteed by this idea:
    *   Math.max(min_fare, Math.min(max_fare, (int) mean + Random.nextGaussian() * stddev)))
    *
    * If the generator lands on a price < $35, we redistribute that to a fare between $35-$75.
    * Min air fare is $35 with 8K flights < $100
    * Max air fare is $543 with ~28 flights > $500
    * Sample dataset has 1.5 million flights
    *
    * @param df
    * @return DataFrame
    */
  def generateFare(df: DataFrame, min_fare: Int, max_fare: Int, mean: Int, stddev: Int): DataFrame = {
    val withFare = df.withColumn("RIGHTSKEWDISTR", round(randn(seed=10)*stddev+mean,2))
    withFare.withColumn("fare",
      when(col("RIGHTSKEWDISTR") < min_fare, scala.util.Random.nextInt(100) + min_fare)
        .when(col("RIGHTSKEWDISTR") > max_fare, max_fare)
        .otherwise(col("RIGHTSKEWDISTR")))
      .drop("RIGHTSKEWDISTR")
  }

  /**
    * Creates normalized time columns - timestamp, epoch time
    * @param df
    * @return
    */
  def getTime(df: DataFrame): DataFrame = {
    df.withColumn("date", date_format(add_months(to_timestamp(col("FL_DATE"), "yyyy-MM-dd"),13), "MM-dd-yyyy"))
      .withColumn("dep_datetime",to_timestamp(concat(col("date"),lit(" "), col("CRS_DEP_TIME")),"MM-dd-yyyy HHmm"))
      .withColumn("arr_datetime",to_timestamp(concat(col("date"),lit(" "), col("CRS_ARR_TIME")),"MM-dd-yyyy HHmm"))
      .withColumn("time", concat(substring(col("CRS_DEP_TIME"),0,2), lit(":"), substring(col("CRS_DEP_TIME"),3,2), lit("-"),
        substring(col("CRS_ARR_TIME"),0,2), lit(":"), substring(col("CRS_ARR_TIME"),3,2)))
      .withColumnRenamed("ORIGIN","from")
      .withColumnRenamed("DEST","to")
      .select("date","time","from","to","dep_datetime","arr_datetime")
  }

  def findLayovers(df: DataFrame, maxLayoverHours: Int): DataFrame = {
    val flightA = df
      .withColumnRenamed("dep_datetime","first_dep_time")
      .withColumnRenamed("arr_datetime","first_arr_time")
      .withColumnRenamed("from","first_from")
      .withColumnRenamed("to","first_to")
      .withColumnRenamed("time","first_time")

    val flightB = df
      .withColumnRenamed("arr_datetime","last_arr_time")
      .withColumnRenamed("dep_datetime","last_dep_time")
      .withColumnRenamed("to","last_to")
      .withColumnRenamed("from","last_from")
      .withColumnRenamed("time","last_time")

    val layovers = flightA.join(flightB, flightA("first_to") === flightB("last_from")
        && flightA("date") === flightB("date")).drop(flightB("date"))
      .withColumn("layoverHours", (((unix_timestamp(col("last_dep_time"))) -
        unix_timestamp(col("first_arr_time"))) / 3600000))
      .filter(col("layoverHours") <= maxLayoverHours && col("layoverHours") > 0)
      .withColumnRenamed("first_from","from")
      .withColumnRenamed("first_to","to")
      .withColumnRenamed("last_to","last")
      .withColumnRenamed("first_dep_time","dep_datetime")
      .withColumnRenamed("first_arr_time","arr_datetime")
      .withColumnRenamed("first_time","time")
      .select("date","time","from","to","last","last_time")

    layovers
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext: SQLContext = spark.sqlContext

    val df = sqlContext.read.option("header", "true").csv(sparkConf.get("spark.file.in"))
    val withTime = getTime(df).select("date","time","from","to","dep_datetime","arr_datetime").distinct
//    println(withTime.count) // 201803 611,987

    val withLayover = findLayovers(withTime, maxLayoverHours = 3)
    val withLayoverReplication = replicate(withLayover,5)
    val withLayoverFare = generateFare(df = withLayoverReplication, min_fare = 100, max_fare = 350, mean = 253, stddev = 60)

    val withReplication = replicate(withTime.drop("dep_datetime","arr_datetime"),10)
    val withNormalisedCols = withReplication.withColumn("last",lit("None")).withColumn("last_time",lit("None"))
    val withDirectFare = generateFare(df = withNormalisedCols,  min_fare = 350, max_fare = 700, mean = 353, stddev = 60)

    val flights = withLayoverFare.union(withDirectFare)
    flights.cache
    save(flights, sparkConf.get("spark.file.flights"))
  }

}
