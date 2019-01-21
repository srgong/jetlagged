/**
  * Created by Sharon on 1/20/19.
  */

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object FareGenerator {
  case class Flight(origin: String, destination: String, fare: Double)

  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Fare Generator")
  val spark: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

  def read(path: String) = {
    sqlContext.read.option("header","true").csv(path)
  }

  /**
    * Converts csv to parquet.
    *
    * @param df
    * @param path
    */
  def save(df: DataFrame, path: String) = {
    df.write.format("parquet").save(path)
  }

  /**
    * For every flight, assign a value based on a right skewed distribution
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
  def generateFare(df: DataFrame): DataFrame = {
    val MIN_FARE=35
    val MAX_FARE=543
    val MEAN=253
    val STDDEV=60

    val withFare = df.withColumn("RIGHTSKEWDISTR", randn(seed=10)*STDDEV+MEAN)
    withFare.withColumn("FARE",
      when(col("RIGHTSKEWDISTR") < MIN_FARE, scala.util.Random.nextInt((75 - 35) + 1) + 35)
        .when(col("RIGHTSKEWDISTR") > MAX_FARE, MAX_FARE)
        .otherwise(col("RIGHTSKEWDISTR")))
  }

  def main(args: Array[String]): Unit = {
    val df = sqlContext.read.option("header", "true").csv("src/main/resources/input")
    println(generateFare(df).count)
    save(generateFare(df), "src/main/resources/output/")
  }

}
