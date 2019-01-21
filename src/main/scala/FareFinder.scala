import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by Sharon on 1/20/19.
  */
object FareFinder {
  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Fare Finder")
  val spark: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

  def save(df: DataFrame, path: String) = {
    df.write.format("parquet").save(path)
  }

  def main(args: Array[String]): Unit = {
    val df = sqlContext.read.parquet("src/main/resources/output")
    println(df.count)
  }
}
