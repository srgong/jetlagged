name := "jetlagged"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

//lazy val root = (project in file("."))
//  .aggregate(util, core)

//lazy val util = (project in file("fare-generator"))

//lazy val core = (project in file("fare-finder"))


libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion,
//  "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1",
  "com.redislabs" % "spark-redis" % "2.3.1-M1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case "META-INF/services/org.apache.spark.sql.redis.DataSourceRegister" => MergeStrategy.concat
  //  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  //  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  //  case "application.conf"                            => MergeStrategy.concat
  //  case "unwanted.txt"                                => MergeStrategy.discard
  case x => MergeStrategy.first
}
