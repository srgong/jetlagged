name := "jetlagged"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
//  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.1",
"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1",
  "com.redislabs" % "spark-redis" % "2.3.1-M2"
)

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case "META-INF/services/org.apache.spark.sql.redis.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


//lazy val root = (project in file("."))
//  .aggregate(util, core)

//lazy val util = (project in file("fare-generator"))

//lazy val core = (project in file("fare-finder"))