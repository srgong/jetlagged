name := "jetlagged"

version := "1.0"

scalaVersion := "2.11.8"

//val sparkVersion = "2.2.0"
val sparkVersion = "2.3.1"
//val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion,
//  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
//  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.1",
"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1",
//  "redis.clients" % "jedis" % "3.0.0",
  "com.redislabs" % "spark-redis" % "2.3.1-M2"
//  "com.redislabs" % "spark-redis" % "2.3.0"
)

resolvers += "rediscala" at "https://raw.github.com/etaty/rediscala-mvn/master/releases/"
//
assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
//  case "org.apache.spark.sql.redis.DefaultSource" => MergeStrategy.concat
////  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


//lazy val root = (project in file("."))
//  .aggregate(util, core)

//lazy val util = (project in file("fare-generator"))

//lazy val core = (project in file("fare-finder"))