name := "jetlagged"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"

//lazy val root = (project in file("."))
//  .aggregate(util, core)

//lazy val util = (project in file("fare-generator"))

//lazy val core = (project in file("fare-finder"))

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  //  "org.apache.hadoop"% "hadoop-client" % "2.7.0",
  //  "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
  //  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "org.apache.spark" %% "spark-avro" % "2.4.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
//  case "application.conf"                            => MergeStrategy.concat
//  case "unwanted.txt"                                => MergeStrategy.discard
  case x => MergeStrategy.first
}
