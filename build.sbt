name := "Spark-Streaming"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-sql" % "2.4.2",
  "org.apache.spark" %% "spark-streaming" % "2.4.2",
  "org.apache.spark" %% "spark-mllib" % "2.4.2"
)