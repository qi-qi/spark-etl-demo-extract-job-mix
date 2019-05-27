name := "spark-etl-demo-extract-job-mix"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
  "info.picocli" % "picocli" % "3.9.6",
  "org.scalatest" %% "scalatest" % "3.0.7" % Test
)