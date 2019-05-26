name := "spark-etl-demo-extract-job-scala"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "commons-cli" % "commons-cli" % "1.4",
  "org.scalatest" %% "scalatest" % "3.0.7" % Test
)