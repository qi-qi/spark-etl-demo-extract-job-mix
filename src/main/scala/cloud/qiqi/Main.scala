package cloud.qiqi

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import picocli.CommandLine

object Main {
  System.setProperty("user.timezone", "UTC")
  private val sparkConf = new SparkConf()
    .setAppName("spark-etl-demo-extract-job-mix")
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .set("spark.sql.session.timeZone", "UTC")

  def main(args: Array[String]): Unit = {
    val conf = CommandLine.populateCommand(Conf, args: _*)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate

    try {
      (0 until conf.daysToRun)
        .map(conf.startDate.plusDays(_))
        .foreach(runDate => Job.run(conf.srcPath, conf.dstPath, runDate))
    } finally {
      spark.stop
    }
  }
}
