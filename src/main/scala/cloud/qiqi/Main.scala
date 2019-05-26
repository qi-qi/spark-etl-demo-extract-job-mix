package cloud.qiqi

import java.time.LocalDate

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

object Main {
  System.setProperty("user.timezone", "UTC")
  private val sparkConf = new SparkConf()
    .setAppName("spark-etl-demo-extract-job-scala")
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .set("spark.sql.session.timeZone", "UTC")

  def main(args: Array[String]): Unit = {
    Conf(args) match {
      case Success(conf) => start(conf.startDate, conf.daysToRun, conf.srcPath, conf.dstPath)
      case Failure(_) => System.exit(-1)
    }
  }

  private def start(startDate: LocalDate, daysToRun: Int, srcPath: String, dstPath: String): Unit = {
    val spark = SparkSession.builder.config(sparkConf).getOrCreate
    try {
      (0 until daysToRun)
        .map(startDate.plusDays(_))
        .foreach(runDate => Job.run(srcPath, dstPath, runDate))
    } finally {
      spark.stop
    }
  }
}
