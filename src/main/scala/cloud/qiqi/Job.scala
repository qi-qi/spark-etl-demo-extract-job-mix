package cloud.qiqi

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Job {
  private lazy val spark = SparkSession.active

  import spark.implicits._

  def run(srcPath: String, dstPath: String, runDate: LocalDate): Unit = {
    val srcFileCsv = "%s/%s.csv".format(srcPath, runDate) //Ex: srcFileCsv => "./data_raw/2018-07-01.csv"

    val df = loadSourceCsv(srcFileCsv, runDate)
      .transform(Util.parseSchema(Schema.mix))
      .transform(Util.addEventDate(runDate))
      .select("event_date", Schema.mix.keys.toSeq: _*) // ensure the selected output columns and in selected ordering

    writeToParquet(df, dstPath, 1)
  }

  private def loadSourceCsv(srcFileCsv: String, runDate: LocalDate): DataFrame = {
    spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(srcFileCsv)
  }

  private def writeToParquet(df: DataFrame, dstPath: String, repartitionNum: Int): Unit = {
    df.withColumn("dt", $"event_date")
      .repartition(repartitionNum)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("dt")
      .parquet(dstPath)
  }
}
