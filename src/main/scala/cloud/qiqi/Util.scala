package cloud.qiqi

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataType

import scala.util.Try

object Util {
  def addEventDate(eventDate: LocalDate)(df: DataFrame): DataFrame = {
    df.withColumn("event_date", lit(Date.valueOf(eventDate)))
  }

  // if source column type not match with schema or column not exist => null
  def parseSchema(schema: Map[String, DataType])(df: DataFrame): DataFrame = {
    schema.keys.foldLeft(df) {
      (tempDf: DataFrame, colName: String) =>
        val colType = schema(colName)
        val column = Try(tempDf(colName).cast(colType)).getOrElse(lit(null).cast(colType))
        tempDf.withColumn(colName, column)
    }
  }
}
