package cloud.qiqi

import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap

object Schema {
  // ListMap: preserve insertion order for columns
  val mix: ListMap[String, DataType] = ListMap(
    "temperature" -> StringType,
    "at_risk" -> IntegerType,
    "skipped_beat" -> FloatType,
    "price" -> DoubleType
  )
}
