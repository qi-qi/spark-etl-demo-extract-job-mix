package cloud.qiqi

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, when}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class UtilTest extends FunSuite with Matchers with BeforeAndAfterAll {
  lazy val spark: SparkSession = SparkSession.active

  import spark.implicits._

  override def beforeAll: Unit = {
    SparkSession.builder
      .master("local[*]")
      .appName("test")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate
  }

  override def afterAll: Unit = {
    SparkSession.active.stop()
  }

  test("addEventDate") {
    val df = Seq(1, 2, 3).toDF("idx")
    val eventDate = "2018-03-21"
    val result = df.transform(Util.addEventDate(LocalDate.parse(eventDate)))
      .select($"idx", $"event_date")
      .map(r => (r.getInt(0), r.getDate(1)))
      .collect()

    result should contain theSameElementsAs Seq(
      (1, Date.valueOf(eventDate)),
      (2, Date.valueOf(eventDate)),
      (3, Date.valueOf(eventDate))
    )
  }

  test("parseSchema: all columns exist | correct data types") {
    val data = Seq(
      ("HIGH", 1, 0.0, 21841.54764467215),
      ("MEDIUM", 1, 2.0, 21510.59978606761),
      ("LOW", 0, 2.0, 17238.724995889344)
    )
    val df = data.toDF("temperature", "at_risk", "skipped_beat", "price")
    val result = df.transform(Util.parseSchema(Schema.mix))
      .map(r => (r.getString(0), r.getInt(1), r.getFloat(2), r.getDouble(3)))
      .collect()

    result should contain theSameElementsAs data
  }

  test("parseSchema: all columns exist | wrong data types") {
    val data = Seq(
      ("HIGH", 1, 0.0, "price_wrong_string_type"),
      ("MEDIUM", 1, 2.0, "price_wrong_string_type"),
      ("LOW", 0, 2.0, "price_wrong_string_type")
    )
    val df = data.toDF("temperature", "at_risk", "skipped_beat", "price")
    val result = df.transform(Util.parseSchema(Schema.mix))
      .filter($"price".isNull)
      .count()

    result should equal(3)
  }

  test("parseSchema: missing columns exist | different orders | correct data types") {
    val data = Seq(
      ("HIGH", 0.0, 1),
      ("MEDIUM", 2.0, 1),
      ("LOW", 2.0, 0)
    )
    val df = data.toDF("temperature", "skipped_beat", "at_risk")
    val result = df.transform(Util.parseSchema(Schema.mix))
      .select($"temperature", $"at_risk", $"skipped_beat", when($"price".isNull, lit(true)).otherwise(lit(false)))
      .map(r => (r.getString(0), r.getInt(1), r.getFloat(2), r.getBoolean(3)))
      .collect()

    result should contain theSameElementsAs Seq(
      ("HIGH", 1, 0.0, true),
      ("MEDIUM", 1, 2.0, true),
      ("LOW", 0, 2.0, true)
    )
  }

  test("parseSchema: all columns exist | null values") {
    val data = Seq(
      ("HIGH", 1, 0.0, None),
      ("MEDIUM", 1, 2.0, Some(21510.59978606761)),
      ("LOW", 0, 2.0, Some(21841.54764467215))
    )
    val df = data.toDF("temperature", "at_risk", "skipped_beat", "price")
    val result = df.transform(Util.parseSchema(Schema.mix))
      .filter($"price".isNull)
      .select($"temperature", $"at_risk", $"skipped_beat")
      .map(r => (r.getString(0), r.getInt(1), r.getFloat(2)))
      .collect()

    result should contain theSameElementsAs Seq(("HIGH", 1, 0.0))
  }
}