package ExperianTest
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.functions._

class LogicTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _
  var config: Config = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[*]").getOrCreate()
    config = ConfigFactory.parseResources("config/config.conf").resolve()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  "Logic" should "run the process and return a joined DataFrame" in {
    val logic = new Logic(config)

    val df1 = spark.createDataFrame(Seq((1, "Alice"),(2, "Bob"),(3, "Charlie"))).toDF("id", "name")
    val df2 = spark.createDataFrame(Seq((1, "2023-01-01"), (2, "2023-02-01"), (4, "2023-03-01"))).toDF("id", "date")
    val df3 = spark.createDataFrame(Seq((1, "Product A"), (2, "Product B"), (3, "Product C"))).toDF("id", "product")
    val inputs: Map[String, DataFrame] = Map("df1" -> df1, "df2" -> df2, "df3" -> df3)

    val joinedDF = logic.joinDF(inputs("df1"), inputs("df2"), "left")

    joinedDF.columns should contain allOf ("id", "name", "date")

    joinedDF.count() shouldEqual 3

    val result = joinedDF.where(col("id") === 1).select("name", "date").collect().head
    result.getString(0) shouldEqual "Alice"
    result.getString(1) shouldEqual "2023-01-01"
  }
}
