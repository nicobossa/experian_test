package ExperianTest

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class GetInputsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "GetInputs" should "load all input DataFrames correctly" in {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val config = ConfigFactory.parseResources("config/config.conf").resolve()
    val getInputs = new GetInputs(spark, config)
    val inputs: Map[String, DataFrame] = getInputs.getAllInputs

    inputs should contain key "df1"
    inputs should contain key "df2"
    inputs should contain key "df3"

    inputs("df1").count() should be > 0L
    inputs("df2").count() should be > 0L
    inputs("df3").count() should be > 0L
  }
}
