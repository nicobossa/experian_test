package ExperianTest

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class GetInputsTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "GetInputs" should "load all input DataFrames correctly" in {
    val config = ConfigLoader.loadConfig() // Asegúrate de que este método esté disponible
    val getInputs = new GetInputs(spark, config)
    val inputs: Map[String, DataFrame] = getInputs.getAllInputs

    // Verificar que los DataFrames se han cargado correctamente
    inputs should contain key "df1"
    inputs should contain key "df2"
    inputs should contain key "df3"

    inputs("df1").count() should be > 0
    inputs("df2").count() should be > 0
    inputs("df3").count() should be > 0
  }
}
