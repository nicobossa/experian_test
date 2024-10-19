package ExperianTest
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MainLauncherTest extends AnyFlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  val config: Config = ConfigFactory.parseResources("config/config.conf").resolve()


  "Logic" should "not return empty DataFrame after run" in {
    val logic = new Logic(config)
    val resultDF: DataFrame = logic.run()
    resultDF.isEmpty should be(false)
  }
}
