package ExperianTest

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigLoaderTest extends AnyFlatSpec with Matchers {

  "ConfigLoader" should "load the configuration from config.conf" in {
    val config: Config = ConfigLoader.loadConfig()

    config.hasPath("app") should be(true)
    config.hasPath("app.inputs") should be(true)
    config.hasPath("app.output") should be(true)

    config.getString("app.inputs.base_1") should equal("src/main/resources/data/base_1.csv")
    config.getString("app.inputs.base_2") should equal("src/main/resources/data/base_2.csv")
    config.getString("app.output.outputPath") should equal("src/main/resources/data/output")
  }
}
