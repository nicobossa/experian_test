package ExperianTest

import com.typesafe.config.ConfigFactory

object ConfigLoader {
  def loadConfig() = {
    ConfigFactory.parseResources("config/config.conf").resolve() //Load the config file
  }
}
