package ExperianTest

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class GetInputs(spark: SparkSession, config: Config) {
  def getAllInputs: Map[String, DataFrame] = {
    Map(
      "df1" -> getInfo(config, "base_1"),
      "df2" -> getInfo(config, "base_2"),
      "df3" -> getInfo(config, "base_3")
    )
  }

  def getInfo(config: Config, baseCod: String): DataFrame = {
    val path = s"app.inputs.$baseCod"
    spark.read.option("header", "true").csv(config.getString(path))
  }
}