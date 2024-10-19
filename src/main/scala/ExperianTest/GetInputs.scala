package ExperianTest

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class GetInputs(spark: SparkSession, config: Config) {
  def getAllInputs: Map[String, DataFrame] = {
    Map( //Set a Map for each input, with all the inputs in a single object, accessible through a specific key
      "df1" -> getInfo(config, "df_1"),
      "df2" -> getInfo(config, "df_2"),
      "df3" -> getInfo(config, "df_3")
    )
  }

  def getInfo(config: Config, baseCod: String): DataFrame = {
    val path = s"app.inputs.$baseCod"
    spark.read.option("header", "true").csv(config.getString(path)) //Method for loading with spark all the csv inputs
  }
}