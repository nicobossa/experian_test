package ExperianTest

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Logic(config: Config) extends LazyLogging {
  def run(): DataFrame = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate() //Cargar la sesion de Spark
    val inputs = new GetInputs(spark, config).getAllInputs // Obtener inputs del proceso

    logger.info(s"Processing data from inputs")

    val df4 = joinDF(inputs("df1"), inputs("df2"), "left")   //Primer Join
    val df5 = joinDF(df4, inputs("df3"), "inner") //Segundo Join
    df5
  }

  def joinDF(dfA: DataFrame, dfB: DataFrame, str: String): DataFrame = {
    dfA.join(dfB.withColumnRenamed("id", "idB"), col("id") === col("idB"), str).drop("idB")
  }
}
