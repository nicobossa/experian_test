package ExperianTest

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Logic(config: Config) extends LazyLogging {
  def run(): DataFrame = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate() //loading spark session
    val inputs = new GetInputs(spark, config).getAllInputs // Get all inputs for the process
    logger.info(s"Processing data from inputs")  //Printing status
    val df4 = joinDF(inputs("df1"), broadcast(inputs("df2")), "left")   //First Join as needed
    val df5 = joinDF(df4, broadcast(inputs("df3")), "inner") //Second Join as s required
    df5
  }

  def joinDF(dfA: DataFrame, dfB: DataFrame, str: String): DataFrame = {
    dfA.join(dfB.withColumnRenamed("id", "idB"), col("id") === col("idB"), str).drop("idB") //Specific method for join
  }
}
