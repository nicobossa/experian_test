package ExperianTest

import com.typesafe.scalalogging.LazyLogging

object MainLauncher extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()  // Start measuring the execution time
    val config = ConfigLoader.loadConfig() //loading the config variable
    val df5 = new Logic(config).run() //running all the logic with Logic Object
    df5.write.mode("overwrite").partitionBy("gender").parquet(config.getString("app.output.outputPath")) // Write final DF
    val endTime = System.currentTimeMillis()    // Register ending time
    val durationSeg =(endTime - startTime) / 1000 // Cast to seconds
    val duration = endTime - startTime // calculating
    println(s"Tiempo de ejecución: $durationSeg segundos")
    println(s"Tiempo de ejecución: $duration complete")
    logger.info("FINAL")
  }
}
