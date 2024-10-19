package ExperianTest

import com.typesafe.scalalogging.LazyLogging

object MainLauncher extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()  // Registrar el tiempo de inicio
    val config = ConfigLoader.loadConfig()
    val df5 = new Logic(config).run()
    df5.write.mode("overwrite").parquet(config.getString("app.output.outputPath")) // Escribir DF
    val endTime = System.currentTimeMillis()    // Registrar el tiempo de finalización
    val durationSeg =(endTime - startTime) / 1000 // Convertir a segundos
    val duration = endTime - startTime // Convertir a segundos
    println(s"Tiempo de ejecución: $durationSeg segundos")
    println(s"Tiempo de ejecución: $duration complete")
    logger.info("FINAL")
  }
}
