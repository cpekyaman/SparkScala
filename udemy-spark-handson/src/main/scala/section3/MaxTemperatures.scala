package section3

import commons.Constants
import udemy.spark.commons.SparkHelper

import scala.math.max

/** Find the maximum temperature by weather station for a year */
object MaxTemperatures {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    val context = SparkHelper.loadText("MaxTemperatures", Constants.resourcesRootPath, "data/1800.csv")

    val parsedLines = context.rdd.map(parseLine)
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    val maxTempsByStation = stationTemps.reduceByKey((x, y) => max(x, y))
    val results = maxTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station max temperature: $formattedTemp")
    }

  }
}