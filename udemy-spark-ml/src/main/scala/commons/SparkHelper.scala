package commons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Session(val spark: SparkSession, val df: DataFrame)
object Session { def apply(spark: SparkSession, df: DataFrame): Session = new Session(spark, df) }

object SparkHelper {
  private val defaultAppName = "SparkML"

  def localSession(appName: String = defaultAppName): SparkSession = {
      session(appName, "local[*]")
  }

  def session(appName: String, master: String): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

  def dfFromCsv(spark: SparkSession, fileName: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"${Constants.resourcesRootPath}/$fileName")
  }

  def startSessionWithDF(appName: String, fileName: String): Session = {
    val spark = localSession(appName)
    val df = dfFromCsv(spark, fileName)

    println(s"loaded file $fileName")
    df.printSchema()

    new Session(spark, df)
  }
}
