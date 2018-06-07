package commons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait Loader {
  def load(spark: SparkSession, fileName: String): DataFrame
}

case object CsvLoader extends Loader {
  override def load(spark: SparkSession, fileName: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"${Constants.resourcesRootPath}/$fileName")
  }
}

case object LibsvmLoader extends Loader {
  override def load(spark: SparkSession, fileName: String): DataFrame = {
    val svmFilePath = s"${Constants.resourcesRootPath}/$fileName"
    spark.read.format("libsvm").load(svmFilePath)
  }
}

class Session(val spark: SparkSession, val df: DataFrame)

object Session {
  def apply(spark: SparkSession, df: DataFrame): Session = new Session(spark, df)
}

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

  def startSessionWithDF(appName: String, fileName: String, loader: Loader = CsvLoader): Session = {
    val spark = localSession(appName)
    val df = loader.load(spark, fileName)

    println(s"loaded file $fileName")
    df.printSchema()

    Session(spark, df)
  }
}
