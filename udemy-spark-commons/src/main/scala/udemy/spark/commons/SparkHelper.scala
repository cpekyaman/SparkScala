package udemy.spark.commons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait Loader {
  def load(spark: SparkSession, resourcesRoot: String, fileName: String): DataFrame
}

case object CsvLoader extends Loader {
  override def load(spark: SparkSession, resourcesRoot: String, fileName: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$resourcesRoot/$fileName")
  }
}

case object LibsvmLoader extends Loader {
  override def load(spark: SparkSession, resourcesRoot: String, fileName: String): DataFrame = {
    val svmFilePath = s"$resourcesRoot/$fileName"
    spark.read.format("libsvm").load(svmFilePath)
  }
}

class Session(val spark: SparkSession, val df: DataFrame)

object Session {
  def apply(spark: SparkSession, df: DataFrame): Session = new Session(spark, df)
}

class RddContext(val sc: SparkContext, val rdd: RDD[String])

object RddContext {
  def apply(sc: SparkContext, rdd: RDD[String]): RddContext = new RddContext(sc, rdd)
}

object SparkHelper {
  private val defaultAppName = "SparkML"

  def localSession(appName: String = defaultAppName): SparkSession = {
    session(appName, "local[*]")
  }

  def session(appName: String, master: String): SparkSession = {
    setLogLevel()

    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

  def loadText(appName: String, resourcesRoot: String, fileName: String): RddContext = {
    setLogLevel()

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local[*]", appName)

    // Load each line of my book into an RDD
    RddContext(sc, sc.textFile(s"$resourcesRoot/$fileName"))
  }

  private def setLogLevel(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  }

  def startSessionWithDF(appName: String, resourcesRoot: String, fileName: String, loader: Loader = CsvLoader): Session = {
    val spark = localSession(appName)
    val df = loader.load(spark, resourcesRoot, fileName)

    println(s"loaded file $fileName")
    df.printSchema()

    Session(spark, df)
  }
}
