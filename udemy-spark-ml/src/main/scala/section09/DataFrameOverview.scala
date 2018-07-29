package section09

import commons.Constants
import udemy.spark.commons.SparkHelper

object DataFrameOverview extends App {
  val session = SparkHelper.startSessionWithDF("DFOverview", Constants.resourcesRootPath,"data/CitiGroup2006_2008")

  println(session.df.columns.toList)
  session.df.head(5).foreach(println)

  session.df.describe().show()

  val df2 = session.df.withColumn("HighPlusLow", session.df("High") + session.df("Low"))
  df2.select("HighPlusLow").head(5).foreach(println)
}
