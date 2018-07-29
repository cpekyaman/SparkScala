package section09

import commons.Constants
import udemy.spark.commons.SparkHelper

object MissingData extends App {
  val session = SparkHelper.startSessionWithDF("DFMissingData", Constants.resourcesRootPath,"data/ContainsNull.csv")

  println("dropping nulls")
  session.df.na.drop().show()

  println("dropping rows with less than 2 no null columns")
  session.df.na.drop(2).show()

  println("filling in missing values")
  session.df.na.fill(Map("Name" -> "NoName", "Sales" -> 0.0)).show()
}
