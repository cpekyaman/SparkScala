package section09

import commons.SparkHelper

object MissingData extends App {
  val session = SparkHelper.startSessionWithDF("DFMissingData", "data/ContainsNull.csv")

  println("dropping nulls")
  session.df.na.drop().show()

  println("dropping rows with less than 2 no null columns")
  session.df.na.drop(2).show()

  println("filling in missing values")
  session.df.na.fill(Map("Name" -> "NoName", "Sales" -> 0.0)).show()
}
