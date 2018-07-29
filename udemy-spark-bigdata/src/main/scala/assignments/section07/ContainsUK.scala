package assignments.section07

import commons.Constants
import udemy.spark.commons.SparkHelper

object ContainsUK extends App {
  val context = SparkHelper.loadText("ContainsUK", Constants.resourcesRootPath, "data/online-retail.csv")

  val rdd = context.rdd.map(_.contains("United Kingdom"))

  println(rdd.take(50))
}
