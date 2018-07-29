package assignments.section07

import commons.Constants
import udemy.spark.commons.SparkHelper

object InvoiceNoMapper extends App {
  val context = SparkHelper.loadText("InvoiceNoMapper", Constants.resourcesRootPath, "data/online-retail.csv")

  // 1. split each line
  // 2. take first element of split
  val rdd = context.rdd.map(_.split("\\,")).map(l => l(0))

  println(rdd.take(20))
}
