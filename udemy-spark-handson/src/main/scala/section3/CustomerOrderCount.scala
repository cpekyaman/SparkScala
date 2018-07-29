package section3

import commons.Constants
import udemy.spark.commons.SparkHelper

/**
  * Created by raistlin on 7/30/2017.
  */
object CustomerOrderCount {

  def main(args: Array[String]): Unit = {
    val context = SparkHelper.loadText("CustomerOrders", Constants.resourcesRootPath,"data/customer-orders.csv")
    val rdd = context.rdd

    val data = rdd.map(_.split("\\,")).map(a => (a(0).toInt, a(2).toFloat))

    val totals = data.reduceByKey((x, y) => x + y).map(_.swap).sortByKey(false)

    val results = totals.collect()
    results.foreach(println)
  }
}
