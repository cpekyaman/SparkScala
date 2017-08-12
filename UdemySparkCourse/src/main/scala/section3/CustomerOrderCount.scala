package section3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by raistlin on 7/30/2017.
  */
object CustomerOrderCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "CustomerOrders")

    // Load each line of my book into an RDD
    val lines = sc.textFile("./UdemySparkCourse/src/main/resources/data/customer-orders.csv")

    val data = lines.map(_.split("\\,")).map(a => (a(0).toInt, a(2).toFloat) )

    val totals = data.reduceByKey((x,y) => x + y).map( _.swap).sortByKey(false)

    val results = totals.collect()
    results.foreach(println)
  }
}
