package section4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by raistlin on 7/30/2017.
  */
object MostPopularSuperHero {

  def countOccurences(line: String): (Int, Int) = {
    val arr = line.split("\\s+")
    (arr(0).toInt, arr.length - 1)
  }

  def parseNames(line: String): Option[(Int, String)] = {
    line.split('\"').toList match {
      case _ :: Nil => None
      case a :: b => Some(a.trim().toInt, b.head)
      case _ => None
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "MostPopularSuperHero")

    val namesRdd = sc.textFile("./UdemySparkCourse/src/main/resources/data/marvel/Marvel-names.txt").flatMap(parseNames)

    val graph = sc.textFile("./UdemySparkCourse/src/main/resources/data/marvel/Marvel-graph.txt")
      .map(countOccurences)
      .reduceByKey((x, y) => x + y)
      .map(x => (namesRdd.lookup(x._1).head, x._2))
      .map(_.swap)
      .sortByKey(false)
      .top(10)

    graph.foreach(println)
  }
}
