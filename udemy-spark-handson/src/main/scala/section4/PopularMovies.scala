package section4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by raistlin on 7/30/2017.
  */
object PopularMovies {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "PopularMovies")

    val movieNames = sc.broadcast(loadMovieNames())

    // Load each line of my book into an RDD
    val lines = sc.textFile("./UdemySparkCourse/src/main/resources/data/ml/u.data")

    val data = lines
        .map( x => (x.split("\\t")(1).toInt, 1))
        .reduceByKey((x,y) => x + y)
        .map(_.swap)
        .sortByKey(false)
        .map(x => (movieNames.value(x._2), x._1))

    val results = data.collect()
    results.foreach(println)
  }
}
