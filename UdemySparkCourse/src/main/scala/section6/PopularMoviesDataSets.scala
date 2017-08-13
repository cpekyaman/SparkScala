package section6

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/** Find the movies with the most ratings. */
object PopularMoviesDataSets {

  // Case class so we can get a column name for our movie ID
  final case class Movie(movieID: Int)
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///E:/DATA/Spark/Temp")
      .getOrCreate()
    
    // Read in each rating line and extract the movie ID; construct an RDD of Movie objects.
    val lines = spark.sparkContext
        .textFile("./UdemySparkCourse/src/main/resources/data//ml/u.data")
        .map(x => Movie(x.split("\t")(1).toInt))
    
    // Convert to a DataSet
    import spark.implicits._
    val moviesDS = lines.toDS()
    
    // Some SQL-style magic to sort all movies by popularity in one line!
    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count")).cache()
    
    // Show the results at this point:
    /*
    |movieID|count|
    +-------+-----+
    |     50|  584|
    |    258|  509|
    |    100|  508|   
    */
    
    topMovieIDs.show()
    
    // Grab the top 10
    val top10 = topMovieIDs.take(10)
    
    // Load up the movie ID -> name map
    val names = loadMovieNames()
    
    // Print the results
    println
    for (result <- top10) {
      // result is just a Row at this point; we need to cast it back.
      // Each row has movieID, count as above.
      println (names(result(0).asInstanceOf[Int]) + ": " + result(1))
    }

    // Stop the session
    spark.stop()
  }
  
}

