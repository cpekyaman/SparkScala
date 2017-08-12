package section4

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * Created by raistlin on 7/31/2017.
  */
object MovieSimilarities {

  case class MovieRating(movie: Int, rating: Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    userRatings._2._1.movie < userRatings._2._2.movie
  }

  type PairKey = (Int, Int)
  type RatingPair = (Double, Double)
  def makePairs(userRatings: UserRatingPair): (PairKey, RatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    ((movieRating1.movie, movieRating2.movie), (movieRating1.rating, movieRating2.rating))
  }

  type RatingPairs = Iterable[RatingPair]
  case class SimilarityScore(value: Double, pairs: Int)

  def computeCosineSimilarity(ratingPairs: RatingPairs): SimilarityScore = {
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
    }

    val numerator: Double = sum_xy
    val denominator = math.sqrt(sum_xx) * math.sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    SimilarityScore(score, ratingPairs.size)
  }

  def findSimilar(movie: Int, similarities: RDD[((Int, Int), SimilarityScore)]): Unit = {
    val scoreThreshold = 0.97
    val coOccurenceThreshold = 50.0

    val filtered = similarities.filter { s =>
      val pairKey = s._1
      val score = s._2

      (pairKey._1 == movie || pairKey._2 == movie) && score.value > scoreThreshold && score.pairs > coOccurenceThreshold
    }

    val results = filtered.map(x => (x._2.value, if(x._1._1 == movie) x._1._2 else x._1._1 )).sortByKey(false).take(10)
    //TODO: print the results cleanly
    results.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CollaborativeFiltering")

    val filePath = "./UdemySparkCourse/src/main/resources/data/ml/u.data"
    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings = sc
      .textFile(filePath)
      .map(_.split("\t"))
      .map(row => (row(0).toInt, MovieRating(row(1).toInt, row(2).toDouble)))

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))
    // Filter out duplicate pairs
    val uniqueJoinedRatings = ratings.join(ratings).filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()

    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val movieSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
    findSimilar(50, movieSimilarities)
  }
}
