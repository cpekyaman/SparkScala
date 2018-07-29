package section3

import commons.Constants
import udemy.spark.commons.SparkHelper

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    val context = SparkHelper.loadText("WordCount", Constants.resourcesRootPath, "data/book.txt")

    // Split into words separated by a space character
    val words = context.rdd.flatMap(x => x.split(" "))

    // Count up the occurrences of each word
    val wordCounts = words.countByValue()

    // Print the results.
    wordCounts.foreach(println)
  }

}

