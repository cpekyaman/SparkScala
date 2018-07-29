package section3

import commons.Constants
import udemy.spark.commons.SparkHelper

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    val context = SparkHelper.loadText("WordCountBetter", Constants.resourcesRootPath, "data/book.txt")

    // Split using a regular expression that extracts words
    val words = context.rdd.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.countByValue()

    // Print the results
    wordCounts.foreach(println)
  }

}

