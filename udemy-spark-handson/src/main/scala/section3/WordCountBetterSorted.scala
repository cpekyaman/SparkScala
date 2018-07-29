package section3

import commons.Constants
import udemy.spark.commons.SparkHelper

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSorted {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    val context = SparkHelper.loadText("WordCountBetterSorted", Constants.resourcesRootPath, "data/book.txt")

    // Split using a regular expression that extracts words
    val words = context.rdd.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}

