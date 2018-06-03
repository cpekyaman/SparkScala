package section09

import commons.SparkHelper

object DataFrameOperations extends App {
  val session = SparkHelper.startSessionWithDF("DFOperations", "data/CitiGroup2006_2008")

  import org.apache.spark.sql.functions._
  import session.spark.implicits._

  val limit = 480
  println(s"Close GT $limit")
  session.df.filter($"Close" > limit).show()

  println(s"Close LT $limit And High LT $limit")
  private val lowItems = session.df.filter($"Close" < limit && $"High" < limit)
  val lowItemsList = lowItems.collectAsList()
  val lowItemCount = lowItems.count()
  println(lowItems)

  println("Pearson Correlation")
  session.df.select(corr("High", "Low")).show()
}
