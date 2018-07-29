package section09

import commons.Constants
import udemy.spark.commons.SparkHelper

object DataFrameAggregates extends App {
  val session = SparkHelper.startSessionWithDF("DFAggregates", Constants.resourcesRootPath,"data/Sales.csv")
  session.df.show()

  println("Group by company")
  println("mean and count")
  private val groupByCompany = session.df.groupBy("Company")
  groupByCompany.mean().show()
  groupByCompany.count().show()

  val column = "Sales"
  println(s"aggregates on $column")

  import org.apache.spark.sql.functions._

  session.df.select(countDistinct(column)).show()
  session.df.select(sumDistinct(column)).show()
  session.df.select(variance(column)).show()
  session.df.select(collect_set(column)).show()
}
