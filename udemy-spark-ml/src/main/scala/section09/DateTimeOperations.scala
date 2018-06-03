package section09

import commons.SparkHelper
import org.apache.spark.sql.DataFrame

object DateTimeOperations extends App {
  val session = SparkHelper.startSessionWithDF("DFDateTime", "data/CitiGroup2006_2008")

  import org.apache.spark.sql.functions._
  import session.spark.implicits._

  private val df: DataFrame = session.df

  println("list months in data")
  df.select(month(df("Date"))).show()

  println("show mean by Year")
  private val meanByYear: DataFrame = df.withColumn("Year", year(df("Date"))).groupBy("Year").mean()
  meanByYear.show()
  meanByYear.select($"Year", $"avg(Close)").show()
}
