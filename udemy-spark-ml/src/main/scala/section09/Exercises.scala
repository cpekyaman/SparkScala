package section09

import commons.SparkHelper

object Exercises extends App {
  val session = SparkHelper.startSessionWithDF("DFExercises", "data/Netflix_2011_2016.csv")

  import session.spark.implicits._
  import org.apache.spark.sql.functions._

  println("show columns")
  println(session.df.columns.toList)

  println("describe dataframe")
  session.df.describe()

  println("first 5 rows")
  session.df.head(5)

  println("new dataframe with HV Ratio")
  session.df.withColumn("HV Ratio", session.df("High") / session.df("Volume")).show()

  println("the day stock is highest")
  session.df.orderBy($"High".desc).show(1)

  session.df.select(mean("Close")).show()

  session.df.select(min("Volume")).show()
  session.df.select(max("Volume")).show()

  println("days stock close value is lower than 600")
  println(session.df.filter($"Close" < 600).count())

  println("correlation between High and Volume")
  session.df.select(corr("High", "Volume")).show()

  println("max High per Year")
  session.df
    .withColumn("Year", year(session.df("Date")))
    .select($"Year", $"High").groupBy("Year")
    .max()
    .show
}
