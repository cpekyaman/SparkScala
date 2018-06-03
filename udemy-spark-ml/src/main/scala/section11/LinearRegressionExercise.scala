package section11

import commons.{FeatureData, MLHelper, Session, SparkHelper}

object LinearRegressionExercise extends App {
  private val session = SparkHelper.localSession("LinearRegressionExc")
  private val df = SparkHelper.dfFromCsv(session, "ml/regression/Clean-Ecommerce.csv")
  private val featureColumnNames = Array("Avg Session Length", "Time on App", "Time on Website", "Length of Membership")

  MLHelper.LinearRegression.run(Session(session, df), FeatureData("Yearly Amount Spent", featureColumnNames))
}
