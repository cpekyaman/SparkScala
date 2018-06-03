package section11

import commons.{FeatureData, MLHelper, Session, SparkHelper}

object LinearRegressionWalkthrough extends App {
  private val session = SparkHelper.localSession("LinearRegressionWT")
  private val df = SparkHelper.dfFromCsv(session, "ml/regression/Clean-USA-Housing.csv")
  private val featureColumnNames = Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population")

  MLHelper.LinearRegression.run(Session(session, df), FeatureData("Price", featureColumnNames))
}
