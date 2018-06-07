package section11

import commons.{FeatureData, MLHelper, SparkHelper}

object LinearRegressionWalkthrough extends App {
  private val session = SparkHelper.startSessionWithDF("LinearRegressionWT", "ml/regression/Clean-USA-Housing.csv")
  private val featureColumnNames = Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population")

  MLHelper.LinearRegression.run(session, FeatureData("Price", featureColumnNames))
}
