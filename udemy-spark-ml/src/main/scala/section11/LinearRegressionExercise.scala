package section11

import commons.Constants
import udemy.spark.commons.{FeatureData, MLHelper, SparkHelper}

object LinearRegressionExercise extends App {
  private val session = SparkHelper.startSessionWithDF("LinearRegressionExc", Constants.resourcesRootPath,"ml/regression/Clean-Ecommerce.csv")
  private val featureColumnNames = Array("Avg Session Length", "Time on App", "Time on Website", "Length of Membership")

  MLHelper.LinearRegression.run(session, FeatureData("Yearly Amount Spent", featureColumnNames))
}
