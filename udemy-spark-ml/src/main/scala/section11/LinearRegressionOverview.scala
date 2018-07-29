package section11

import commons.Constants
import org.apache.spark.ml.regression.LinearRegression
import udemy.spark.commons.{LibsvmLoader, MLHelper, SparkHelper}

object LinearRegressionOverview extends App {
  private val session = SparkHelper.startSessionWithDF("LinearRegression",
    Constants.resourcesRootPath, Constants.lregLibsvmSampleDataPath, LibsvmLoader)

  val lr = new LinearRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)
  val lrModel = lr.fit(session.df)

  MLHelper.LinearRegression.describe(lrModel)
}
