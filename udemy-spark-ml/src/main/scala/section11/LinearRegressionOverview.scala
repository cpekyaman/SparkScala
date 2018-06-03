package section11

import commons.{Constants, MLHelper, SparkHelper}
import org.apache.spark.ml.regression.LinearRegression

object LinearRegressionOverview extends App {
  private val session = SparkHelper.localSession("LinearRegression")

  private val svmFilePath = s"${Constants.resourcesRootPath}/ml/regression/sample_linear_regression_data.txt"
  private val svmData = session.read.format("libsvm").load(svmFilePath)
  svmData.printSchema()

  val lr = new LinearRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)
  val lrModel = lr.fit(svmData)

  MLHelper.LinearRegression.describe(lrModel)
}
