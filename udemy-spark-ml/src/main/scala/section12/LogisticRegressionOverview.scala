package section12

import commons.{Constants, MLHelper, SparkHelper}
import org.apache.spark.ml.classification.LogisticRegression

object LogisticRegressionOverview extends App {
  private val session = SparkHelper.localSession("LogisticRegression")

  private val svmFilePath = s"${Constants.resourcesRootPath}/ml/classification/sample_libsvm_data.txt"
  private val svmData = session.read.format("libsvm").load(svmFilePath)
  svmData.printSchema()

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val lrModel = lr.fit(svmData)

  MLHelper.LogisticRegression.describe(lrModel)

}
