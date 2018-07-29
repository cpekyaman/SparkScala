package section12

import commons.Constants
import org.apache.spark.ml.classification.LogisticRegression
import udemy.spark.commons.{LibsvmLoader, MLHelper, SparkHelper}

object LogisticRegressionOverview extends App {
  private val session = SparkHelper.startSessionWithDF("LogisticRegression", Constants.resourcesRootPath,"ml/classification/sample_libsvm_data.txt", LibsvmLoader)

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val lrModel = lr.fit(session.df)

  MLHelper.LogisticRegression.describe(lrModel)

}
