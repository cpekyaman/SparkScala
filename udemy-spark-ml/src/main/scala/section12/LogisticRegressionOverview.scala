package section12

import commons.{LibsvmLoader, MLHelper, SparkHelper}
import org.apache.spark.ml.classification.LogisticRegression

object LogisticRegressionOverview extends App {
  private val session = SparkHelper.startSessionWithDF("LogisticRegression", "ml/classification/sample_libsvm_data.txt", LibsvmLoader)

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  val lrModel = lr.fit(session.df)

  MLHelper.LogisticRegression.describe(lrModel)

}
