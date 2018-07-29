package section13

import commons.Constants
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import udemy.spark.commons.{LibsvmLoader, SparkHelper}

object ModelEvaluationOverview extends App {
  private val session = SparkHelper.startSessionWithDF("ModelEvaluationOV",
    Constants.resourcesRootPath, Constants.lregLibsvmSampleDataPath, LibsvmLoader)

  val Array(training, test) = session.df.randomSplit(Array(0.9, 0.1), seed = 12345)

  val lr = new LinearRegression()
  val parameterGrid = new ParamGridBuilder()
    .addGrid(lr.regParam, Array(0.1, 0.01))
    .addGrid(lr.fitIntercept)
    .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
    .build()

  val tvs = new TrainValidationSplit()
    .setEstimator(lr)
    .setEvaluator(new RegressionEvaluator())
    .setEstimatorParamMaps(parameterGrid)
    .setTrainRatio(0.8)
    .setParallelism(2)

  val model = tvs.fit(training)

  model.transform(test)
    .select("features", "label", "prediction")
    .show()
}
