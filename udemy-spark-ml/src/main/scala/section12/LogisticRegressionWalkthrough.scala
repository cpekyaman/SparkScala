package section12

import commons.Constants
import udemy.spark.commons._

object LogisticRegressionWalkthrough extends App {
  private val session = SparkHelper.startSessionWithDF("LogisticRegressionWT", Constants.resourcesRootPath, "ml/classification/titanic.csv")
  private val featureColumnNames = Array("Pclass", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked")

  private val ts = TransformSteps(Array(TransformStep("Sex", "SexIndex", "SexVec"), TransformStep("Embarked", "EmbarkIndex", "EmbarkVec")))

  MLHelper.LogisticRegression.run(session, FeatureData("Survived", featureColumnNames), ts)
}
