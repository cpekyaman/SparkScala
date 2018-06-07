package section12

import commons._

object LogisticRegressionWalkthrough extends App {
  private val session = SparkHelper.startSessionWithDF("LogisticRegressionWT", "ml/classification/titanic.csv")
  private val featureColumnNames = Array("Pclass", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked")

  private val ts = TransformSteps(Array(TransformStep("Sex", "SexIndex", "SexVec"), TransformStep("Embarked", "EmbarkIndex", "EmbarkVec")))

  MLHelper.LogisticRegression.run(session, new FeatureData("Survived", featureColumnNames), ts)
}
