package section13

import commons.SparkHelper

object ModelEvaluationWalkthrough extends App {
  private val session = SparkHelper.startSessionWithDF("ModelEvaluationWT", "ml/regression/Clean-USA-Housing.csv")
  private val featureColumnNames = Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population")

  //TODO: use vector assembler to transform data then apply trainvalidationsplit

}
