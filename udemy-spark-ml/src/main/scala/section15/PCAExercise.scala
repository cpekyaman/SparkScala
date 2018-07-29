package section15

import commons.Constants
import org.apache.spark.ml.feature.{PCA, StandardScaler}
import udemy.spark.commons.{MLHelper, SparkHelper}

object PCAExercise extends App {
  private val session = SparkHelper.startSessionWithDF("PCAExercise", Constants.resourcesRootPath,"ml/pca/Cancer_Data")

  val featureColumnNames = Array("mean radius", "mean texture", "mean perimeter", "mean area", "mean smoothness",
    "mean compactness", "mean concavity", "mean concave points", "mean symmetry", "mean fractal dimension",
    "radius error", "texture error", "perimeter error", "area error", "smoothness error", "compactness error",
    "concavity error", "concave points error", "symmetry error", "fractal dimension error", "worst radius",
    "worst texture", "worst perimeter", "worst area", "worst smoothness", "worst compactness", "worst concavity",
    "worst concave points", "worst symmetry", "worst fractal dimension")

  val data = MLHelper.extractFeatures(session.df, featureColumnNames)

  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(false)

  val scaledData = scaler.fit(session.df).transform(session.df)

  val pcaDf = new PCA()
    .setInputCol("scaledFeatures")
    .setOutputCol("pcaFeatures")
    .setK(4)
    .fit(scaledData)
    .transform(scaledData)

  pcaDf.select("pcaFeatures").show()
}
