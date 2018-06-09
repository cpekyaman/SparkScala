package section14

import commons.{MLHelper, SparkHelper}
import org.apache.spark.ml.clustering.KMeans

object ClusteringExercise extends App {
  private val session = SparkHelper.startSessionWithDF("ClusteringExercise", "ml/clustering/wholesale-customers-data.csv")
  private val featureColumnNames = Array("Channel", "Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen")

  val kmeans = new KMeans().setK(4).setSeed(System.currentTimeMillis())
  val data = MLHelper.extractFeatures(session.df, featureColumnNames)
  val model = kmeans.fit(data)

  val wsse = model.computeCost(data)
  println(s"wsse is $wsse")
  model.clusterCenters.foreach(println)
}
