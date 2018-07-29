package udemy.spark.commons

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Column, DataFrame, Dataset}

class MLSession(val model: PipelineModel, val training: Dataset[_], val test: Dataset[_])

/**
  * Used to select columns relevant to analysis
  *
  * @param labelColumn
  * @param featureColumns
  */
class FeatureData(val labelColumn: String, val featureColumns: Array[String]) {
  def toColumns(df: DataFrame): Array[Column] = df(labelColumn).as("label") +: featureColumns.map(df(_))

  def toTransformedColumns(ts: TransformSteps): Array[String] = "label" +: featureColumns.map(c => transformedName(c, ts))

  private def transformedName(col: String, ts: TransformSteps): String = {
    ts.steps.find(s => s.column.equals(col)) match {
      case Some(t) => t.encodedColumn
      case None => col
    }
  }
}
object FeatureData {
  def apply(labelColumn: String, featureColumns: Array[String]): FeatureData =
    new FeatureData(labelColumn, featureColumns)
}

/**
  * Represents each categorical column transform step
  *
  * @param column
  * @param indexColumn
  * @param encodedColumn
  */
class TransformStep(val column: String, val indexColumn: String, val encodedColumn: String)
object TransformStep {
  def apply(column: String, indexColumn: String, encodedColumn: String): TransformStep =
    new TransformStep(column, indexColumn, encodedColumn)
}

/**
  * Creates actual transformers for categorical columns
  *
  * @param steps
  */
class TransformSteps(val steps: Array[TransformStep]) {
  def indexers(): Array[StringIndexer] = steps.map(indexer)

  def encoders(): Array[OneHotEncoderEstimator] = steps.map(encoder)

  private def indexer(s: TransformStep): StringIndexer =
    new StringIndexer().setInputCol(s.column).setOutputCol(s.indexColumn)

  private def encoder(s: TransformStep): OneHotEncoderEstimator =
    new OneHotEncoderEstimator().setInputCols(Array(s.indexColumn)).setOutputCols(Array(s.encodedColumn))
}
object TransformSteps { def apply(steps: Array[TransformStep]): TransformSteps = new TransformSteps(steps) }
object EmptyTransformSteps extends TransformSteps(Array())

object MLHelper {

  object LinearRegression {
    def run(session: Session, featureData: FeatureData): Unit = {
      val mlSession = prepareMLSession(session, featureData, EmptyTransformSteps, new LinearRegression())
      val results = mlSession.model.transform(mlSession.test)
      results.printSchema()
    }

    def describe(lrModel: LinearRegressionModel): Unit = {
      println(s"coefficients: ${lrModel.coefficients}, intercept: ${lrModel.intercept}")

      val summary = lrModel.summary
      println(s"iterations ${summary.totalIterations}")
      summary.residuals.show()

      println(s"RMSE ${summary.rootMeanSquaredError}")
      println(s"r2 ${summary.r2}")
    }
  }

  object LogisticRegression {
    def run(session: Session, featureData: FeatureData, transformSteps: TransformSteps): Unit = {
      import session.spark.implicits._

      val mlSession = prepareMLSession(session, featureData, transformSteps, new LogisticRegression())
      val results = mlSession.model.transform(mlSession.test)
      val predictionAndLabels = results.select($"prediction", $"label").as[(Double, Double)].rdd

      val metrics = new MulticlassMetrics(predictionAndLabels)

      // Confusion matrix
      println("Confusion matrix:")
      println(metrics.confusionMatrix)
    }

    def describe(lrModel: LogisticRegressionModel): Unit = {
      println(s"coefficients: ${lrModel.coefficients}, intercept: ${lrModel.intercept}")

      val summary = lrModel.summary
      println(s"iterations ${summary.totalIterations}")
    }
  }

  def extractFeatures(ds: DataFrame, cols: Array[String]): DataFrame = {
    new VectorAssembler().setInputCols(cols).setOutputCol("features").transform(ds).select("features")
  }

  def prepareMLSession(session: Session, fd: FeatureData, ts: TransformSteps, finalStage: PipelineStage): MLSession = {
    println("Original dataset model:")
    session.df.printSchema()

    val columns = fd.toColumns(session.df)
    val lrData = session.df.select(columns: _*).na.drop()

    println("filtered dataset model:")
    lrData.printSchema()

    val Array(training, test) = lrData.randomSplit(Array(0.7, 0.3), seed = System.currentTimeMillis())

    val assembler = new VectorAssembler().setInputCols(fd.toTransformedColumns(ts)).setOutputCol("features")

    val pipeline = new Pipeline().setStages(ts.indexers() ++ ts.encoders() :+ assembler :+ finalStage)
    val model = pipeline.fit(training)

    new MLSession(model, training, test)
  }
}
