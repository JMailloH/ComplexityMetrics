package org.apache.spark.mllib.evaluation

import java.lang.Math.log
import scala.math.abs
import org.apache.spark.annotation.Since
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics

/**
  * Complexity metrics in supervised learning problems.
  *
  * @param data an RDD of LabeledPoint (features, label).
  * @param seed an RDD of LabeledPoint (features, label).
  * @author Jesus Maillo
  */
@Since("2.2.0")
class DecisionTreeProgresion @Since("2.2.0") (data: RDD[LabeledPoint], numPartitionMap: Int, impurity: String = "gini", maxDepth: Int = 20, maxBins: Int = 32, seed: Int = 123456789)  extends Serializable {
  private val labels = data.map(_.label).distinct.collect
  private val numClasses: Int = labels.length.toInt
  private val categoricalFeaturesInfo = Map[Int, Int]()

  /**
    * DTP. Decision Tree Progresion.
    * It returns the percentual difference between the accuracy obtained by training with the whole dataset and the half of the dataset.
    */
  @Since("2.2.0")
  def DTP(): Double = {
    val Array(train, test) = data.randomSplit(Array(0.9, 0.1), seed)
    var accuracyTrain: Double = computeDTAccuracy(train, test)

    val trainSubsample = train.sample(false, 0.5, seed)
    var accuracyTrainSubSample: Double = computeDTAccuracy(trainSubsample, test)

    ((accuracyTrain-accuracyTrainSubSample)/accuracyTrain)*(100.0d)
  }

  /****************************************************/
  /*Auxiliary methods for the calculation of metric.**/
  /****************************************************/

  /**
    * Compute the training phase and obtain the accuracy by classifing the test.
    */
  @Since("2.2.0")
  private def computeDTAccuracy(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Double = {

    val model = DecisionTree.trainClassifier(train, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    val labelAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }.persist
    labelAndPreds.count

    val metrics = new MulticlassMetrics(labelAndPreds)
    metrics.accuracy
  }

}