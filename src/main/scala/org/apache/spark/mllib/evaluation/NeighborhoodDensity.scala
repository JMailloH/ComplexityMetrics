package org.apache.spark.mllib.evaluation

import scala.collection.Map
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.classification.kNN_IS.kNN_IS
import java.lang.Math.log
import scala.math.abs

/**
  * Complexity metrics in supervised learning problems.
  *
  * @param data an RDD of LabeledPoint (features, label).
  * @param seed an RDD of LabeledPoint (features, label).
  * @author Jesus Maillo
  */
@Since("2.2.0")
class NeighborhoodDensity @Since("2.2.0") (data: RDD[LabeledPoint], k: Int = 1, numPartitionMap: Int, numReduces: Int, seed: Int = 123456789)  extends Serializable {
  private val distanceType = 0 // 0 -> Euclidean | 1 -> Manhattan

  /**
    * ND. Neighborhood Density.
    * Return the percentual difference between the mean Euclidean distance with all the dataset and randomly dropping the half of the dataset.
    */
  @Since("2.2.0")
  def ND(): Double = {
    val Array(neighborhood, validation) = data.randomSplit(Array(0.9, 0.1), seed)
    val neighborhoodSubsample = neighborhood.sample(false, 0.5, seed)

    var averageDistance: Double = computeAverageDistance(neighborhood, validation)
    var averageDistanceSubSample: Double = computeAverageDistance(neighborhoodSubsample, validation)

    (abs(averageDistance-averageDistanceSubSample)/averageDistance)*(100.0d)
  }

  /****************************************************/
  /*Auxiliary methods for the calculation of metrics.**/
  /****************************************************/

  /**
    * Compute the average of the distance.
    */
  @Since("2.2.0")
  private def computeAverageDistance(dataA: RDD[LabeledPoint], dataB: RDD[LabeledPoint]): Double = {
    val knn = kNN_IS.setup(dataA, dataB, k, distanceType, numPartitionMap, numReduces)
    val predictions = knn.averageDistance()
    val avgDist: MultivariateStatisticalSummary = Statistics.colStats(predictions)
    avgDist.mean(0).toDouble
  }

}