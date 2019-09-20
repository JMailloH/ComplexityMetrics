package org.apache.spark.mllib.evaluation

import scala.collection.Map
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import java.lang.Math.log

/**
  * Complexity metrics in supervised learning problems.
  *
  * @param data an RDD of LabeledPoint (features, label).
  * @author Jesus Maillo
  */
@Since("2.2.0")
class Metrics @Since("2.2.0") (data: RDD[LabeledPoint])  extends Serializable {

  private val labels = data.map(_.label).distinct.collect
  private val numClasses: Int = labels.length
  private val numFeatures: Int = data.first.features.size
  private val numSamples: Int = data.cache.count.toInt
  private var numSamples_c =  Array.ofDim[Int](numClasses)
  private var max =  Array.ofDim[Double](numFeatures)
  private var min = Array.ofDim[Double](numFeatures)
  private var mean = Array.ofDim[Double](numFeatures)
  private var max_c =  Array.ofDim[Double](numClasses, numFeatures)
  private var min_c =  Array.ofDim[Double](numClasses, numFeatures)
  private var mean_c =  Array.ofDim[Double](numClasses, numFeatures)
  private var minmin = Array.ofDim[Double](numFeatures)
  private var minmax = Array.ofDim[Double](numFeatures)
  private var maxmax = Array.ofDim[Double](numFeatures)
  private var maxmin = Array.ofDim[Double](numFeatures)
  this.computeStatictis(data)

  /**
    * F1. Maximum Fisher's discriminant ratio.
    */
  @Since("2.2.0")
  def F1(): Double = {
    var F1_aux: Double = 0.0d
    var F1: Double = 0.0d

    // Compute the numerator
    var numerator = Array.ofDim[Double](numFeatures)
    for (i <- 0 until numClasses)
      for (j <- 0 until numFeatures)
        numerator(j) = numSamples_c(i) * (mean_c(i)(j) - mean(j)) * (mean_c(i)(j) - mean(j))

    // Compute the denominator. Later, the sum is calculated by multiplying the mean by the number of samples.
    val denominator = Statistics.colStats(data.map{ row =>
      var aux_denominator = Array.ofDim[Double](numFeatures)
      val features = row.features
      val label = row.label

      for (i <- 0 until numClasses)
        if (label == labels(i))
          for (j <- 0 until numFeatures)
            aux_denominator(j) = (features(j)-mean_c(i)(j))*(features(j)-mean_c(i)(j))

      Vectors.dense(aux_denominator)
    }).mean

    // Calculation of F1 for each feature, keeping the maximum.
    for (i <- 0 until numFeatures){
      F1_aux = (numerator(i)/(denominator(i)*numSamples))
      if (F1_aux > F1)
        F1 = F1_aux
    }

    F1
  }


  /**
    * F2. Volume of overlapping region.
    */
  @Since("2.2.0")
  def F2(): Double = {
    var overlap: Double = 1.0d
    var range: Double = 1.0d

    for (i <- 0 until numFeatures){
      range = range*(maxmax(i)-minmin(i))
      var aux = minmax(i)-maxmin(i)
      if(0 > aux)
        aux = 0
      overlap = overlap*aux
    }

    overlap/range
  }


  /**
    * F3. Maximum Individual Feature Efficiency.
    */
  @Since("2.2.0")
  def F3(): Double = {
    var F3_aux: Double = 0.0d
    var F3: Double = 0.0d
    var numSamplesOverlap: Double = 0.0d

    for (i: Int <- 0 until numFeatures){
      numSamplesOverlap = data.filter{ row =>
        var bool_exit = false
        var feature_value: Double = row.features(i)
        if(feature_value > maxmin(i) && feature_value < minmax(i)){
          bool_exit = true
        }else{
          bool_exit = false
        }
        bool_exit
      }.count.toDouble

      F3_aux = (numSamples - numSamplesOverlap)/numSamples
      if (F3_aux > F3)
        F3 = F3_aux
    }

    F3
  }

  /**
    * F4. Collective feature Efficiency.
    */
  @Since("2.2.0")
  def F4(): Double = {
    var exitOne = false
    var F4: Double = 0.0d
    var numSamplesOverlap: Double = 0.0d
    var numSamplesOverlap_min: Double = Double.MaxValue
    var featureRemoved: Int = 0
    var auxData: RDD[Array[Double]] = data.map(_.features.toArray)
    var realIndex = (0 until numFeatures).toArray
    var x: Int = 0
    var i: Int = 0

    while((x < numFeatures) && (!exitOne)){
      i = 0
      numSamplesOverlap_min = Double.MaxValue
      while((i < (numFeatures-x)) && (!exitOne)){
        numSamplesOverlap = auxData.filter { row =>
          if (row(i) > maxmin(realIndex(i)) && row(i) < minmax(realIndex(i)))
            true
          else
            false
        }.count.toDouble

        if (numSamplesOverlap < numSamplesOverlap_min) {
          numSamplesOverlap_min = numSamplesOverlap
          featureRemoved = i
        }

        if (numSamplesOverlap == 0)
          exitOne = true

        i = i + 1
      }

      if(!exitOne) {
        auxData = auxData.filter { line =>
          if (line(featureRemoved) > maxmin(realIndex(featureRemoved)) && line(featureRemoved) < minmax(realIndex(featureRemoved)))
            true
          else
            false
        }.map { row =>
          row.take(featureRemoved) ++ row.drop(featureRemoved)
        }
        auxData.count()
        realIndex = realIndex.take(featureRemoved) ++ realIndex.drop(featureRemoved)
      }
      F4 = (numSamples - numSamplesOverlap)/numSamples
      x = x + 1
    }

    if (exitOne)
      1.0d
    else
      F4
  }

  /**
    * T2. Average number of points per dimension.
    */
  @Since("2.2.0")
  def T2(): Double = {
    numSamples.toFloat / numFeatures.toFloat
  }

  /**
    * C1. Entropy of class proportions.
    */
  @Since("2.2.0")
  def C1(): Double = {
    var summatory: Double = 0.0d
    val firstTerm: Double = (-1.0d)/log(numClasses.toDouble)

    for (i: Int <- 0 until numClasses){
      summatory = summatory + ((numSamples_c(i).toDouble/numSamples.toDouble)*log(numSamples_c(i).toDouble/numSamples.toDouble))
    }
    firstTerm * summatory
  }

  /**
    * C2. Imbalance ratio.
    */
  @Since("2.2.0")
  def C2(): Double = {
    var summatory: Double = 0.0d
    val firstTerm: Double = (numClasses.toDouble-1)/numClasses.toDouble

    for (i: Int <- 0 until numClasses){
      summatory = summatory + (numSamples_c(i).toDouble/(numSamples.toDouble-numSamples_c(i).toDouble))
    }
    firstTerm * summatory
  }

  /****************************************************/
  /*Auxiliary methods for the calculation of metrics.**/
  /****************************************************/

  /**
    * Computation of the basic statistics necessary to obtain the metrics
    */
  @Since("2.2.0")
  private[mllib] def computeStatictis(data: RDD[LabeledPoint]) {
    // Compute column summary statistics.
    val statictis: MultivariateStatisticalSummary = Statistics.colStats(data.map(_.features))
    for (i <- 0 until numFeatures){
      max(i) = statictis.max(i)
      min(i) = statictis.min(i)
      mean(i) = statictis.mean(i)
    }

    val statictis_c = Array.ofDim[MultivariateStatisticalSummary](numClasses)
    var it: Int = 0
    labels.foreach{ x =>
      val data_class = data.filter(line => line.label == x).map(_.features)
      var aux_statistics = Statistics.colStats(data.filter(line => line.label == x).map(_.features))
      numSamples_c(it) = data_class.count.toInt
      for (i <- 0 until numFeatures){
        max_c(it)(i) = aux_statistics.max(i)
        min_c(it)(i) = aux_statistics.min(i)
        mean_c(it)(i) = aux_statistics.mean(i)
      }
      it+=1
    }

    for (i <- 0 until numFeatures){
      minmin(i) = minminF(i)
      minmax(i) = minmaxF(i)
      maxmax(i) = maxmaxF(i)
      maxmin(i) = maxminF(i)
    }

  }

  /**
    * Returns the minimum of the maximums per class.
    * @param ith Feature that we want to obtain.
    */
  private def minmaxF(ith: Int): Double ={
    var minmax_aux = Double.MaxValue
    var minmax = Double.MaxValue
    for (i <- 0 until numClasses){
      minmax_aux = max_c(i)(ith)
      if (minmax_aux < minmax)
        minmax = minmax_aux
    }
    minmax
  }

  /**
    * Returns the minimum of the minimum per class.
    * @param ith Feature that we want to obtain.
    */
  private def minminF(ith: Int): Double ={
    var minmin_aux = Double.MaxValue
    var minmin = Double.MaxValue
    for (i <- 0 until numClasses){
      minmin_aux = min_c(i)(ith)
      if (minmin_aux < minmin)
        minmin = minmin_aux
    }
    minmin
  }

  /**
    * Returns the maximums of the minimum per class.
    * @param ith Feature that we want to obtain.
    */
  private def maxminF(ith: Int): Double ={
    var maxmin_aux = 0.0d
    var maxmin = 0.0d
    for (i <- 0 until numClasses){
      maxmin_aux = min_c(i)(ith)
      if (maxmin_aux > maxmin)
        maxmin = maxmin_aux
    }
    maxmin
  }

  /**
    * Returns the maximums of the maximums per class.
    * @param ith Feature that we want to obtain.
    */
  private def maxmaxF(ith: Int): Double ={
    var maxmax_aux = 0.0d
    var maxmax = 0.0d
    for (i <- 0 until numClasses){
      maxmax_aux = max_c(i)(ith)
      if (maxmax_aux > maxmax)
        maxmax = maxmax_aux
    }
    maxmax
  }

}