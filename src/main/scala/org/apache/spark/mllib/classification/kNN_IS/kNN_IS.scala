package org.apache.spark.mllib.classification.kNN_IS

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import utils.keel.KeelParser

/**
 * Distributed kNN class.
 *
 *
 * @author Jesus Maillo
 */

class kNN_IS(train: RDD[LabeledPoint], test: RDD[LabeledPoint], k: Int, distanceType: Int, numPartitionMap: Int, numReduces: Int) extends Serializable {

  //Count the samples of each data set and the number of classes
  private var numSamplesTrain: Int = 0
  private var numSamplesTest: Int = 0
  private var testBroadcast: Broadcast[Array[LabeledPoint]] = null
  private var testWithKey: RDD[(Int, LabeledPoint)] = null

  //Setting Iterative MapReduce
  private def broadcastTest(test: Array[LabeledPoint], context: SparkContext) = context.broadcast(test)

  //Getters
  def getTrain: RDD[LabeledPoint] = train
  def getTest: RDD[LabeledPoint] = test
  def getK: Int = k
  def getDistanceType: Int = distanceType
  def getNumSamplesTrain: Int = numSamplesTrain
  def getNumSamplesTest: Int = numSamplesTest

  /**
   * Initial setting necessary. Auto-set the number of iterations and load the data sets and parameters.
   *
   * @return Instance of this class. *this*
   */
  def setup(): kNN_IS = {
    testWithKey = test.zipWithIndex().map { line => (line._2.toInt, line._1) }.sortByKey().cache
    testBroadcast = broadcastTest(test.collect, test.sparkContext)
    numSamplesTrain = train.count.toInt
    numSamplesTest = test.count.toInt

    this
  }

  /**
   * Predict. kNN
   *
   * @return RDD[Vector]. Average distance for each sample.
   */
  def averageDistance(): RDD[Vector] = {
    train.mapPartitions(split => knn(split, testBroadcast)).reduceByKey(combine).map { case (key, value) =>
      var averageDist =  Array.ofDim[Double](1)
      for (i <- 0 until k)
        averageDist(0) = averageDist(0) + value(i)
      averageDist(0) = averageDist(0) / k.toDouble
      Vectors.dense(averageDist)
    }
  }

  /**
   * Calculate the K nearest neighbor from the test set over the train set.
   *
   * @param iter Iterator of each split of the training set.
   * @param testSet The test set in a broadcasting
   * @return K Nearest Neighbors for this split
   */
  def knn[T](iter: Iterator[LabeledPoint], testSet: Broadcast[Array[LabeledPoint]]): Iterator[(Int, Array[Double])] = {
    // Initialization
    var train = new ArrayBuffer[LabeledPoint]
    val size = testSet.value.length

    var dist: Distance.Value = null
    //Distance MANHATTAN or EUCLIDEAN
    if (distanceType == 1)
      dist = Distance.Manhattan
    else
      dist = Distance.Euclidean

    //Join the train set
    while (iter.hasNext)
      train.append(iter.next)

    var knn = new KNN(train, k, dist)
    var result = new Array[(Int, Array[Double])](size)

    for (i <- 0 until size)
      result(i) = (i, knn.neighborsDist(testSet.value(i).features))

    result.iterator
  }

  /**
   * Join the result of the map taking the nearest neighbors.
   *
   * @param mapOut1 A element of the RDD to join
   * @param mapOut2 Another element of the RDD to join
   * @return Combine of both element with the nearest neighbors
   */
  def combine(mapOut1: Array[Double], mapOut2: Array[Double]): Array[Double] = {

    var itOut1 = 0
    var itOut2 = 0
    var out: Array[Double] = new Array[Double](k)

    var i = 0
    while (i < k) {
      if (mapOut1(itOut1) <= mapOut2(itOut2)) { // Update the matrix taking the k nearest neighbors
        out(i) = mapOut1(itOut1)
        if (mapOut1(itOut1) == mapOut2(itOut2)) {
          i += 1
          if (i < k) {
            out(i) = mapOut2(itOut2)
            itOut2 = itOut2 + 1
          }
        }
        itOut1 = itOut1 + 1

      } else {
        out(i) = mapOut2(itOut2)
        itOut2 = itOut2 + 1
      }
      i += 1
    }

    out
  }

}

/**
 * Distributed kNN class.
 *
 *
 * @author Jesus Maillo
 */
object kNN_IS {
  /**
   * Initial setting necessary.
   *
   * @param train Data that iterate the RDD of the train set
   * @param test The test set in a broadcasting
   * @param k number of neighbors
   * @param distanceType MANHATTAN = 1 ; EUCLIDEAN = 2
   * @param converter Dataset's information read from the header
   * @param numPartitionMap Number of partition. Number of map tasks
   * @param numReduces Number of reduce tasks
   * @param numIterations Autosettins = -1. Number of split in the test set and number of iterations
   */
  def setup(train: RDD[LabeledPoint], test: RDD[LabeledPoint], k: Int, distanceType: Int, numPartitionMap: Int, numReduces: Int) = {
    new kNN_IS(train, test, k, distanceType, numPartitionMap, numReduces).setup()
  }
}