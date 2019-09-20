package org.apache.spark.run

import org.apache.log4j.Logger
import utils.keel.KeelParser
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.evaluation.Metrics
import org.apache.spark.mllib.evaluation.NeighborhoodDensity
import org.apache.spark.mllib.evaluation.DecisionTreeProgresion
import org.apache.spark.mllib.classification.kNN_IS.kNN_IS
import scala.math.abs

object runMetrics extends Serializable {

  var sc: SparkContext = null

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

    if (arg.length != 4) {
      logger.error("=> wrong parameters number")
      System.err.println("Parameters \n\t<path-to-dataset>\n\t<path-to-output>\n\t<model>\n\t<numPartitionMap>")
      System.exit(1)
    }

    //Reading parameters
    val pathDataset = arg(0)
    val pathOutput = arg(1)
    val model = arg(2)
    val k = 1.toInt
    val numPartitionMap = arg(3).toInt
    var timeBegRead: Long = 0l
    var timeEndRead: Long = 0l
    var timeBegF1: Long = 0l
    var timeEndF1: Long = 0l
    var timeBegF2: Long = 0l
    var timeEndF2: Long = 0l
    var timeBegF3: Long = 0l
    var timeEndF3: Long = 0l
    var timeBegF4: Long = 0l
    var timeEndF4: Long = 0l
    var timeBegT2: Long = 0l
    var timeEndT2: Long = 0l
    var timeBegC1: Long = 0l
    var timeEndC1: Long = 0l
    var timeBegC2: Long = 0l
    var timeEndC2: Long = 0l

    //Clean pathOutput for set the jobName
    var outDisplay: String = pathOutput

    //Basic setup
    val jobName = "Metrics"

    //Spark Configuration
    val conf = new SparkConf().setAppName(jobName)
    sc = new SparkContext(conf)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToDataset \"" + pathDataset + "\"")
    logger.info("=> pathToOuput \"" + pathOutput + "\"")

    timeBegRead = System.nanoTime
    //Reading the dataset
    val data = sc.textFile(pathDataset, 256).map { line =>
      val array = line.split(",")
      var arrayDouble = array.map(f => f.toDouble)
      val featureVector = Vectors.dense(arrayDouble.init)
      val label = arrayDouble.last
      LabeledPoint(label, featureVector)
    }.cache

    var output = new ListBuffer[String]
    output += "***Report.txt ==> Contain: Metrics and runtimes***\n"


    if (model == "ND") {
      val nd = new NeighborhoodDensity(data = data, k, numPartitionMap, numReduces = 10, seed = 123456789)
      val ndStr = nd.ND
      timeEndRead = System.nanoTime
      val ndTime = (timeEndRead - timeBegRead) / 1e9
      output += "@ND\n" + ndStr + "\n@NDRuntime\n" + ndTime + "\n"
    }else if (model == "DTP"){
      val dtp = new DecisionTreeProgresion(data = data, numPartitionMap = numPartitionMap)
      val dtpStr = dtp.DTP
      timeEndRead = System.nanoTime
      val ndTime = (timeEndRead - timeBegRead) / 1e9
      output += "@DTP\n" + dtpStr + "\n@DTPRuntime\n" + ndTime + "\n"
    }else{
      var metrics = new Metrics(data)
      timeEndRead = System.nanoTime
      val readTime = (timeEndRead - timeBegRead) / 1e9
      output +="@ComputeStatictisRuntime\n" + readTime + "\n"

      timeBegF1 = System.nanoTime
      val F1 = metrics.F1
      timeEndF1 = System.nanoTime
      val F1Time = (timeEndF1 - timeBegF1) / 1e9
      output +="@F1\n" + F1 + "\n@F1Runtime\n" + F1Time + "\n"

      timeBegF2 = System.nanoTime
      val F2 = metrics.F2
      timeEndF2 = System.nanoTime
      val F2Time = (timeEndF2 - timeBegF2) / 1e9
      output +="@F2\n" + F2 + "\n@F2Runtime\n" + F2Time + "\n"

      timeBegF3 = System.nanoTime
      val F3 = metrics.F3
      timeEndF3 = System.nanoTime
      val F3Time = (timeEndF3 - timeBegF3) / 1e9
      output +="@F3\n" + F3 + "\n@F3Runtime\n" + F3Time + "\n"

      timeBegF4 = System.nanoTime
      val F4 = metrics.F4
      timeEndF4 = System.nanoTime
      val F4Time = (timeEndF4 - timeBegF4) / 1e9
      output +="@F4\n" + F4 + "\n@F4Runtime\n" + F4Time + "\n"

      timeBegT2 = System.nanoTime
      val T2 = metrics.T2
      timeEndT2 = System.nanoTime
      val T2Time = (timeEndT2 - timeBegT2) / 1e9
      output +="@T2\n" + T2 + "\n@T2Runtime\n" + T2Time + "\n"

      timeBegC1 = System.nanoTime
      val C1 = metrics.C1
      timeEndC1 = System.nanoTime
      val C1Time = (timeEndC1 - timeBegC1) / 1e9
      output +="@C1\n" + C1 + "\n@C1Runtime\n" + C1Time + "\n"

      timeBegC2 = System.nanoTime
      val C2 = metrics.C2
      timeEndC2 = System.nanoTime
      val C2Time = (timeEndC2 - timeBegC2) / 1e9
      output +="@C2\n" + C2 + "\n@C2Runtime\n" + C2Time + "\n"
    }

    data.count

    val Report = sc.parallelize(output, 1)
    Report.saveAsTextFile(pathOutput + "/Report.txt")
  }

}
