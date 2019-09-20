#!/bin/bash

spark-submit --master local[*] --class org.apache.spark.run.runMetrics ./target/ComplexityMetrics-1.0.jar <path-to-dataset> <path-to-output> ClassicMetrics 10
spark-submit --master local[*] --class org.apache.spark.run.runMetrics ./target/ComplexityMetrics-1.0.jar <path-to-dataset> <path-to-output> ND 10
spark-submit --master local[*] --class org.apache.spark.run.runMetrics ./target/ComplexityMetrics-1.0.jar <path-to-dataset> <path-to-output> DTP 10

