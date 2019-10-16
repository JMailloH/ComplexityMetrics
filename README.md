# Complexity Metrics for Big Data problems.

There is currently a wide availability of data mining algorithms in big data, however, there are no specific metrics focused on tackling complexity and redundancy problems in large datasets. Thus, we propose to answer the following question: do classification algorithms need so much data?

To address this objective, we propose two metrics and the implementation of several state of the art metrics, adapting them to the big data problem. This repository is a open-source package that includes complexity metrics. This package includes two original proposals for studying the density and complexity of large datasets:
 - ND: Neighborhood Density, this metric returns the percentual difference of the euclidean distance, calculated with all available data, and with half of the randomly chosen data.
 - DTP: Decision Tree Progression, this metric returns the accuracy percentage difference by training a Decision Tree with the totality of the data, and discarding half of them randomly.
 
It also includes some state-of-the-art metrics [1], which have been designed and developed to run large datasets.
 - F1: Maximum Fisher's discriminant ratio
 - F2: Volume of overlapping region
 - F3: Maximum individual feature efficiency
 - F4: Collective feature efficiency
 - C1: Entropy of class portions
 - C2: Imbalance ratio
 
If you want to know more about the metrics and experiments carried out and the conclusions obtained, please consult and cite the following reference 
## Cite this software as:
[README under construction. Reference to be filled in]

## References
[1] Lorena, A. C., Garcia, L. P., Lehmann, J., Souto, M. C., & Ho, T. K. (2019). How Complex Is Your Classification Problem?: A Survey on Measuring Classification Complexity. ACM Computing Surveys (CSUR), 52(5), 107.
 
# How to use

## Pre-requiriments and software version
The following software have to get installed:
- Scala. Version 2.11
- Spark. Version 2.3.2
- Maven. Version 3.5.2
- JVM. Java Virtual Machine. Version 1.8.0 because Scala run over it.

## Download and build with maven
- Download source code: It is host on GitHub. To get the sources and compile them we will need the next git instruction.
```git clone https://github.com/JMailloH/ComplexityMetrics.git ```
- Build jar file: Once we have the sources, we generate the .jar file of the project by the next maven instruction.
```mvn package -Dmaven.test.skip=true. ```

Another alternative is download by spark-package at [https://spark-packages.org/package/JMailloH/ComplexityMetrics](https://spark-packages.org/package/JMailloH/ComplexityMetrics)


## How to run
If you want to run the software and obtain the result of each one of the metrics, you can consult the example file: [runMetrics.scala](https://github.com/JMailloH/ComplexityMetrics/tree/master/src/main/scala/run/runMetrics.scala).

The run.sh file contains an example call for each algorithm. A generic sample of run could be: 

spark-submit --master "URL" --class org.apache.spark.run.runMetrics ./target/ComplexityMetrics-1.0.jar "path-to-dataset" "path-to-output" "Metric" "number-of-maps" 

- ```--class org.apache.spark.run.runMetrics ./target/ComplexityMetrics-1.0.jar``` Determine the jar file to be run.
- ```"path-to-dataset"``` Path from HDFS to dataset.
- ```"path-to-output"``` Path from HDFS to output.
- ```"Metric"``` ND, DTP or ClassicMetrics (to compute the rest of the metrics).
- ```"number-of-maps"``` Number of map tasks.
