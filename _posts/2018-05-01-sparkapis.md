---
title: "Spark APIs -- Scala, Java, Python RDD vs. DataFrame vs. DataSet"
date: 2018-05-01
---

## Spark APIs: RDD, DataFrame, DataSet in Scala, Java, Python 

Once upon a time there was only one way to use Apache Spark but support for additional programming languages and APIs have been introduced in recent times. A novice can be confused by the different options that have become available since Spark 1.6 and intimidated by the idea of setting up a project to explore these APIs. I'm going to show how to use Spark with three different APIs in three different programming languages by implementing the _Hello World_ of Big Data, _Word Count_, for each combination.

### Project Setup: IntelliJ, Maven, JVM, Fat JARs
[This repo](https://github.com/g1thubhub/bdrecipes) contains everything needed for running Spark or Pyspark locally and can be used as a template for more complex projects. Apache Maven is used as a build tool so dependencies, build configurations etc. are specified in a [POM file](https://github.com/g1thubhub/bdrecipes/blob/master/pom.xml). After cloning, the project can be opened and executed with Maven from the terminal or imported with a modern IDE like Intellij via:  ```
File => New => Project From Existing Sources => Open => Import project from external model => Maven ```

To build the project without an IDE, go to its source directory and execute the command ```mvn clean package``` which compiles an "uber" or "fat" JAR at _bdrecipes/target/**top-modules-1.0-SNAPSHOT.jar**_. This file is a self-contained unit that is executable so it will contain all dependencies specified in the [POM](https://github.com/g1thubhub/bdrecipes/blob/master/pom.xml). If you run your application on a cluster, the Spark dependencies are already "there" and shouldn't be included in the JAR that contains the program code and other non-Spark dependencies. This could be done by adding a _provided_ scope to the two the Spark dependencies (_spark-sql_2.11_ and _spark-core_2.11_) in the [POM](https://github.com/g1thubhub/bdrecipes/blob/master/pom.xml).

Run an application using the command ```java -cp target/top-modules-1.0-SNAPSHOT.jar``` or ```scala -cp target/top-modules-1.0-SNAPSHOT.jar``` plus the fully qualified name of the class that should be executed. To run the DataSet API example for both Scala and Java, use the following commands:
```
scala -cp target/top-modules-1.0-SNAPSHOT.jar spark.apis.wordcount.Scala_DataSet

java -cp target/top-modules-1.0-SNAPSHOT.jar spark.apis.wordcount.Java_DataSet
```

### PySpark Setup
Python is not a JVM-based language and the Python scripts that are included in the repo are actually completely independent from the Maven project and its dependencies. In order to run the Python examples, you need to install _pyspark_ which I did on MacOS via
```pip3 install pyspark```.
The scripts can be run from an IDE or from the terminal via  ```python3 python_dataframe.py```


### Implementation

Each Spark program will implement the same simple _Word Count_ logic:
1.	Read the lines of a text file; _Moby Dick_ will be used here
2. 	Extract all words from those lines and normalize them. For simplicity, we will just split the lines on whitespace here and lowercase the words 
3.	Count how often each element occurs
4.	Create an output file that contains the element and its occurrence frequency

The solutions for the various combinations using the most recent version of Spark (2.3) can be found here:
* [Scala + RDD](https://github.com/g1thubhub/bdrecipes/blob/master/src/main/java/spark/apis/wordcount/Scala_RDD.scala)
* [Scala + DataFrame](https://github.com/g1thubhub/bdrecipes/blob/master/src/main/java/spark/apis/wordcount/Scala_DataFrame.scala)
* [Scala + DataSet](https://github.com/g1thubhub/bdrecipes/blob/master/src/main/java/spark/apis/wordcount/Scala_DataSet.scala)
* [Python + RDD](https://github.com/g1thubhub/bdrecipes/blob/master/src/main/java/spark/apis/wordcount/python_rdd.py)
* [Python + DataFrame](https://github.com/g1thubhub/bdrecipes/blob/master/src/main/java/spark/apis/wordcount/python_dataframe.py)
* [Java + RDD](https://github.com/g1thubhub/bdrecipes/blob/master/src/main/java/spark/apis/wordcount/Java_RDD.java)
* [Java + DataFrame](https://github.com/g1thubhub/bdrecipes/blob/master/src/main/java/spark/apis/wordcount/Java_DataFrame.java)
* [Java + DataSet](https://github.com/g1thubhub/bdrecipes/blob/master/src/main/java/spark/apis/wordcount/Java_DataSet.java)

These source files should contain enough comments so there is no need to describe the code in detail here.  