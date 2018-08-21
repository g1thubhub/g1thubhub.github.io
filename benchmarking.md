---
layout: default
---

# Benchmarks, Spark and Graal
<p></p>
A very important question is how long something takes and the answer to that is fairly straightforward in normal life: Check the current time, then perform the unit of work that should be measured, then check the time again. The end time minus the start time would equal the amount of time that the task took, the **elapsed time** or wallclock time. The programmatic version of this simple measuring technique could look like

{% include Measure.html %}

In the case of Apache Spark, the _computation_ would likely be of type `Dataset[_]` or `RDD[_]`. In fact, the two third party benchmarking frameworks for Spark mentioned below are based on a function similar to the one shown above for measuring the execution time of a Spark job.  
<br>
It is surprisingly hard to accurately predict how long something will take in programming: The result from a single invocation of the naive method above is likely not very reliable since numerous non-deterministic factors can interfere with a measurement, especially when the underlying runtime applies dynamic optimizations like the Java Virtual Machine. Even the usage of a dedicated microbenchmarking framework like JMH only solves parts of the problem --  the user is reminded every time of that caveat after JMH completes:
```
[info] REMEMBER: The numbers below are just data. To gain reusable insights, you need to follow up on
[info] why the numbers are the way they are. Use profilers (see -prof, -lprof), design factorial
[info] experiments, perform baseline and negative tests that provide experimental control, make sure
[info] the benchmarking environment is safe on JVM/OS/HW level, ask for reviews from the domain experts.
[info] Do not assume the numbers tell you what you want them to tell.
```


## From the Apache Spark creators: _spark-sql-perf_
If benchmarking a computation on a local machine is already hard, then doing this for a distributed computation/environment should be very hard. [_spark-sql-perf_](https://github.com/databricks/spark-sql-perf) is the official performance testing framework for Spark 2. The following twelve benchmarks are particularly interesting since they target various features and APIs of Spark; they are organized into three classes :

<br>
[DatasetPerformance](https://github.com/g1thubhub/spark-sql-perf/blob/Fix_3_local_benchmark_classes/src/main/scala/com/databricks/spark/sql/perf/DatasetPerformance.scala) compares the same workloads expressed via RDD, Dataframe and Dataset API:
<br>
- **range** just creates 100 million integers of datatype _Long_ which are wrapped in a case class in the case of RDDs and Datasets
-  **filter** applies four consecutive filters to 100 million _Longs_ 
- **map** applies an increment operation 100 million _Longs_ four times
- **average** computes the average of one million _Longs_ using a user-defined function for Datasets, a built-in sql function for DataFrames and a map/reduce combination for RDDs.
<br> 
<br>
[JoinPerformance](https://github.com/g1thubhub/spark-sql-perf/blob/Fix_3_local_benchmark_classes/src/main/scala/com/databricks/spark/sql/perf/DatasetPerformance.scala) is based on three data sets with one million, 100 million and one billion _Longs_:
- **singleKeyJoins**: joins each one of the three basic data sets in a left table with each one of the three basic data sets in a right table via four different join types (Inner Join, Right Join, Left Join and Full Outer Join) 
- **varyDataSize**: joins two tables consisting of 100 million integers each with a 'data' column containing Strings of 5 different lengths (1, 128, 256, 512 and 1024 characters)
- **varyKeyType**: joins two tables consisting of 100 million integers and casts it to four different data types (_String_, _Integer_, _Long_ and _Double_)
- **varyNumMatches**
<br> 
<br>
[AggregationPerformance](https://github.com/g1thubhub/spark-sql-perf/blob/Fix_3_local_benchmark_classes/src/main/scala/com/databricks/spark/sql/perf/AggregationPerformance.scala):
- **varyNumGroupsAvg** and **twoGroupsAvg** both compute the average of one table column and group them by the other column. They differ in the cardinaliy and shape of the input tables used. <br>
The next two aggregation benchmarks use the three data sets that are also used in the _DataSetPerformance_ benchmark described above:
- **complexInput**: For each of the three integer tables, adds a single column together nine times
- **aggregates**: aggregates a single column via four different aggregation types (_SUM_, _AVG_, _COUNT_ and _STDDEV_)
<br> 
<br>

Running these benchmarks produces....... almost nothing, most of them are broken or will crash in the current state of the official _master_ branch due to various problems (issues with reflection, missing table registrations, wrong UDF pattern, ...):
```shell
$ bin/run --benchmark AggregationPerformance
[...]
[error] Exception in thread "main" java.lang.InstantiationException: com.databricks.spark.sql.perf.AggregationPerformance
[error]   at java.lang.Class.newInstance(Class.java:427)
[error]   at com.databricks.spark.sql.perf.RunBenchmark$$anonfun$6.apply(RunBenchmark.scala:81)
[error]   at com.databricks.spark.sql.perf.RunBenchmark$$anonfun$6.apply(RunBenchmark.scala:82)
[error]   at scala.util.Try.getOrElse(Try.scala:79)
[...]

$ bin/run --benchmark JoinPerformance
[...]
[error] Exception in thread "main" java.lang.InstantiationException: com.databricks.spark.sql.perf.JoinPerformance
[error]   at java.lang.Class.newInstance(Class.java:427)
[error]   at com.databricks.spark.sql.perf.RunBenchmark$$anonfun$6.apply(RunBenchmark.scala:81)
[error]   at com.databricks.spark.sql.perf.RunBenchmark$$anonfun$6.apply(RunBenchmark.scala:82)
[error]   at scala.util.Try.getOrElse(Try.scala:79)

```

I repaired these issues and was able to run all of these twelve benchmarks sucessfully; the fixed edition can be downloaded from my personal repo [here](https://github.com/g1thubhub/spark-sql-perf/tree/Fix_3_local_benchmark_classes), a [PR](https://github.com/databricks/spark-sql-perf/pull/165) was also submitted. Enough complaints, the first results generated via `$ bin/run --benchmark DatasetPerformance` that compare the same workloads expressed in RDD, Dataset and Dataframe APIs are:
```
name                     |minTimeMs| maxTimeMs| avgTimeMs| stdDev 
-------------------------|---------|---------|---------|---------     
DF: average              | 36.53   | 119.91  | 56.69   | 32.31
DF: back-to-back filters | 2080.06 | 2273.10 | 2185.40 | 70.31
DF: back-to-back maps    | 1984.43 | 2142.28 | 2062.64 | 62.38
DF: range                | 1981.36 | 2155.65 | 2056.18 | 70.89 
DS: average              | 59.59   | 378.97  | 126.16  | 125.39
DS: back-to-back filters | 3219.80 | 3482.17 | 3355.01 | 88.43
DS: back-to-back maps    | 2794.68 | 2977.08 | 2890.14 | 59.55
DS: range                | 2000.36 | 3240.98 | 2257.03 | 484.98
RDD: average             | 20.21   | 51.95   | 30.04   | 11.31
RDD: back-to-back filters| 1704.42 | 1848.01 | 1764.94 | 56.92
RDD: back-to-back maps   | 2552.72 | 2745.86 | 2678.29 | 65.86
RDD: range               | 593.73  | 689.74  | 665.13  | 36.92
```

This is rather surprising and counterintuitive given the focus of the architecture changes and performance improvements in Spark 2 -- the RDD API performs best (= lowest numbers in the fourth column) for three out of four workloads, Dataframes only outperform the two other APIs in the _back-to-back maps_ benchmark with 2062 ms versus 2890 ms in the case of Datasets and 2678 in the case of RDDs.

The results for the two other benchmark classes are as follows:

<br>
`bin/run --benchmark AggregationPerformance`
```                    
name                                | minTimeMs | maxTimeMs | avgTimeMs | stdDev
------------------------------------|-----------|-----------|-----------|--------         
aggregation-complex-input-100milints| 19917.71  |23075.68   | 21604.91  | 1590.06
aggregation-complex-input-1bilints  | 227915.47 |228808.97  | 228270.96 | 473.89 
aggregation-complex-input-1milints  | 214.63    |315.07     | 250.08    | 56.35
avg-ints10                          | 213.14    |1818.041   | 763.67    | 913.40 
avg-ints100                         | 254.02    |690.13     | 410.96    | 242.38
avg-ints1000                        | 650.53    |1107.93    | 812.89    | 255.94
avg-ints10000                       | 2514.60   |3273.21    | 2773.66   | 432.72 
avg-ints100000                      | 18975.83  |20793.63   | 20016.33  | 937.04 
avg-ints1000000                     | 233277.99 |240124.78  | 236740.79 | 3424.07
avg-twoGroups100000                 | 218.86    |405.31     | 283.57    | 105.49
avg-twoGroups1000000                | 194.57    |402.21     | 276.33    | 110.62 
avg-twoGroups10000000               | 228.32    |409.40     | 303.74    | 94.25  
avg-twoGroups100000000              | 627.75    |733.01     | 673.69    | 53.88 
avg-twoGroups1000000000             | 4773.60   |5088.17    | 4910.72   | 161.11
avg-twoGroups10000000000            | 43343.70  |47985.57   | 45886.03  | 2352.40
single-aggregate-AVG-100milints     | 20386.24  |21613.05   | 20803.14  | 701.50 
single-aggregate-AVG-1bilints       | 209870.54 |228745.61  | 217777.11 | 9802.98
single-aggregate-AVG-1milints       | 174.15    |353.62     | 241.54    | 97.73 
single-aggregate-COUNT-100milints   | 10832.29  |11932.39   | 11242.52  | 601.00
single-aggregate-COUNT-1bilints     | 94947.80  |103831.10  | 99054.85  | 4479.29
single-aggregate-COUNT-1milints     | 127.51    |243.28     | 166.65    | 66.36
single-aggregate-STDDEV-100milints  | 20829.31  |21207.90   | 20994.51  | 193.84
single-aggregate-STDDEV-1bilints    | 205418.40 |214128.59  | 211163.34 | 4976.13
single-aggregate-STDDEV-1milints    | 181.16    |246.32     | 205.69    | 35.43
single-aggregate-SUM-100milints     | 20191.36  |22045.60   | 21281.71  | 969.26
single-aggregate-SUM-1bilints       | 216648.77 |229335.15  | 221828.33 | 6655.68
single-aggregate-SUM-1milints       | 186.67    |1359.47    | 578.54    | 676.30
```


`bin/run --benchmark JoinPerformance`
```
name                                            |minTimeMs |maxTimeMs |avgTimeMs |stdDev
------------------------------------------------|----------|----------|----------|--------         
singleKey-FULL OUTER JOIN-100milints-100milints | 44081.59 |46575.33  | 45418.33 |1256.54
singleKey-FULL OUTER JOIN-100milints-1milints   | 36832.28 |38027.94  | 37279.31 |652.39
singleKey-FULL OUTER JOIN-1milints-100milints   | 37293.99 |37661.37  | 37444.06 |192.69
singleKey-FULL OUTER JOIN-1milints-1milints     | 936.41   |2509.54   | 1482.18  |890.29
singleKey-JOIN-100milints-100milints            | 41818.86 |42973.88  | 42269.81 |617.71
singleKey-JOIN-100milints-1milints              | 20331.33 |23541.67  | 21692.16 |1660.02
singleKey-JOIN-1milints-100milints              | 22028.82 |23309.41  | 22573.63 |661.30
singleKey-JOIN-1milints-1milints                | 708.12   |2202.12   | 1212.86  |856.78 
singleKey-LEFT JOIN-100milints-100milints       | 43651.79 |46327.19  | 44658.37 |1455.45
singleKey-LEFT JOIN-100milints-1milints         | 22829.34 |24482.67  | 23633.77 |827.56
singleKey-LEFT JOIN-1milints-100milints         | 32674.77 |34286.75  | 33434.05 |810.04
singleKey-LEFT JOIN-1milints-1milints           | 682.51   |773.95    | 715.53   |50.73
singleKey-RIGHT JOIN-100milints-100milints      | 44321.99 |45405.85  | 44965.93 |570.00
singleKey-RIGHT JOIN-100milints-1milints        | 32293.54 |32926.62  | 32554.74 |330.73
singleKey-RIGHT JOIN-1milints-100milints        | 22277.12 |24883.91  | 23551.74 |1304.34
singleKey-RIGHT JOIN-1milints-1milints          | 683.04   |935.88    | 768.62   |144.85
```


## From Phil: Spark & JMH

The surprising results from the _DatasetPerformance_ benchmark above should make us sceptical -- probably the benchmarking code or setup itself is to blame for the odd measurement, not the actual Spark APIs. A popular and quasi-official benchmarking framework for languages targeting the JVM is [JMH](http://openjdk.java.net/projects/code-tools/jmh/) so why not use it for the twelve Spark benchmarks? I "translated" them into JMH versions [here](https://github.com/g1thubhub/jmh-spark) and produced new results, among them the previously odd _DatasetPerformance_ cases:
```
Phils-MacBook-Pro: pwd
/Users/Phil/IdeaProjects/jmh-spark
Phils-MacBook-Pro: ls
README.md benchmarks  build.sbt project   src   target

Phils-MacBook-Pro: sbt benchmarks/jmh:run Bench_APIs1
[...]
Phils-MacBook-Pro: sbt benchmarks/jmh:run Bench_APIs2


Benchmark               (start)  Mode   Cnt  Score     Error    Units
Bench_APIs1.rangeDataframe    1  avgt   20  2618.631 ± 59.210   ms/op
Bench_APIs1.rangeDataset      1  avgt   20  1646.626 ± 33.230   ms/op
Bench_APIs1.rangeDatasetJ     1  avgt   20  2069.763 ± 76.444   ms/op
Bench_APIs1.rangeRDD          1  avgt   20   448.281 ± 85.781   ms/op
Bench_APIs2.averageDataframe  1  avgt   20    24.614 ± 1.201    ms/op
Bench_APIs2.averageDataset    1  avgt   20    41.799 ± 2.012    ms/op
Bench_APIs2.averageRDD        1  avgt   20    12.280 ± 1.532    ms/op
Bench_APIs2.filterDataframe   1  avgt   20  2395.985 ± 36.333   ms/op
Bench_APIs2.filterDataset     1  avgt   20  2669.160 ± 81.043   ms/op
Bench_APIs2.filterRDD         1  avgt   20  2776.382 ± 62.065   ms/op
Bench_APIs2.mapDataframe      1  avgt   20  2020.671 ± 136.371  ms/op
Bench_APIs2.mapDataset        1  avgt   20  5218.872 ± 177.096  ms/op
Bench_APIs2.mapRDD            1  avgt   20  2957.573 ± 26.458   ms/op
```

These results are more in line with expectations: Dataframes perform best in two out of four benchmarks. The Spark-internal functionality used for the other two (_average_ and _range_) indeed favour RDDs: 
<br>
## From IBM: _spark-bench_
To be published

## From CERN:
To be pusblished


* * * 
* * * 

# Enter GraalVM

Most computer programs nowadays are written in higher-level languages so humans can create them faster and understand them easier. But since a machine can only "understand" numerical languages, these high-level artefacts cannot directly be executed by a processor so typically one or more additional steps are required before a program can be run. Some programming languages transform their user's source code into an intermediate representation which then gets compiled again into and interpreted as machine code. The languages of interest in this article (roughly) follow this strategy: The programmer only writes high-level source code which is then automatically transformed to a file ending in _.class_ that contains platform-independent bytecode. This bytecode file is further compiled down to machine code by the Java Virtual Machine while hardware-specific aspects are fully taken care of and, depending on the compiler used, optimizations are applied. Finally this machine code is executed in the JVM runtime.
<br><br>

One of the most ambitious software projects of the past years has probably been the development of a general-purpose virtual machine, Oracle's [Graal](http://www.graalvm.org/) project, "one VM to rule them all." There are several aspects to this technology, two of the highlights include the goal of providing seamless interoperability between (JVM and non-JVM) programming languages while running them efficiently on the same JVM and a new, high performance Java compiler. Twitter already uses the enterprise edition in production and saves around [8% of CPU utilization](https://www.youtube.com/watch?v=MFevkAKVTqU&feature=youtu.be&t=1m29s). The Community edition can be downloaded and used for free, more details below. <br> <br> 


## Graal and Scala
Graal works at the bytecode level. In order to to run Scala code via Graal, I created a toy example that is inspired by the benchmarks described above: The source code snippet below creates 10 million integers, increments each number by one, removes all odd elements and finally sums up all of the remaining even numbers. These four operations are repeated 100 times and during each step the execution time and the sum (which stays the same across all 100 iterations) are printed out. Before the program terminates, the total run time will also be printed. The following source code implements this logic -- not in the most elegant way but with optimization potential for the final compiler phase where Graal will come into play:
{% include ProcessNumbers.html %}
The transformation of this code to the intermediate bytecode representation is done as usual, via ```scalac ProcessNumbers.scala```. The resulting bytecode file is not directly interpretable by humans but those JVM instructions can be made more intelligible by disassembling them with the command `javap -c -cp`. The original source code above has less than 30 lines, the disassembled version has more than 200 lines but in a simpler structure and with a small instruction set: <br><br>
`javap -c -cp ./ ProcessNumbers$`
```Java
public final class ProcessNumbers$ {
  [...]

  public void main(java.lang.String[]);
    Code:
       0: lconst_0
       1: invokestatic  #137                // Method scala/runtime/LongRef.create:(J)Lscala/runtime/LongRef;
       4: astore_2
       5: getstatic     #142                // Field scala/runtime/RichInt$.MODULE$:Lscala/runtime/RichInt$;
       8: getstatic     #35                 // Field scala/Predef$.MODULE$:Lscala/Predef$;
      11: iconst_1
      12: invokevirtual #145                // Method scala/Predef$.intWrapper:(I)I
      15: bipush        100
      17: invokevirtual #149                // Method scala/runtime/RichInt$.to$extension0:(II)Lscala/collection/immutable/Range$Inclusive;
      20: aload_2
      21: invokedynamic #160,  0            // InvokeDynamic #3:apply$mcVI$sp:(Lscala/runtime/LongRef;)Lscala/runtime/java8/JFunction1$mcVI$sp;
      26: invokevirtual #164                // Method scala/collection/immutable/Range$Inclusive.foreach$mVc$sp:(Lscala/Function1;)V
      29: getstatic     #35                 // Field scala/Predef$.MODULE$:Lscala/Predef$;
      32: ldc           #166                // String *********************************
      34: invokevirtual #170                // Method scala/Predef$.println:(Ljava/lang/Object;)V
      37: getstatic     #35                 // Field scala/Predef$.MODULE$:Lscala/Predef$;
      40: new           #172                // class java/lang/StringBuilder
      43: dup
      44: ldc           #173                // int 15
      46: invokespecial #175                // Method java/lang/StringBuilder."<init>":(I)V
      49: ldc           #177                // String Total time:
      51: invokevirtual #181                // Method java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
      54: aload_2
      55: getfield      #185                // Field scala/runtime/LongRef.elem:J
      58: invokevirtual #188                // Method java/lang/StringBuilder.append:(J)Ljava/lang/StringBuilder;
      61: ldc           #190                // String  ms
      63: invokevirtual #181                // Method java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
      66: invokevirtual #194                // Method java/lang/StringBuilder.toString:()Ljava/lang/String;
      69: invokevirtual #170                // Method scala/Predef$.println:(Ljava/lang/Object;)V
      72: return
[...]
}
```
Now we come to the Graal part: My system JDK is 
```bash
Phils-MacBook-Pro $ java -version
java version "1.8.0_171"
Java(TM) SE Runtime Environment (build 1.8.0_171-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.171-b11, mixed mode)
``` 
I downloaded the community edition of Graal from [here](https://github.com/oracle/graal/releases) and placed it in a folder along with Scala libraries and the files for the toy benchmarking example mentioned above:
```bash
Phils-MacBook-Pro: ls
ProcessNumbers$.class	ProcessNumbers.class	ProcessNumbers.scala	graalvm		scalalibs

Phils-MacBook-Pro: ./graalvm/Contents/Home/bin/java -version
openjdk version "1.8.0_172"
OpenJDK Runtime Environment (build 1.8.0_172-20180626105433.graaluser.jdk8u-src-tar-g-b11)
GraalVM 1.0.0-rc5 (build 25.71-b01-internal-jvmci-0.46, mixed mode)

Phils-MacBook-Pro: ls scalalibs/
jline-2.14.6.jar			scala-library.jar			scala-reflect.jar			scala-xml_2.12-1.0.6.jar
scala-compiler.jar			scala-parser-combinators_2.12-1.0.7.jar	scala-swing_2.12-2.0.0.jar		scalap-2.12.6.jar
``` 
Let's run this benchmark with the "normal" JDK via `java -cp ./lib/scala-library.jar:. ProcessNumbers`. Around 31 seconds are needed as can be seen below (only the first and last iterations are shown)
```Bash
Phils-MacBook-Pro: java -cp ./lib/scala-library.jar:. ProcessNumbers
Iteration 1 took 536 milliseconds		25000005000000
Iteration 2 took 533 milliseconds		25000005000000
Iteration 3 took 350 milliseconds		25000005000000
Iteration 4 took 438 milliseconds		25000005000000
Iteration 5 took 345 milliseconds		25000005000000
[...]
Iteration 95 took 293 milliseconds		25000005000000
Iteration 96 took 302 milliseconds		25000005000000
Iteration 97 took 333 milliseconds		25000005000000
Iteration 98 took 282 milliseconds		25000005000000
Iteration 99 took 308 milliseconds		25000005000000
Iteration 100 took 305 milliseconds		25000005000000
*********************************
Total time: 31387 ms
```

And here a run that invokes Graal as JIT compiler:
```Bash
Phils-MacBook-Pro:testo a$ ./graalvm/Contents/Home/bin/java -cp ./lib/scala-library.jar:. ProcessNumbers
Iteration 1 took 1287 milliseconds		25000005000000
Iteration 2 took 264 milliseconds		25000005000000
Iteration 3 took 132 milliseconds		25000005000000
Iteration 4 took 120 milliseconds		25000005000000
Iteration 5 took 128 milliseconds		25000005000000
[...]
Iteration 95 took 111 milliseconds		25000005000000
Iteration 96 took 124 milliseconds		25000005000000
Iteration 97 took 122 milliseconds		25000005000000
Iteration 98 took 123 milliseconds		25000005000000
Iteration 99 took 120 milliseconds		25000005000000
Iteration 100 took 149 milliseconds		25000005000000
*********************************
Total time: 14207 ms
```
14 seconds compared to 31 seconds means a 2x speedup with Graal, not bad. The first iteration takes much longer but then a turbo boost seems to kick in -- most iterations from 10 to 100 take around 100 to 120 ms in the Graal run compared to 290-310 ms in the vanilla Java run. Graal itself has an option to deactivate the optimization via the `-XX:-UseJVMCICompiler` flag, trying that results in similar numbers compared with the first run:
```Bash
Phils-MacBook-Pro: /Users/a/graalvm/Contents/Home/bin/java -XX:-UseJVMCICompiler -cp ./lib/scala-library.jar:. ProcessNumbers
Iteration 1 took 566 milliseconds		25000005000000
Iteration 2 took 508 milliseconds		25000005000000
Iteration 3 took 376 milliseconds		25000005000000
Iteration 4 took 456 milliseconds		25000005000000
Iteration 5 took 310 milliseconds		25000005000000
[...]
Iteration 95 took 301 milliseconds		25000005000000
Iteration 96 took 301 milliseconds		25000005000000
Iteration 97 took 285 milliseconds		25000005000000
Iteration 98 took 302 milliseconds		25000005000000
Iteration 99 took 296 milliseconds		25000005000000
Iteration 100 took 296 milliseconds		25000005000000
*********************************
Total time: 30878 ms
```
## Graal and Spark
Why not invoke Graak for Spark jobs. Let's do this for my benchmarking project introduced above with the `-jvm` flag:
```bash
Phils-MacBook-Pro:jmh-spark $ sbt 
 Loading settings for project jmh-spark-build from plugins.sbt ...
 Loading project definition from /Users/Phil/IdeaProjects/jmh-spark/project
 Loading settings for project jmh-spark from build.sbt ...
 Set current project to jmh-spark (in build file:/Users/Phil/IdeaProjects/jmh-spark/)
 sbt server started at local:///Users/Phil/.sbt/1.0/server/c980c60cda221235db06/sock

sbt:jmh-spark> benchmarks/jmh:run -jvm /Users/Phil/testo/graalvm/Contents/Home/bin/java
 Running (fork) spark_benchmarks.MyRunner -jvm /Users/Phil/testo/graalvm/Contents/Home/bin/java
```
