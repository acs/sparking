# WordCount with Apache Spark

In this sample, the words of a file are counted using Apache Spark.

To run this sample:

```
$ rm -rf /tmp/sparking-wordcount
$ sbt compile
$ sbt run
```

You can check in `/tmp/sparking-wordcount` folder the words ordered by frequency of the README.md file.

To generate the jar file for this sample:

```
sbt package
```

This can be submitted to a running Spark with:

```
spark-submit --class "WordCount" --master "local[*]" target/scala-2.11/wordcount_2.11-0.1.jar
```

More details will come.
