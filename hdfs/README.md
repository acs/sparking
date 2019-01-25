# Sparking with HDFS

In this folder there will be samples on howto configure a HDFS cluster and use it from Spark.

* [Apache Hadoop - Setting up a local Test Environment](https://devops.datenkollektiv.de/apache-hadoop-setting-up-a-local-test-environment.html)

After this deployment it just pending to connect Spark to this network cluster to start working with Spark reading from HDFS.

# Reading files from HDFS using Scala/Spark

```
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

val conf = spark.sparkContext.hadoopConfiguration
conf.addResource(new Path("/opt/hadoop-2.7.2/etc/hadoop/core-site.xml"))
val fs = FileSystem.get(conf)
val dirPath = new Path("/")
val filestatus = fs.listStatus(dirPath )
filestatus.foreach(f => println(f.getModificationTime))
```


## Other resources

* [Hadoop Docker from Big Data Europe](https://github.com/big-data-europe/docker-hadoop)

