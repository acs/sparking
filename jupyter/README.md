# Using Jupyter with SPARK

In order to start a Jupyter server with `pyspark` just add to your .bash_profile:

```
export SPARK_HOME="/home/acs/devel/spark/spark-2.3.2-bin-hadoop2.7"
export PATH="$SPARK_HOME/bin:$PATH"

export PYSPARK_SUBMIT_ARGS="pyspark-shell"
export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS='notebook' pyspark
```

and then goto http://localhost:8080 and start using Spark inside Jupyter. The normal
approach is to start with the loading of a CSV file to play with:

```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("London Crime").getOrCreate()
data = spark.read.format("csv").option("header", "true").load("../datasets/london_crime_by_lsoa.csv")
data.printSchema()
data.count()
data.limit(5).show()
....

```
