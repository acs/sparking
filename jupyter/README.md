# Using Jupyter with Spark

## Configure Spark inside Jupyter

Create a python venv and install jupyter in it:

```
python3 -m venv venv
source venv/bin/activate
pip install notebook
```

Install the package `findspark`:

```
source venv/bin/active
pip install findspark
```

Start jupyter with:

```
SPARK_HOME=~/devel/spark/spark-2.3.2-bin-hadoop2.7 jupyter notebook
```

changing `/devel/spark/spark-2.3.2-bin-hadoop2.7` with the folder with your Spark installation.

and at the top of your notebook:

```
import findspark
findspark.init()
```

To use plots matplotlib is also needed:

```
pip install matplotlib
```


## Playing with London Crime dataset

The dataset can be downloaded from:

https://www.kaggle.com/jboysen/london-crime

The notebook is in [london.ipynb](london.ipynb).