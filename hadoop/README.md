# Installing Hadoop

Hadoop will be installed and configured in distributed mode but
in one node.

* Download it: https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.8.5/hadoop-2.8.5.tar.gz
* Install it: 
```
acs@~/devel/hadoop $ tar xfz hadoop-2.8.5.tar.gz
```

## Configure process

* Configure ssh
```
acs@~/devel/hadoop $ ssh localhost
....
Last login: Mon Oct 29 05:23:51 2018 from ::1
acs@debian:~$
``` 

* Configure hadoop, hdfs and yarn

```
acs@~/devel/hadoop $ vi hadoop-2.8.5/etc/hadoop/hadoop-env.sh 
export JAVA_HOME=/usr
export HADOOP_PREFIX=/home/acs/devel/hadoop/hadoop-2.8.5
export HADOOP_MAPRED_HOME=/home/acs/devel/hadoop/hadoop-2.8.5
```

```
acs@~/devel/hadoop $ vi hadoop-2.8.5/etc/hadoop/core-site.xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

```
acs@~/devel/hadoop $ cp hadoop-2.8.5/etc/hadoop/mapred-site.xml.template hadoop-2.8.5/etc/hadoop/mapred-site.xml
acs@~/devel/hadoop $ vi hadoop-2.8.5/etc/hadoop/mapred-site.xml
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
```


```
acs@~/devel/hadoop $ vi hadoop-2.8.5/etc/hadoop/yarn-site.xml
...
 <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  ...
```

```
acs@~/devel/hadoop $ vi hadoop-2.8.5/etc/hadoop/hdfs-site.xml
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
```

## Start HDFS service

Format the HDFS filesystem:

```
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hdfs namenode -format
...
18/10/30 03:47:09 INFO common.Storage: Storage directory /tmp/hadoop-acs/dfs/name has been successfully formatted.
...
acs@~/devel/hadoop/hadoop-2.8.5 $ tree /tmp/hadoop-acs/dfs/
/tmp/hadoop-acs/dfs/
└── name
    └── current
        ├── fsimage_0000000000000000000
        ├── fsimage_0000000000000000000.md5
        ├── seen_txid
        └── VERSION

2 directories, 4 files
```

Start master and slave nodes:

```
acs@~/devel/hadoop/hadoop-2.8.5 $ sbin/start-dfs.sh 
Starting namenodes on [localhost]
localhost: starting namenode, logging to /home/acs/devel/hadoop/hadoop-2.8.5/logs/hadoop-acs-namenode-debian.out
localhost: starting datanode, logging to /home/acs/devel/hadoop/hadoop-2.8.5/logs/hadoop-acs-datanode-debian.out
Starting secondary namenodes [0.0.0.0]
0.0.0.0: starting secondarynamenode, logging to /home/acs/devel/hadoop/hadoop-2.8.5/logs/hadoop-acs-secondarynamenode-debian.out
acs@~/devel/hadoop/hadoop-2.8.5 $ jps
...
17111 SecondaryNameNode
...
16749 DataNode
16431 NameNode
```

HDFS NameNode web interface: http://localhost:50070/

Let's create some contents:

```
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hdfs dfs -mkdir /user
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hdfs dfs -mkdir /user/acs
```

All the commands available for HDFS are documented in:

```
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hdfs dfs -help
```

If the filesystem is not HDFS you can use:

```
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hadoop fs -help
```


## Start Yarn service

```
acs@~/devel/hadoop/hadoop-2.8.5 $ sbin/start-yarn.sh 
starting yarn daemons
starting resourcemanager, logging to /home/acs/devel/hadoop/hadoop-2.8.5/logs/yarn-acs-resourcemanager-debian.out
localhost: starting nodemanager, logging to /home/acs/devel/hadoop/hadoop-2.8.5/logs/yarn-acs-nodemanager-debian.out
acs@~/devel/hadoop/hadoop-2.8.5 $ jps | grep Man
18433 NodeManager
17976 ResourceManager
```

Yarn resource manager: http://localhost:8088

## Send MapReduce job to Hadoop

```
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hdfs dfs -mkdir input
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hdfs dfs -put etc/hadoop/* input/
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hdfs dfs -ls input/
Found 30 items
-rw-r--r--   1 acs supergroup       4942 2018-10-30 04:11 input/capacity-scheduler.xml
-rw-r--r--   1 acs supergroup       1335 2018-10-30 04:11 input/configuration.xsl
-rw-r--r--   1 acs supergroup        318 2018-10-30 04:11 input/container-executor.cfg
-rw-r--r--   1 acs supergroup        872 2018-10-30 04:11 input/core-site.xml
...
```

```
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.5.jar grep input output 'dfs[a-z.]+'
18/10/30 04:14:40 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
18/10/30 04:14:41 INFO input.FileInputFormat: Total input files to process : 30
18/10/30 04:14:41 INFO mapreduce.JobSubmitter: number of splits:30
18/10/30 04:14:41 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1540868707259_0001
18/10/30 04:14:41 INFO impl.YarnClientImpl: Submitted application application_1540868707259_0001
18/10/30 04:14:41 INFO mapreduce.Job: The url to track the job: http://debian:8088/proxy/application_1540868707259_0001/
18/10/30 04:14:41 INFO mapreduce.Job: Running job: job_1540868707259_0001
18/10/30 04:14:48 INFO mapreduce.Job: Job job_1540868707259_0001 running in uber mode : false
18/10/30 04:14:48 INFO mapreduce.Job:  map 0% reduce 0%
18/10/30 04:14:55 INFO mapreduce.Job:  map 20% reduce 0%
18/10/30 04:15:00 INFO mapreduce.Job:  map 30% reduce 0%
18/10/30 04:15:02 INFO mapreduce.Job:  map 40% reduce 0%
18/10/30 04:15:08 INFO mapreduce.Job:  map 47% reduce 0%
18/10/30 04:15:09 INFO mapreduce.Job:  map 57% reduce 0%
18/10/30 04:15:12 INFO mapreduce.Job:  map 63% reduce 0%
18/10/30 04:15:14 INFO mapreduce.Job:  map 73% reduce 0%
18/10/30 04:15:18 INFO mapreduce.Job:  map 80% reduce 0%
18/10/30 04:15:19 INFO mapreduce.Job:  map 90% reduce 27%
18/10/30 04:15:21 INFO mapreduce.Job:  map 97% reduce 27%
18/10/30 04:15:22 INFO mapreduce.Job:  map 100% reduce 27%
18/10/30 04:15:23 INFO mapreduce.Job:  map 100% reduce 100%
18/10/30 04:15:24 INFO mapreduce.Job: Job job_1540868707259_0001 completed successfully
18/10/30 04:15:24 INFO mapreduce.Job: Counters: 49
....
acs@~/devel/hadoop/hadoop-2.8.5 $ bin/hdfs dfs -cat output/*
6	dfs.audit.logger
4	dfs.class
3	dfs.logger
3	dfs.server.namenode.
2	dfs.audit.log.maxbackupindex
2	dfs.period
2	dfs.audit.log.maxfilesize
1	dfs.log
1	dfs.file
1	dfs.servers
1	dfsadmin
1	dfsmetrics.log
1	dfs.replication
```



