# JOINs with Apache Spark

The plan is to have in this folder Jupyter notebooks with samples on howto
implement joins in Apache Spark.

Until that is implemented, some key samples are stored here:

* Join two tables with duplicated names in columns (use aliases to avoid having columns with the same name):

```
val employees = sc.parallelize(Array[(String, Option[Int])](
  ("Rafferty", Some(31)), ("Jones", Some(33)), ("Heisenberg", Some(33)), ("Robinson", Some(34)), ("Smith", Some(34)), ("Williams", null)
)).toDF("Name", "DepartmentID")

val departments = sc.parallelize(Array(
  (31, "Sales"), (33, "Engineering"), (34, "Clerical"),
  (35, "Marketing")
)).toDF("DepartmentID", "Name")

employees.join(departments, "DepartmentID").select(employees("DepartmentID"), employees("Name").alias("ename"), departments("Name").alias("dname")).show()
+------------+----------+-----------+
|DepartmentID|     ename|      dname|
+------------+----------+-----------+
|          31|  Rafferty|      Sales|
|          34|  Robinson|   Clerical|
|          34|     Smith|   Clerical|
|          33|     Jones|Engineering|
|          33|Heisenberg|Engineering|
+------------+----------+-----------+
```

## Resources

* [Beyond traditional join with Apache Spark](http://kirillpavlov.com/blog/2016/04/23/beyond-traditional-join-with-apache-spark/)
