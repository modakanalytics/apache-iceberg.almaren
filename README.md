# Apache-Iceberg Connector

[![Build Status](https://travis-ci.com/modakanalytics/Iceberg.almaren.svg?token=TEB3zRDqVUuChez9334q&branch=master)](https://travis-ci.com/modakanalytics/iceberg.almaren)

Apache-Iceberg Connector allow you to execute any SQL statement using Apache Spark.

```
libraryDependencies += "com.github.music-of-the-ainur" %% "iceberg-almaren" % "0.0.1-2.4"
```

```
spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.9.0-2.4,com.github.music-of-the-ainur:iceberg-almaren_2.11:0.0.1-2.4"
```
## Iceberg Batch

### Example

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.iceberg.Iceberg.IcebergImplicit

import spark.implicits._

 val almaren = Almaren("Iceberg-almaren")

 val updateSourceDf = Seq(
    ("John", "Jones"),
    ("David", "Smith"),
    ("Michael", "Lee"),
    ("Chris", "Johnson"),
    ("Mike", "Brown")
  ).toDF("first_name", "last_name")

```

### Parameters



## Iceberg Query 

### Example 

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.iceberg.Iceberg.IcebergImplicit

import spark.implicits._

 val almaren = Almaren("Iceberg-almaren")

```
### Parameters