# Spark 2.4.0编程指南--Spark DataSources


## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0
## 视频
- Spark 2.4.0编程指南--Spark DataSources(bilibili视频): https://www.bilibili.com/video/av38193405/?p=5

<iframe width="800" height="500" src="//player.bilibili.com/player.html?aid=38193405&cid=68636905&page=5" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


## 前置条件
- 已安装好java(选用的是java 1.8.0_191)
- 已安装好scala(选用的是scala  2.11.121)
- 已安装好hadoop(选用的是Hadoop 2.9.2)
- 已安装好hive(选用的是apache-hive-3.1.1-bin)
- 已安装好spark(选用的是spark-2.4.0-bin-hadoop2.7)

## 技能标签
- parquet、orc、csv、json、text、avro格式文件的读、写
- spark.sql直接运行文件
- BucketyBy,PartitionBy 读写文件
- mergining dataSet
- jdbc(mysql)读写操作
- Hive操作(create drop database ,create,insert,show,truncate,drop table)
- 官网: http://spark.apache.org/docs/2.4.0/sql-data-sources.html


## 常规 Load/Save (parquet)

### 读取parquest格式文件
- 读取parquet格式文件users.parquet
- users.parquet 直接打开是十六进制数据

```
spark.read.load("hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet").show

//+------+--------------+----------------+
//|  name|favorite_color|favorite_numbers|
//+------+--------------+----------------+
//|Alyssa|          null|  [3, 9, 15, 20]|
//|   Ben|           red|              []|
//+------+--------------+----------------+
```


### 保存parquest格式文件
- 读取parquet格式文件users.parquet
- users.parquet 直接打开是十六进制数据
- 保存的数据会在这个目录下namesAndFavColors.parquet


```
val usersDF = spark.read.load("hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet")
usersDF.select("name", "favorite_color").write.save("hdfs://standalone.com:9000/home/liuwen/data/parquest/namesAndFavColors.parquet")

spark.read.load("hdfs://m0:9000/home/liuwen/data/parquest/namesAndFavColors.parquet").show


//+------+--------------+
//|  name|favorite_color|
//+------+--------------+
//|Alyssa|          null|
//|   Ben|           red|
//+------+--------------+

```


##  Load/Save (json)

### 读取json格式文件
- 读取json格式文件people.json
- people.json存储的是json格式的数据
- 注意，json格式存储的文件，每行中都包含字段名称信息，比较占空间，不推荐使用,可以考虑用默认的 parquet格式存储

```
spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/data/json/people.json").show

//+----+-------+
//| age|   name|
//+----+-------+
//|null|Michael|
//|  30|   Andy|
//|  19| Justin|
//+----+-------+

```

### 保存json格式文件
- 读取json格式文件people.json
- people.json存储的是json格式的数据


```
spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/data/json/people.json").show

//+----+-------+
//| age|   name|
//+----+-------+
//|null|Michael|
//|  30|   Andy|
//|  19| Justin|
//+----+-------+


//保存json格式数据到hdfs上面
ds.select("name", "age").write.format("json").save("hdfs://standalone.com:9000/home/liuwen/output/json/namesAndAges.json")


//读取保存的数据
 spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/output/json/namesAndAges.json").show
 
//+----+-------+
//| age|   name|
//+----+-------+
//|null|Michael|
//|  30|   Andy|
//|  19| Justin|
//+----+-------+

```
- HDFS查看保存的文件信息

```
 hdfs dfs -ls -R hdfs://standalone.com:9000/home/liuwen/output/json
 
// drwxr-xr-x   - liuwen supergroup          0 2018-12-18 17:44 //hdfs://standalone.com:9000/home/liuwen/output/json/namesAndAges.json
//-rw-r--r--   1 liuwen supergroup          0 2018-12-18 17:44 //hdfs://standalone.com:9000/home/liuwen/output/json/namesAndAges.json/_SUCCESS
//-rw-r--r--   1 liuwen supergroup         71 2018-12-18 17:44 //hdfs://standalone.com:9000/home/liuwen/output/json/namesAndAges.json/part-00000-6690fee8-33d3-413c-8364-927f02593ff2-c000.json


 hdfs dfs -cat hdfs://standalone.com:9000/home/liuwen/output/json/namesAndAges.json/*
//数据在文件 namesAndAges.json/part-00000-6690fee8-33d3-413c-8364-927f02593ff2-c000.json
//{"name":"Michael"}
//{"name":"Andy","age":30}
//{"name":"Justin","age":19}
//[liuwen@standalone ~]$ 
```

##  Load/Save (csv)

### 读取csv格式文件

```
 val peopleDFCsv = spark.read.format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("hdfs://m0:9000/home/liuwen/data/csv/people.csv")
 //peopleDFCsv: org.apache.spark.sql.DataFrame = [name: string, age: int ... 1 more field]
 peopleDFCsv.show
 
// +-----+---+---------+
//| name|age|      job|
//+-----+---+---------+
//|Jorge| 30|Developer|
//|  Bob| 32|Developer|
//+-----+---+---------+


```
### 写csv格式文件
```
//保存json格式数据到hdfs上面
peopleDFCsv.select("name", "age").write.format("csv").save("hdfs://standalone.com:9000/home/liuwen/output/csv/people.csv")

spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs://standalone.com:9000//home/liuwen/output/csv/people.csv").show

//+-----+---+
//|Jorge| 30|
//+-----+---+
//|  Bob| 32|
//+-----+---+
```

- 查看hdfs上csv文件
```
hdfs dfs -ls -R   hdfs://m0:9000/home/liuwen/output/csv/

//drwxr-xr-x   - liuwen supergroup          0 2018-12-18 18:04 hdfs://m0:9000/home/liuwen/output/csv/people.csv
//-rw-r--r--   1 liuwen supergroup          0 2018-12-18 18:04 hdfs://m0:9000/home/liuwen/output/csv/people.csv/_SUCCESS
//-rw-r--r--   1 liuwen supergroup         16 2018-12-18 18:04 hdfs://m0:9000/home/liuwen/output/csv/people.csv/part-00000-d6ad5563-5908-4c0e-8e6f-f13cd0ff445e-c000.csv

hdfs dfs -text hdfs://m0:9000/home/liuwen/output/csv/people.csv/part-00000-d6ad5563-5908-4c0e-8e6f-f13cd0ff445e-c000.csv

//Jorge,30
//Bob,32

```

##  Load/Save (orc)

### 写orc格式文件

```
val usersDF = spark.read.load("hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet")
usersDF.show
//+------+--------------+----------------+
//|  name|favorite_color|favorite_numbers|
//+------+--------------+----------------+
//|Alyssa|          null|  [3, 9, 15, 20]|
//|   Ben|           red|              []|
//+------+--------------+----------------+

usersDF.write.format("orc").option("orc.bloom.filter.columns", "favorite_color").option("orc.dictionary.key.threshold", "1.0").save("hdfs://standalone.com:9000/home/liuwen/output/orc/users_with_options.orc")


```
### 读orc格式文件

```

 spark.read.format("orc").load("hdfs://standalone.com:9000/home/liuwen/output/orc/users_with_options.orc").show
 
//+------+--------------+----------------+
//|  name|favorite_color|favorite_numbers|
//+------+--------------+----------------+
//|Alyssa|          null|  [3, 9, 15, 20]|
//|   Ben|           red|              []|
//+------+--------------+----------------+

```

### 直接在文件上运行sql
- 直接在文件上运行sql
```
val sqlDF = spark.sql("SELECT * FROM parquet.`hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet`")

sqlDF.show
//+------+--------------+----------------+
//|  name|favorite_color|favorite_numbers|
//+------+--------------+----------------+
//|Alyssa|          null|  [3, 9, 15, 20]|
//|   Ben|           red|              []|
//+------+--------------+----------------+
```


### saveAsTable
- 把数据保存为Hive表
```
val sqlDF = spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/output/json/employ.json")
    sqlDF.show
    //+----+-------+
    //| age|   name|
    //+----+-------+
    //|null|Michael|
    //|  30|   Andy|
    //|  19| Justin|

    sqlDF.write.saveAsTable("people_bucketed")

    val sqlDF2 = spark.sql("select * from people_bucketed")

```
- 读取hive表中的数据
```
 val sqlDF = spark.sql("select * from people_bucketed")

```

### bucket
- 把数据保存为Hive表,bucketBy 分桶
```
val sqlDF = spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/output/json/employ.json")
    sqlDF.show
    //+----+-------+
    //| age|   name|
    //+----+-------+
    //|null|Michael|
    //|  30|   Andy|
    //|  19| Justin|

    sqlDF.write.bucketBy(42, "name").sortBy("salary")
      .saveAsTable("people_bucketed3")

    val sqlDF2 = spark.sql("select * from people_bucketed3")
    sqlDF2.show

```
- 读取hive表中的数据
```
 val sqlDF = spark.sql("select * from people_bucketed3")

```


### partitionBy
- 把数据保存为Hive表,partitionBy 按字段分区
```
  val spark = sparkSession(true)
    val usersDF = spark.read.load("hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet")

    usersDF.show()
    //+------+--------------+----------------+
    //|  name|favorite_color|favorite_numbers|
    //+------+--------------+----------------+
    //|Alyssa|          null|  [3, 9, 15, 20]|
    //|   Ben|           red|              []|
    //+------+--------------+----------------+


    //保存在HDFS上 hdfs://standalone.com:9000/user/liuwen/namesPartByColor.parquet
    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

```
- 读取hive表中的数据
```
 val sqlDF = spark.sql("select * from namesPartByColor.parquet")

```


### dataFrame的合并
- 把两个dataSet合并，就是把两个dataSet先保存到hdfs的文件上，这两个dataSet的文件在同一个目录上，再读这个目录下的文件
```

    import spark.implicits._
    val df1 = Seq(1,2,3,5).map(x => (x,x * x)).toDF("a","b")
    val df2 = Seq(10,20,30,50).map(x => (x,x * x)).toDF("a","b")

    df1.write.parquet("data/test_table/key=1")
    df1.show()
//    +---+---+
//    |  a|  b|
//    +---+---+
//    |  1|  1|
//    |  2|  4|
//    |  3|  9|
//    |  5| 25|
//    +---+---+
    df2.write.parquet("data/test_table/key=2")
    df2.show()
//    +---+----+
//    |  a|   b|
//    +---+----+
//    | 10| 100|
//    | 20| 400|
//    | 30| 900|
//    | 50|2500|
//    +---+----+

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()
//    root
//    |-- a: integer (nullable = true)
//    |-- b: integer (nullable = true)
//    |-- key: integer (nullable = true)


    mergedDF.show()

//    +---+----+---+
//    |  a|   b|key|
//    +---+----+---+
//    | 10| 100|  2|
//    | 20| 400|  2|
//    | 30| 900|  2|
//    | 50|2500|  2|
//    |  1|   1|  1|
//    |  2|   4|  1|
//    |  3|   9|  1|
//    |  5|  25|  1|
//    +---+----+---+

```

### mysql(jdbc)
- 读mysql的数据
```
val connectionProperties = new Properties()
    connectionProperties.put("user","admin")
    connectionProperties.put("password","000000")

    val jdbcDF = spark.read.jdbc("jdbc:mysql://mysql.com:3306/test","test.test2",connectionProperties)

    jdbcDF.show()

```

- 往mysql写数据
```

    val connectionProperties = new Properties()
    connectionProperties.put("user","admin")
    connectionProperties.put("password","000000")

    val jdbcDF = spark.read.jdbc("jdbc:mysql://macbookmysql.com:3306/test","test.test",connectionProperties)

    jdbcDF.show()


    jdbcDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://macbookmysql.com:3306/test","test.test3",connectionProperties)

```


### spark hive
- 就是支持hive的语法，只不过是在spark中执行，把hive的数据转成dataFrame，供spark算子计算

```

     val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
     // .master("spark://standalone.com:7077")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("CREATE database IF NOT EXISTS test_tmp")
    sql("use test_tmp")
    sql("CREATE TABLE IF NOT EXISTS student(name VARCHAR(64), age INT)")
    sql("INSERT INTO TABLE student  VALUES ('小王', 35), ('小李', 50)")

```

end