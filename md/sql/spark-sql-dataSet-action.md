# Spark 2.4.0编程指南--spark dataSet action

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0

## 文档
- (官网文档): http://spark.apache.org/docs/2.4.0/sql-getting-started.html

## 前置条件
- 已安装好java(选用的是java 1.8.0_191)
- 已安装好scala(选用的是scala  2.11.121)
- 已安装好hadoop(选用的是Hadoop 3.1.1)
- 已安装好spark(选用的是spark 2.4.0)

## 技能标签
- Spark session 创建
- 在Spark 2.0之后，RDD被数据集(Dataset)取代 ，保留RDD旧api
- 数据集数据集介绍
- 读取本地文件(txt,json),HDFS文件
- 对txt格式文件数据遍历(行数据转成对象)
- 对json格式文件数据遍历(直接转对象)
- 数据集的action操作
- collect,collectAsList,count,describe,first,foreach,head,reduce,show,take,takeAsList,toLocalIterator
- 官网: http://spark.apache.org/docs/2.4.0/sql-getting-started.html

## DataSet(数据集)

```
数据集是分布式数据集合。数据集是Spark 1.6中添加的一个新接口，它提供了RDD的优势（强类型，使用强大的lambda函数的能力）以及Spark SQL优化执行引擎的优点。数据集可以从JVM对象构造，然后使用功能转换（map，flatMap，filter等）进行操作。数据集API在Scala和Java中可用。 Python没有对Dataset API的支持。但由于Python的动态特性，数据集API的许多好处已经可用（即您可以通过名称自然地访问行的字段row.columnName）。 R的情况类似。
```
### BaseSparkSession
- 公用得到SparkSession的方法

```
def sparkSession(isLocal:Boolean = false): SparkSession = {

    if(isLocal){
      master = "local"
      val spark = SparkSession.builder
        .master(master)
        .appName(appName)
        .getOrCreate()
      //spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
      //import spark.implicits._
      spark
    }else{
      val spark = SparkSession.builder
        .master(master)
        .appName(appName)
        .config("spark.eventLog.enabled","true")
        .config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
        .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")
        .getOrCreate()
     // spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
      //import spark.implicits._
      spark
    }

  }
```
### textFile
- 读取本地文件

```
    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
    df.show()

//    +-----------+
//    |      value|
//    +-----------+
//    |Michael, 29|
//    |   Andy, 30|
//    | Justin, 19|
//    |  Think, 30|
//    +-----------+
```

### textFile
- 读取HDFS文件

```
    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.textFile("hdfs://standalone.com:9000/home/liuwen/data/people.txt")
    df.show()


//    +-----------+
//    |      value|
//    +-----------+
//    |Michael, 29|
//    |   Andy, 30|
//    | Justin, 19|
//    |  Think, 30|
//    +-----------+

    spark.stop()
```

### text
- 读取本地文件
```
   val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.text("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
    df.show()

//    +-----------+
//    |      value|
//    +-----------+
//    |Michael, 29|
//    |   Andy, 30|
//    | Justin, 19|
//    |  Think, 30|
//    +-----------+

```

### text
- 读取HDFS数据
```
object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.text("hdfs://standalone.com:9000/home/liuwen/data/people.txt")
    df.show()

//    +-----------+
//    |      value|
//    +-----------+
//    |Michael, 29|
//    |   Andy, 30|
//    | Justin, 19|
//    |  Think, 30|
//    +-----------+

    spark.stop()
  }

}

```

### foreach 遍历文件内容
- 对象遍历

```

object Run1 extends BaseSparkSession{

  case class Person(name: String, age: Long)


  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    import spark.implicits._
    spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
      .map(line => Person(line.split(",")(0),line.split(" ")(1).trim.toLong))
        .foreach( person => println(s"name:${person.name}\t age:${person.age}"))

    spark.stop()

  }
}

```

### first
- 得到dataSet的第一个元素

```
    val spark = sparkSession()
    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")

    println(dataSet.first()) //first里边调用的是head()
    spark.stop()
```

### head
- 得到dataSet的第一个元素
```
    val spark = sparkSession()
    val dataSet = spark.read.textFile("/home/liuwen/data/a.text")
    println(dataSet.head()) //first里边调用的是head()
```
### head n
- 得到dataSet的前n个元素
```
    val spark = sparkSession()
    val dataSet = spark.read.textFile("/home/liuwen/data/a.text")
    println(dataSet.head(5)) //first里边调用的是head()
```

### count
- 得到dataSet 一共有多少行数据
```
    val spark = sparkSession()
    val dataSet = spark.read.textFile("/home/liuwen/data/a.text")
    println(dataSet.count())

```

### collect 
- 收集dataSet中所有行的数据，在本地输出
```
    val spark = sparkSession()
    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")
    println(dataSet.collect().mkString("\n"))
```

### collectAsList
- 收集dataSet中所有的数据，转成java.util.List对象

```
    val spark = sparkSession(true)

    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")
    println( dataSet.collectAsList())
    import scala.collection.JavaConversions._
    for( v <- dataSet.collectAsList()) println(v)
    spark.stop()
```

### foreache
- 遍历dataSet中的每一行数据

```
   val spark = sparkSession(true)
    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")
    dataSet.foreach(println(_))
```

### foreache class
- 以对象形式遍历dataSet中所有的数据
```
object Run1 extends BaseSparkSession{

  case class Person(name: String, age: Long)


  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    import spark.implicits._
    spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
      .map(line => Person(line.split(",")(0),line.split(" ")(1).trim.toLong))
        .foreach( person => println(s"name:${person.name}\t age:${person.age}"))

    spark.stop()


  }
}

```

### map
- 遍历数据集中的每一个元素，进行map函数操作

```
    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/a.text")
    import spark.implicits._
    val lineWordLength = dataSet.map( line => line.split(" ").size)

    println(lineWordLength.collect().mkString("\n"))
```


### reduce
- 遍历dataSet中的元素，每两两进行reduce函数操作
```
    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.text")

    /**
      * 统计所有行单词个数
      */
    import spark.implicits._
    val lineWordLength = dataSet.map( line => line.split(" ").size)
    val result = lineWordLength.reduce((a,b) => a + b)

    println(result)
```

### show
- 以表格形式显示dataSet数据，默认显示前20行数据
```
   val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.cn.text")

 
    val result = dataSet.show()
    println(result)
```

### show n
- 以表格形式显示dataSet数据，默认显示前20行数据
```
   val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.cn.text")

    /**
      * 以表格的形式显示前3行数据
      * numRows是显示前几行的数据
      */

    val result = dataSet.show(3)
    println(result)
```

### show truncate
- 以表格形式显示dataSet数据，默认显示前20行数据
- 参数truncate=false，是不截断显示所有数据，true是进截断

```

    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.text")

    /**
      * 以表格的形式显示前3行数据
      * numRows是显示前几行的数据
      * false 不进行返回行数据截断
      */

    val result = dataSet.show(10,false)
    println(result)
```

### take
- take 是相当于head

```
    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.txt")
    val result = dataSet.take(10) //等于head(n)
    println(result.mkString("\n"))
```

### describe

```
 val spark = sparkSession()

    val dataSet = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/json/people.json")

    dataSet.describe("name","age").show()

//    +-------+-------+------------------+
//    |summary|   name|               age|
//    +-------+-------+------------------+
//    |  count|      3|                 2|
//    |   mean|   null|              24.5|
//    | stddev|   null|7.7781745930520225|
//    |    min|   Andy|                19|
//    |    max|Michael|                30|
//    +-------+-------+------------------+
```



end