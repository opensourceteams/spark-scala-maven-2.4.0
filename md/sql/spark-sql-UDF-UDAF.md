# Spark 2.4.0编程指南--Spark SQL UDF和UDAF


## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0

## 视频
- Spark 2.4.0编程指南--Spark SQL UDF和UDAF(bilibili视频) : https://www.bilibili.com/video/av38193405/?p=4

<iframe width="800" height="500" src="//player.bilibili.com/player.html?aid=38193405&cid=67137841&page=4" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


## 文档
- (官网文档): http://spark.apache.org/docs/2.4.0/sql-getting-started.html#aggregations

## 前置条件
- 已安装好java(选用的是java 1.8.0_191)
- 已安装好scala(选用的是scala  2.11.121)
- 已安装好hadoop(选用的是Hadoop 3.1.1)
- 已安装好spark(选用的是spark 2.4.0)

## 技能标签
- 了解UDF 用户定义函数（User-defined functions, UDFs）
- 了解UDAF （user-defined aggregate function), 用户定义的聚合函数
- UDF示例(统计行数据字符长度)
- UDF示例(统计行数据字符转大写)
- UDAF示例(统计总行数)
- UDAF示例(统计最大收入)
- UDAF示例(统计平均收入)
- UDAF示例(统计按性别分组的最大收入)
- 官网: http://spark.apache.org/docs/2.4.0/sql-getting-started.html#aggregations

## UDF
用户定义函数（User-defined functions, UDFs）是大多数 SQL 环境的关键特性，用于扩展系统的内置功能。 UDF允许开发人员通过抽象其低级语言实现来在更高级语言（如SQL）中启用新功能。 Apache Spark 也不例外，并且提供了用于将 UDF 与 Spark SQL工作流集成的各种选项。

- 用户定义函数（User-defined functions, UDFs）
- UDF对表中的单行进行转换，以便为每行生成单个对应的输出值


##示例
- 得到SparkSession
 
### BaseSparkSession
```
/**
  * 得到SparkSession
  * 首先 extends BaseSparkSession
  * 本地: val spark = sparkSession(true)
  * 集群:  val spark = sparkSession()
  */
class BaseSparkSession {

  var appName = "sparkSession"
  var master = "spark://standalone.com:7077" //本地模式:local     standalone:spark://master:7077


  def sparkSession(): SparkSession = {
    val spark = SparkSession.builder
      .master(master)
      .appName(appName)
      .config("spark.eventLog.enabled","true")
      .config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
      .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")
      .getOrCreate()
    spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
    //import spark.implicits._
    spark
  }


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


  /**
    * 得到当前工程的路径
    * @return
    */
  def getProjectPath:String=System.getProperty("user.dir")
}

```
### UDF (统计字段长度)
- 对数据集中，每行数据的特定字段，计算字符长度
- 通过 spark.sql 直接在字段查询处调用函数名称

```

/**
  * 自定义匿名函数
  * 功能: 得到某列数据长度的函数
  */
object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    val ds = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")

    ds.show()

//    +-------+------+
//    |   name|salary|
//    +-------+------+
//    |Michael|  3000|
//    |   Andy|  4500|
//    | Justin|  3500|
//    |  Berta|  4000|
//    +-------+------+


    spark.udf.register("strLength",(str: String) => str.length())

    ds.createOrReplaceTempView("employees")

    spark.sql("select name,salary,strLength(name) as name_Length from employees").show()

//    +-------+------+-----------+
//    |   name|salary|name_Length|
//    +-------+------+-----------+
//    |Michael|  3000|          7|
//    |   Andy|  4500|          4|
//    | Justin|  3500|          6|
//    |  Berta|  4000|          5|
//    +-------+------+-----------+

    spark.stop()
  }
}

```
### UDF (字段转成大写)
- 对数据集中，每行数据的特定字段，计算字符长度
- 通过 dataSet.withColumn 调用column
- Column通过udf函数转换

```

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


/**
  * 自定义匿名函数
  * 功能: 得到某列数据长度的函数
  */
object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    val ds = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")
    ds.show()

//    +-------+------+
//    |   name|salary|
//    +-------+------+
//    |Michael|  3000|
//    |   Andy|  4500|
//    | Justin|  3500|
//    |  Berta|  4000|
//    +-------+------+

    import org.apache.spark.sql.functions._
    val strUpper = udf((str: String) => str.toUpperCase())

    import spark.implicits._
    ds.withColumn("toUpperCase", strUpper($"name")).show
//    +-------+------+-----------+
//    |   name|salary|toUpperCase|
//    +-------+------+-----------+
//    |Michael|  3000|    MICHAEL|
//    |   Andy|  4500|       ANDY|
//    | Justin|  3500|     JUSTIN|
//    |  Berta|  4000|      BERTA|
//    +-------+------+-----------+



    spark.stop()
  }
}


```

## UDAF
- UDAF（user-defined aggregate function, 用户定义的聚合函数
- 同时处理多行，并且返回一个结果，通常结合使用 GROUP BY 语句（例如 COUNT 或 SUM）


### count
- 统计一共有多少行数据

```

package com.opensource.bigdata.spark.sql.n_08_spark_udaf.n_01_spark_udaf_count

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * ).initialize()方法,初使使，即没数据时的值
  * ).update() 方法把每一行的数据进行计算，放到缓冲对象中
  * ).merge() 把每个分区，缓冲对象进行合并
  * ).evaluate()计算结果表达式，把缓冲对象中的数据进行最终计算
  */
object Run2 extends BaseSparkSession{



  object CustomerCount extends UserDefinedAggregateFunction{

    //聚合函数的输入参数数据类型
    def inputSchema: StructType = {
      StructType(StructField("inputColumn",StringType) :: Nil)
    }

    //中间缓存的数据类型
    def bufferSchema: StructType = {
      StructType(StructField("sum",LongType)  :: Nil)
    }

    //最终输出结果的数据类型
    def dataType: DataType = LongType

    def deterministic: Boolean = true

    //初始值，要是DataSet没有数据，就返回该值
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
    }


    /**
      *
      * @param buffer  相当于把当前分区的，每行数据都需要进行计算，计算的结果保存到buffer中
      * @param input
      */
    def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
      if(!input.isNullAt(0)){
        buffer(0) = buffer.getLong(0) + 1
      }
    }

    /**
      * 相当于把每个分区的数据进行汇总
      * @param buffer1  分区一的数据
      * @param buffer2  分区二的数据
      */
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit={
      buffer1(0) = buffer1.getLong(0) +buffer2.getLong(0)  //   salary
    }


    //计算最终的结果
    def evaluate(buffer: Row): Long = buffer.getLong(0)


  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    spark.udf.register("customerCount",CustomerCount)

    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")
    df.createOrReplaceTempView("employees")
    val sqlDF = spark.sql("select customerCount(name)  as average_salary from employees  ")

    df.show()
//    +-------+------+
//    |   name|salary|
//    +-------+------+
//    |Michael|  3000|
//    |   Andy|  4500|
//    | Justin|  3500|
//    |  Berta|  4000|
//    +-------+------+

    sqlDF.show()


//    +--------------+
//    |average_salary|
//    +--------------+
//    |           4.0|
//    +--------------+

    spark.stop()
  }

}


```

### max
- 统计收入最高的
```
package com.opensource.bigdata.spark.sql.n_08_spark_udaf.n_03_spark_udaf_sum

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * ).initialize()方法,初使使，即没数据时的值
  * ).update() 方法把每一行的数据进行计算，放到缓冲对象中
  * ).merge() 把每个分区，缓冲对象进行合并
  * ).evaluate()计算结果表达式，把缓冲对象中的数据进行最终计算
  */
object Run extends BaseSparkSession{



  object CustomerSum extends UserDefinedAggregateFunction{

    //聚合函数的输入参数数据类型
    def inputSchema: StructType = {
      StructType(StructField("inputColumn",LongType) :: Nil)
    }

    //中间缓存的数据类型
    def bufferSchema: StructType = {
      StructType(StructField("sum",LongType) :: StructField("count",LongType) :: Nil)
    }

    //最终输出结果的数据类型
    def dataType: DataType = LongType

    def deterministic: Boolean = true

    //初始值，要是DataSet没有数据，就返回该值
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
    }


    /**
      *
      * @param buffer  相当于把当前分区的，每行数据都需要进行计算，计算的结果保存到buffer中
      * @param input
      */
    def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
      if(!input.isNullAt(0)){
        buffer(0) =   buffer.getLong(0) + input.getLong(0)
      }
    }

    /**
      * 相当于把每个分区的数据进行汇总
      * @param buffer1  分区一的数据
      * @param buffer2  分区二的数据
      */
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit={
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    }


    //计算最终的结果
    def evaluate(buffer: Row): Long = buffer.getLong(0)


  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    spark.udf.register("customerSum",CustomerSum)

    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")
    df.createOrReplaceTempView("employees")
    val sqlDF = spark.sql("select customerSum(salary)  as average_salary from employees  ")

    df.show
//    +-------+------+
//    |   name|salary|
//    +-------+------+
//    |Michael|  3000|
//    |   Andy|  4500|
//    | Justin|  3500|
//    |  Berta|  4000|
//    +-------+------+

    sqlDF.show()


//    +--------------+
//    |average_salary|
//    +--------------+
//    |        15000|
//    +--------------+

    spark.stop()
  }

}


```


### average
- 统计平均收入水平
```
package com.opensource.bigdata.spark.sql.n_08_spark_udaf.n_04_spark_udaf_average

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object Run extends BaseSparkSession{



  object MyAverage extends UserDefinedAggregateFunction{

    //聚合函数的输入参数数据类型
    def inputSchema: StructType = {
      StructType(StructField("inputColumn",LongType) :: Nil)
    }

    //中间缓存的数据类型
    def bufferSchema: StructType = {
      StructType(StructField("sum",LongType) :: StructField("count",LongType) :: Nil)
    }

    //最终输出结果的数据类型
    def dataType: DataType = DoubleType

    def deterministic: Boolean = true

    //初始值，要是DataSet没有数据，就返回该值
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }


    /**
      *
      * @param buffer  相当于把当前分区的，每行数据都需要进行计算，计算的结果保存到buffer中
      * @param input
      */
    def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
      if(!input.isNullAt(0)){
        buffer(0) = buffer.getLong(0) + input.getLong(0)   // salary
        buffer(1) = buffer.getLong(1) + 1  // count
      }
    }

    /**
      * 相当于把每个分区的数据进行汇总
      * @param buffer1  分区一的数据
      * @param buffer2  分区二的数据
      */
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit={
      buffer1(0) = buffer1.getLong(0) +buffer2.getLong(0)  //   salary
      buffer1(1) = buffer1.getLong(1) +buffer2.getLong(1)  // count
    }


    //计算最终的结果
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)


  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    spark.udf.register("MyAverage",MyAverage)

    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")
    df.createOrReplaceTempView("employees")
    val sqlDF = spark.sql("select MyAverage(salary)  as average_salary from employees  ")

    sqlDF.show()

    spark.stop()
  }

}


```


### group by max
- 按性别分组统计收入最高是多少
- 即统计男，女，各收入最高是多少

```
package com.opensource.bigdata.spark.sql.n_08_spark_udaf.n_05_spark_udaf_groupby_max

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * ).initialize()方法,初使使，即没数据时的值
  * ).update() 方法把每一行的数据进行计算，放到缓冲对象中
  * ).merge() 把每个分区，缓冲对象进行合并
  * ).evaluate()计算结果表达式，把缓冲对象中的数据进行最终计算
  */
object Run extends BaseSparkSession{



  object CustomerMax extends UserDefinedAggregateFunction{

    //聚合函数的输入参数数据类型
    def inputSchema: StructType = {
      StructType(StructField("inputColumn",LongType) :: Nil)
    }

    //中间缓存的数据类型
    def bufferSchema: StructType = {
      StructType(StructField("sum",LongType) :: StructField("count",LongType) :: Nil)
    }

    //最终输出结果的数据类型
    def dataType: DataType = LongType

    def deterministic: Boolean = true

    //初始值，要是DataSet没有数据，就返回该值
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
    }


    /**
      *
      * @param buffer  相当于把当前分区的，每行数据都需要进行计算，计算的结果保存到buffer中
      * @param input
      */
    def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
      if(!input.isNullAt(0)){
        if(input.getLong(0) > buffer.getLong(0)){
          buffer(0) = input.getLong(0)
        }
      }
    }

    /**
      * 相当于把每个分区的数据进行汇总
      * @param buffer1  分区一的数据
      * @param buffer2  分区二的数据
      */
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit={
      if( buffer2.getLong(0) >  buffer1.getLong(0)) buffer1(0) = buffer2.getLong(0)
    }


    //计算最终的结果
    def evaluate(buffer: Row): Long = buffer.getLong(0)


  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    spark.udf.register("customerMax",CustomerMax)

    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employeesCN.json")
    df.createOrReplaceTempView("employees")
    val sqlDF = spark.sql("select gender,customerMax(salary)  as average_salary from employees group by gender  ")

    df.show
//    +------+----+------+
//    |gender|name|salary|
//    +------+----+------+
//    |    男|小王| 30000|
//    |    女|小丽| 50000|
//    |    男|小军| 80000|
//    |    女|小李| 90000|
//    +------+----+------+

    sqlDF.show()



//    +------+--------------+
//    |gender|average_salary|
//    +------+--------------+
//    |    男|       80000|
//    |    女|       90000|
//    +------+--------------+

    spark.stop()
  }

}

```

## 其它支持
- Spark SQL 支持集成现有 Hive 中的 UDF ,UDAF 和 UDTF 的（Java或Scala）实现。
- UDTFs（user-defined table functions, 用户定义的表函数）可以返回多列和多行
end


