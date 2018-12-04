# Spark 触发Job 提交


## 客户端源码
- github: https://github.com/opensourceteams/spark-scala-maven
- BaseScalaSparkContext.scala

```scala
package com.opensource.bigdata.spark.standalone.base


import org.apache.spark.{SparkConf, SparkContext}

class BaseScalaSparkContext {

  var appName = "standalone"
  var master = "spark://standalone.com:7077" //本地模式:local     standalone:spark://master:7077


  def sparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled","true")
   // conf.set("spark.ui.port","10002")
    conf.set("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
    conf.set("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/eventLog")
    //executor debug,是在提交作的地方读取
    //conf.set("spark.executor.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002")
    conf.setJars(Array("/opt/n_001_workspaces/bigdata/spark-scala-maven/target/spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)

    //设置日志级别
    //sc.setLogLevel("ERROR")
    sc
  }
}

```

- WorldCount.scala

```scala
package com.opensource.bigdata.spark.standalone.wordcount

import com.opensource.bigdata.spark.standalone.base.BaseScalaSparkContext

object WorldCount extends BaseScalaSparkContext{


  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    appName = "HelloWorld-standalone"
    //master="spark://10.211.55.2:7077"
    val sc = sparkContext

    println("SparkContext加载完成")


    val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("hdfs://standalone.com:9000/opt/data/a.txt")
    println(distFile)

   val result = distFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    println(s"结果:${result.collect().mkString}")

    val threadName = Thread.currentThread().getId + Thread.currentThread().getName

    println(s"${threadName}===================结果:执行了毫秒:${System.currentTimeMillis() - startTime}")


    sc.stop()

  }
}

```

## 源码分析
### worldCount.scala
- RDD之间的依赖关系为

```shell
val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("hdfs://standalone.com:9000/opt/data/a.txt")
val result = distFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
-----------------------------------------------------------------
val rdd0 =  HadoopRDD
val rdd1 =  distFile = sc.textFile 内部进行了一次map操作，hadoopRDD.map(pair => pair._2.toString)
val rdd2 =  distFile.flatMap(_.split(" "))
val rdd3 =  distFile.flatMap(_.split(" ")).map((_,1)
val rdd4 =  distFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

-----------------------------------------------------------------
ShuffledRDD[4]     ->      ShuffleDependency        ->   rdd4
MapPartitionsRDD[3]     ->      OneToOneDependency(NarrowDependency)		->   rdd3
MapPartitionsRDD[2]     ->      OneToOneDependency(NarrowDependency)		->   rdd2
MapPartitionsRDD[1]     ->      OneToOneDependency(NarrowDependency)		->   rdd1
HadoopRDD[0]     ->      Nil		->   rdd0
-----------------------------------------------------------------


```
[![WorldCount中RDD之间的关系](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/WorldCount%E4%B8%ADRDD%E4%B9%8B%E9%97%B4%E7%9A%84%E5%85%B3%E7%B3%BB.png "WorldCount中RDD之间的关系")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/WorldCount%E4%B8%ADRDD%E4%B9%8B%E9%97%B4%E7%9A%84%E5%85%B3%E7%B3%BB.png "WorldCount中RDD之间的关系")




