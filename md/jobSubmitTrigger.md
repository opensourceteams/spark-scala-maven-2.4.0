# Spark 触发Job提交

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven
- csdn(汇总视频在线看): https://blog.csdn.net/thinktothings/article/details/84726769

## youtube 视频说明
- Spark 触发Job提交(youtube视频) : https://youtu.be/X49RIqz2AjM


## bilibili 视频说明
- Spark 触发Job提交(bilibili视频) : https://www.bilibili.com/video/av37445008/

<iframe src="//player.bilibili.com/player.html?aid=37445008&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>



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
#### RDD之间的依赖关系

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

#### SparkContext中runJob调用
- RDD的collect方法，调用SparkContext的runJob方法

```scala
  /**
   * Return an array that contains all of the elements in this RDD.
   */
  def collect(): Array[T] = withScope {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }
```
- SparkContext runJob方法调用

```scala
  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
  }
```

```scala
  /**
   * Run a job on a given set of partitions of an RDD, but take a function of type
   * `Iterator[T] => U` instead of `(TaskContext, Iterator[T]) => U`.
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]): Array[U] = {
    val cleanedFunc = clean(func)
    runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
  }
```

```scala
  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array.
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }
```
```scala
  /**
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function. This is the main entry point for all actions in Spark.
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }
```
#### DagScheduler方法调用
- DagScheduler中runJob方法调用

```scala
 /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @throws Exception when the job fails
   */
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    waiter.awaitResult() match {
      case JobSucceeded =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case JobFailed(exception: Exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }
```

- DagScheduler submitJob 方法调用

```scala
 /**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }
```

#### DAGSchedulerEventProcessLoop 中runJob方法调用
- DAGScheduler事件循环器中发送事件：JobSubmitted

```scala
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
```




