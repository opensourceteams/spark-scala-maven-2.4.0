# Spark2.4.0 SparkSession 源码分析

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0
 
## 时序图

![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/SparkSession%E6%97%B6%E5%BA%8F%E5%9B%BE.jpg)

## 前置条件
- Hadoop版本: hadoop-2.9.2
- Spark版本: spark-2.4.0-bin-hadoop2.7
- JDK.1.8.0_191
- scala2.11.12
 
 
##  主要内容描述

- 创建SparkContext
- new SparkSession
 
## 客户端程序

### BaseSparkSession 工具类
```
package com.opensource.bigdata.spark.standalone.base

import java.io.File

import org.apache.spark.sql.SparkSession

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


  def sparkSession(isLocal:Boolean = false, isHiveSupport:Boolean = false, remoteDebug:Boolean=false): SparkSession = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    if(isLocal){
      master = "local"
      var builder = SparkSession.builder
        .master(master)
        .appName(appName)
        .config("spark.sql.warehouse.dir",warehouseLocation)

      if(isHiveSupport){
        builder = builder.enableHiveSupport()
          //.config("spark.sql.hive.metastore.version","2.3.3")
      }

      val spark = builder.getOrCreate()

      //spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
      //import spark.implicits._
      spark
    }else{

      var builder = SparkSession.builder
        .master(master)
        .appName(appName)
        .config("spark.sql.warehouse.dir",warehouseLocation)

        .config("spark.eventLog.enabled","true")
        .config("spark.eventLog.compress","true")
        .config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
        .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")

       //executor debug,是在提交作的地方读取
        if(remoteDebug){
          builder.config("spark.executor.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002")
        }



      if(isHiveSupport){
        builder = builder.enableHiveSupport()
        //.config("spark.sql.hive.metastore.version","2.3.3")
      }

      val spark = builder.getOrCreate()
      //需要有jar才可以在远程执行
      spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")



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


### 客户端程序
```
package com.opensource.bigdata.spark.standalone.sql.dataset.n_01_textFile_head

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{


  appName = "Dataset head"

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false,false,false)
    //返回dataFrame
    val df = spark.read.textFile("data/text/line.txt")
    val result = df.head(3)

    println(s"运行结果: ${result.mkString("\n")}")




    spark.stop()
  }

}

```


## SparkSession

### SparkSession.Builder
```
      var builder = SparkSession.builder
        .master(master)
        .appName(appName)
        .config("spark.sql.warehouse.dir",warehouseLocation)

        .config("spark.eventLog.enabled","true")
        .config("spark.eventLog.compress","true")
        .config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
        .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")

```

### SparkSession.Builder.getOrCreate()
- 得到SparkSession
- new SparkConf()
- 调用SparkContext.getOrCreate(sparkConf)构建SparkContext
- new SparkSession(sparkContext, None, None, extensions) 实例化SparkSession
- setDefaultSession(session)
- setActiveSession(session)

```
  /**
     * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
     * one based on the options set in this builder.
     *
     * This method first checks whether there is a valid thread-local SparkSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default SparkSession, and if yes, return that one. If no valid global default
     * SparkSession exists, the method creates a new SparkSession and assigns the
     * newly created SparkSession as the global default.
     *
     * In case an existing SparkSession is returned, the config options specified in
     * this builder will be applied to the existing SparkSession.
     *
     * @since 2.0.0
     */
    def getOrCreate(): SparkSession = synchronized {
      assertOnDriver()
      // Get the session from current thread's active session.
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        if (options.nonEmpty) {
          logWarning("Using an existing SparkSession; some configuration may not take effect.")
        }
        return session
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
          if (options.nonEmpty) {
            logWarning("Using an existing SparkSession; some configuration may not take effect.")
          }
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }

          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }

        // Initialize extensions if the user has defined a configurator class.
        val extensionConfOption = sparkContext.conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
        if (extensionConfOption.isDefined) {
          val extensionConfClassName = extensionConfOption.get
          try {
            val extensionConfClass = Utils.classForName(extensionConfClassName)
            val extensionConf = extensionConfClass.newInstance()
              .asInstanceOf[SparkSessionExtensions => Unit]
            extensionConf(extensions)
          } catch {
            // Ignore the error if we cannot find the class or when the class has the wrong type.
            case e @ (_: ClassCastException |
                      _: ClassNotFoundException |
                      _: NoClassDefFoundError) =>
              logWarning(s"Cannot use $extensionConfClassName to configure session extensions.", e)
          }
        }

        session = new SparkSession(sparkContext, None, None, extensions)
        options.foreach { case (k, v) => session.initialSessionOptions.put(k, v) }
        setDefaultSession(session)
        setActiveSession(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            defaultSession.set(null)
          }
        })
      }

      return session
    }

```

### SparkContext.getOrCreate(sparkConf)
- 构建SparkContext

```
/**
   * This function may be used to get or instantiate a SparkContext and register it as a
   * singleton object. Because we can only have one active SparkContext per JVM,
   * this is useful when applications may wish to share a SparkContext.
   *
   * @note This function cannot be used to create multiple SparkContext instances
   * even if multiple contexts are allowed.
   * @param config `SparkConfig` that will be used for initialisation of the `SparkContext`
   * @return current `SparkContext` (or a new one if it wasn't created before the function call)
   */
  def getOrCreate(config: SparkConf): SparkContext = {
    // Synchronize to ensure that multiple create requests don't trigger an exception
    // from assertNoOtherContextIsRunning within setActiveContext
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(config), allowMultipleContexts = false)
      } else {
        if (config.getAll.nonEmpty) {
          logWarning("Using an existing SparkContext; some configuration may not take effect.")
        }
      }
      activeContext.get()
    }
  }


```

end