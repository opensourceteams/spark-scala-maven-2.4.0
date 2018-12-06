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

    conf.set("spark.scheduler.revive.interval","100000s")
    //executor debug,是在提交作的地方读取
    //conf.set("spark.executor.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002")
    conf.setJars(Array("/opt/n_001_workspaces/bigdata/spark-scala-maven/target/spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)

    //设置日志级别
    //sc.setLogLevel("ERROR")
    sc
  }
}
