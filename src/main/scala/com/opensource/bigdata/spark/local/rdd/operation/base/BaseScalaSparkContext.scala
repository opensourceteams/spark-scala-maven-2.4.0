package com.opensource.bigdata.spark.local.rdd.operation.base

import org.apache.spark.{SparkConf, SparkContext}

class BaseScalaSparkContext {

  var appName = "local"
  var master = "local[1]" //本地模式:local     standalone:spark://master:7077


  def pre(): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled","true")
   // conf.set("spark.ui.port","10002")
    conf.set("spark.history.fs.logDirectory","/opt/module/bigdata/spark-1.6.0-cdh5.15.0/rundata/historyEventLog")
    conf.set("spark.eventLog.dir","/opt/log/spark/log/eventLog")
    conf.setJars(Array("/opt/n_001_workspaces/bigdata/spark-scala-maven/target/spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)

    //sc.setLogLevel("ERROR")
    sc
  }
}
