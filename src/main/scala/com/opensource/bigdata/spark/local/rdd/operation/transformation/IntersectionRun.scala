package com.opensource.bigdata.spark.local.rdd.operation.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 交集
  */
object IntersectionRun {

  var appName = "worldcount-3"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array("A","B","C","B"),2)
    val r2 = sc.parallelize(Array("A","D","A","A"),2)
    //val r3 = r1.distinct().union(r2.distinct())
    val r3 = r1.intersection(r2)

    r3.collect().foreach(x => println(s"行:${x}"))




    sc.stop()
  }

  def pre(): SparkContext ={
    var startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.history.fs.logDirectory","/opt/bigdata/spark-1.6.0-cdh5.15.0/rundata/historyEventLog")
    conf.set("spark.eventLog.dir","/home/spark/log/eventLog")
    conf.setJars(Array("D:\\workspaces\\bigdata\\spark-scala-maven\\target\\spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)
    sc
  }
}
