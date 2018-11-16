package com.opensource.bigdata.spark.local.rdd.operation.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 相当于内连接，对两个rdd(只能是 (k,v) 元组rdd)进行连接
  */
object FullOuterJoinRun {

  var appName = "worldcount-3"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array(("a",1),("b",2)),2)
    val r2 = sc.parallelize(Array(("a",1),("c",2),("d",2),("a",2)),2)
    val r3 = r1.fullOuterJoin(r2)
    //val r2 = r1.distinct().collect().mkString

    println("结果:"+ r3.collect().mkString)



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
