package com.opensource.bigdata.spark.local.worldcount

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {

  var appName = "worldcount-3"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.history.fs.logDirectory", "/opt/module/bigdata/spark-1.6.0-cdh5.15.0/rundata/historyEventLog")
    conf.set("spark.eventLog.dir", "/opt/log/spark/log/eventLog")
    conf.setJars(Array("/opt/n_001_workspaces/bigdata/spark-scala-maven/target/spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)
    /**
      * idea中远程提交时，/home/temp/a.txt文件需要在window中存在，并且是在项目文件所有磁盘上
      */
    val distFile: org.apache.spark.rdd.RDD[String] = sc.textFile("/opt/data/a.txt")
    println("===================")
    println(distFile)

    val result = distFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)


    println(s"结果:${result.collect().mkString}")
    val threadName = Thread.currentThread().getId + Thread.currentThread().getName

    println(s"${threadName}===================结果:执行了毫秒:${System.currentTimeMillis() - startTime}")

    sc.stop()

  }
}
