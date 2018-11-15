package com.opensource.bigdata.spark.standalone.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WorldTotalCount {

  var appName = "worldTotalcount-3"
  var master = "spark://standalone.com:7077" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    var startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.history.fs.logDirectory","/opt/bigdata/spark-1.6.0-cdh5.15.0/rundata/historyEventLog")
    conf.set("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/eventLog")
    conf.setJars(Array("D:\\workspaces\\bigdata\\spark-scala-maven\\target\\spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)
    //val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("e://temp//a//a.txt")
    /**
      *  idea中远程提交时，/home/temp/a.txt文件需要在window中存在，并且是在项目文件所有磁盘上
      */
    val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("/home/temp/a.txt")
    println("===================")
    println(distFile)

    val result = distFile.flatMap(_.split(" ")).map((_,1)).reduce( (a,b)  => ("",a._2 + b._2)  )

    println(s"结果 :${result._2}")
    //println(s"结果:${result.collect().mkString}")
    val threadName = Thread.currentThread().getId + Thread.currentThread().getName
    println(s"===================结果:执行了毫秒:${System.currentTimeMillis() - startTime}")
    sc.stop()

  }
}
