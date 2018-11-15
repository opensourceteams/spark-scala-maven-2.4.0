package com.opensource.bigdata.spark.local.rdd.operation.action

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyAndSortWithRun {

  var appName = "worldcount-3"
  /**
    * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
    * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
    */
  var master = "local[2]" //本地模式:local  local[2]    standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    var startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.history.fs.logDirectory","/opt/bigdata/spark-1.6.0-cdh5.15.0/rundata/historyEventLog")
    conf.set("spark.eventLog.dir","/home/spark/log/eventLog")
    conf.setJars(Array("D:\\workspaces\\bigdata\\spark-scala-maven\\target\\spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)
    //val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("e://temp//a//a.txt")
    /**
      *  idea中远程提交时，/home/temp/a.txt文件需要在window中存在，并且是在项目文件所有磁盘上
      */
    val r1:org.apache.spark.rdd.RDD[String] = sc.textFile("/home/data/input/b.txt")

    println(s"结果:${r1.collect().mkString}")

    val r2 = r1.map(_.split(" "))
    println(s"结果:${r2.collect().mkString}")
    val r3 = r2.map(x => (x(0) ,x(1) ) )
    println(s"结果:${r3.collect().mkString}")

    val r4 = r3.groupByKey()
    //val r5 = r4.map(x => (x._1,x._2.toList.sorted))
    val r6 = r4.map(x => (x._1,x._2.toList.sortWith((a,b) => a < b)))

    println(s"结果:${r6.collect().mkString}")

    val threadName = Thread.currentThread().getId + Thread.currentThread().getName

    println(s"===================threadName:${threadName}  结果:执行了毫秒:${System.currentTimeMillis() - startTime}")
    sc.stop()

  }
}
