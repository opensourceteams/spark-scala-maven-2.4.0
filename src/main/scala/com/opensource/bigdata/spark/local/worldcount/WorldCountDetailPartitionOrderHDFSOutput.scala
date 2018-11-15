package com.opensource.bigdata.spark.local.worldcount

import org.apache.spark.{SparkConf, SparkContext}

object WorldCountDetailPartitionOrderHDFSOutput {

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
    val r1:org.apache.spark.rdd.RDD[String] = sc.textFile("hdfs://standalone:9000/root/data/a/a.txt")
    println("===================")
    println(r1)
    val r2 = r1.flatMap(_.split(" "))
    val r3 =  r2.map((_,1))
   val r4 = r3.reduceByKey(_+_,2)
    val r5Sort = r4.sortByKey()
   r5Sort.saveAsTextFile("d:/home/data/output/2")
    println(s"结果:${r5Sort}")
    val threadName = Thread.currentThread().getId + Thread.currentThread().getName

    println(s"===================结果:执行了毫秒:${System.currentTimeMillis() - startTime}")
    sc.stop()

  }
}
