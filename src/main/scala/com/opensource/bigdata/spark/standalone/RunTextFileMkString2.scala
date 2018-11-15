package com.opensource.bigdata.spark.standalone

import org.apache.spark.{SparkConf, SparkContext}

object RunTextFileMkString2 {

  var appName = "local-34"
  var master = "spark://standalone.com:7077" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    var startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName(appName).setMaster(master)


    val sc = new SparkContext(conf)
    //val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("e://temp//a//a.txt")
    val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("hdfs://standalone.com:9000/root/data/a/a.txt")
    println("===================")
    println(distFile)
    val array =distFile.collect()
    val threadName = Thread.currentThread().getId + Thread.currentThread().getName
    println(s" $threadName 结果 :${array.mkString}")
    println(s"===================结果:执行了毫秒:${System.currentTimeMillis() - startTime}")
    sc.stop()

  }
}
