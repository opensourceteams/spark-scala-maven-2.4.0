package com.opensource.bigdata.spark.standalone

import org.apache.spark.{SparkConf, SparkContext}

object RunTextFileMkString {

  var appName = "localTest50"
  var master = "spark://standalone.com:7077" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.storage.blockManagerSlaveTimeoutMs","10000s")
    conf.set("spark.storage.blockManagerSlaveTimeoutMs","10000s")
    conf.set("spark.executor.heartbeatInterval","10000s")
    conf.set("spark.network.timeout","10000000ms")
    conf.set("spark.executor.heartbeat.maxFailures","100000")
    val sc = new SparkContext(conf)
    //只能用hdfs路径，本地路径，需要和服务器有同样的路径
    val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("hdfs://standalone.com:9000/root/data/a/a.txt")
    println("===================")
    println(distFile)
    val array =distFile.collect()
    println(s"结果 array:$array")

    val threadName = Thread.currentThread().getId + Thread.currentThread().getName
    println(s" $threadName 结果 :${array.mkString}")

  }
}
