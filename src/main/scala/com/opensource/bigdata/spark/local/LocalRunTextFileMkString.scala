package com.opensource.bigdata.spark.local

import org.apache.spark.{SparkConf, SparkContext}

object LocalRunTextFileMkString {

  var appName = "localTest"
  var master = "local" //本地模式:local

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.storage.blockManagerSlaveTimeoutMs","10000s")
    conf.set("spark.storage.blockManagerSlaveTimeoutMs","10000s")
    conf.set("spark.executor.heartbeatInterval","10000s")
    conf.set("spark.network.timeout","10000000ms")
    conf.set("spark.executor.heartbeat.maxFailures","10000")
    val sc = new SparkContext(conf)
    val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("e://temp//a//a.text")
    println("===================")
    println(distFile)
    val array =distFile.collect()
    println(s"结果 array:$array")

    val threadName = Thread.currentThread().getId + Thread.currentThread().getName
    println(s" $threadName 结果 :${array.mkString}")

  }
}
