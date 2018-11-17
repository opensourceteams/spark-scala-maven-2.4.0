package com.opensource.bigdata.spark.local

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object LocalRun {

  var appName = "localTest"
  var master = "local[1]" //本地模式:local

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    println("==========")
    println(distData)

  }
}
