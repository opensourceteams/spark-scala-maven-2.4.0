package com.opensource.bigdata.spark.standalone.wordcount

import com.opensource.bigdata.spark.standalone.base.BaseScalaSparkContext



object WorldCount extends BaseScalaSparkContext{


  def main(args: Array[String]): Unit = {
  //  val log = LoggerFactory.getLogger(WorldCount.getClass)

    val startTime = System.currentTimeMillis()

    appName = "HelloWorld-standalone"
    //master="spark://10.211.55.2:7077"
    val sc = sparkContext

    println("SparkContext加载完成")


    val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("hdfs://standalone.com:9000/opt/data/b.txt")
    println(distFile)

   val result = distFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    println(s"结果:${result.collect().mkString}")


    val threadName = Thread.currentThread().getId + Thread.currentThread().getName

    println(s"${threadName}===================结果:执行了毫秒:${System.currentTimeMillis() - startTime}")


    sc.stop()

  }
}

