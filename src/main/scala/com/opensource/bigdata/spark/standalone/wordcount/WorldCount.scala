package com.opensource.bigdata.spark.standalone.wordcount

import com.opensource.bigdata.spark.standalone.base.BaseScalaSparkContext

object WorldCount extends BaseScalaSparkContext{


  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    appName = "HelloWorld-standalone"
    //master="spark://10.211.55.2:7077"
    val sc = sparkContext

    println("SparkContext加载完成")
    while (true){
      Thread.sleep(1000l)
    }



    //val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("e://temp//a//a.txt")
    /**
      *  idea中远程提交时，/home/temp/a.txt文件需要在window中存在，并且是在项目文件所有磁盘上
      */
    val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("hdfs://standalone.com:9000/opt/data/a.txt")
    println("===================")
    println(distFile)

   val result = distFile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    println(s"结果:${result.collect().mkString}")
    val threadName = Thread.currentThread().getId + Thread.currentThread().getName

    println(s"${threadName}===================结果:执行了毫秒:${System.currentTimeMillis() - startTime}")
    while (true){
      Thread.sleep(1000l)
    }
    sc.stop()

  }
}
