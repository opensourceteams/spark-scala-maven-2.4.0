package com.opensource.bigdata.spark.standalone.wordcount.spark.session.n.n_03_collect

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {
    appName = "WorldCountDebug"


    val spark = sparkSession(true,false,false,-1)
    val distFile = spark.read.textFile("file:///"+ getProjectPath + "/src/main/resource/data/text/worldCount.txt")
    println("结果:"+distFile.collect().mkString("\n"))


    spark.stop()

  }
}

