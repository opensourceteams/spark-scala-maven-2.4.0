package com.opensource.bigdata.spark.standalone.wordcount.spark.session

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object WorldCount extends BaseSparkSession{


  def main(args: Array[String]): Unit = {
    appName = "WorldCount"


    val spark = sparkSession(false,false,false,-1)
    import spark.implicits._
    val distFile = spark.read.textFile("data/text/worldCount.txt")
    val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(x => x ).count()

    println(s"结果:${dataset.collect().mkString("\n\n")}")

    spark.stop()

  }
}

