package com.opensource.bigdata.spark.standalone.wordcount.spark.session.n.n_01_show

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {
    appName = "WorldCountDebug"


    val spark = sparkSession(true,false,false,-1)
    val distFile = spark.read.textFile("file:///"+ getProjectPath + "/src/main/resource/data/text/worldCount.txt")
    distFile.show()


    spark.stop()

  }
}

