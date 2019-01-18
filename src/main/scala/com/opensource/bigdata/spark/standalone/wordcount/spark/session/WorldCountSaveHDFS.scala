package com.opensource.bigdata.spark.standalone.wordcount.spark.session

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object WorldCountSaveHDFS extends BaseSparkSession{


  def main(args: Array[String]): Unit = {
    appName = "WorldCountSaveHDFS"


    val spark = sparkSession(false,false,false,7)
    import spark.implicits._
    val distFile = spark.read.textFile("data/text/worldCount.txt")
    val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(x => x ).count()

    dataset.write.format("csv").save("hdfs://standalone.com:9000/user/liuwen/data/output/output_8")

    spark.stop()

  }
}

