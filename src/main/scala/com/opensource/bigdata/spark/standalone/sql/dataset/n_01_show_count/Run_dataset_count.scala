package com.opensource.bigdata.spark.standalone.sql.dataset.n_01_show_count

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run_dataset_count extends BaseSparkSession{


  appName = "Dataset textFile count"

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true,false,false)
    //返回dataFrame
    val df = spark.read.textFile("data/text/line.txt")
    println(s"结果:${df.count()}")



    spark.stop()
  }

}
