package com.opensource.bigdata.spark.standalone.sql.dataset.n_01_show_count

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run_dataset_count extends BaseSparkSession{


  appName = "Dataset textFile count partitions"

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false,false,true,-1)
    //读取HDFS上文件
    val rs = spark.read.textFile("data/text/line.txt")
    val count = rs.count()
    println(s"结果:${count}")



    spark.stop()
  }

}
