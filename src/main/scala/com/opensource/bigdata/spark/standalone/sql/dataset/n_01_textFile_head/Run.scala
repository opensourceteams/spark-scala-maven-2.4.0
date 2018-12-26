package com.opensource.bigdata.spark.standalone.sql.dataset.n_01_textFile_head

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{


  appName = "Dataset head"

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false,false,false)
    //返回dataFrame
    val df = spark.read.textFile("data/text/line.txt")
    val result = df.head(3)

    println(s"运行结果: ${result.mkString("\n")}")




    spark.stop()
  }

}
