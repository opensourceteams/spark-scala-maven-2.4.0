package com.opensource.bigdata.spark.local.sql.dataset.n_01_textFile_head

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/line.txt")
    val result = df.head(1)

    println(s"运行结果: ${result.mkString("\n")}")












    spark.stop()
  }

}
