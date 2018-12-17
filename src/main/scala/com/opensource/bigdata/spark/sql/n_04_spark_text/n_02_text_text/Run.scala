package com.opensource.bigdata.spark.sql.n_04_spark_text.n_02_text_text

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.text("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
    df.show()

//    +-----------+
//    |      value|
//    +-----------+
//    |Michael, 29|
//    |   Andy, 30|
//    | Justin, 19|
//    |  Think, 30|
//    +-----------+











    spark.stop()
  }

}
