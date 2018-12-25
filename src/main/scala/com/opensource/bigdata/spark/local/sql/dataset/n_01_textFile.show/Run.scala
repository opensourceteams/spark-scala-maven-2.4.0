package com.opensource.bigdata.spark.local.sql.dataset.n_01_textFile.show

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
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
