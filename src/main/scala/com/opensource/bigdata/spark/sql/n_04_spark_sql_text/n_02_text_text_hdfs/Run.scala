package com.opensource.bigdata.spark.sql.n_04_spark_sql_text.n_02_text_text_hdfs

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.text("hdfs://standalone.com:9000/home/liuwen/data/people.txt")
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
