package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_03_dataframe_select_column_hdfs

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false)
    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/json/people.json")
    df.select("name","age").show

//    +-------+----+
//    |   name| age|
//    +-------+----+
//    |Michael|null|
//    |   Andy|  30|
//    | Justin|  19|
//    |  Think|  30|
//    +-------+----+












    spark.stop()
  }

}
