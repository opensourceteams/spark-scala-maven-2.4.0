package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_03_dataframe_select_column

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")
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
