package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_10_dataframe_groupBy_string

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")


    //注意，所以列都需要加上$符
    df.groupBy("age").count()
//    +---+-----+
//    |age| name|
//    +---+-----+
//    | 30| Andy|
//    | 30|Think|
//    +---+-----+








    spark.stop()
  }

}
