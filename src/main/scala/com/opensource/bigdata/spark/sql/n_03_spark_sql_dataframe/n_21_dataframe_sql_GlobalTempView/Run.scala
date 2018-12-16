package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_21_dataframe_sql_GlobalTempView

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")

    df.createGlobalTempView("people")
    val sqlDF = spark.sql("select * from global_temp.people").show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//    |  30|   Andy|
//    |  19| Justin|
//    |  30|  Think|
//    |  35|  Think|
//    +----+-------+


    spark.newSession().sql("select * from global_temp.people").show()
    //    +----+-------+
    //    | age|   name|
    //    +----+-------+
    //    |null|Michael|
    //    |  30|   Andy|
    //    |  19| Justin|
    //    |  30|  Think|
    //    |  35|  Think|
    //    +----+-------+






    spark.stop()
  }

}
