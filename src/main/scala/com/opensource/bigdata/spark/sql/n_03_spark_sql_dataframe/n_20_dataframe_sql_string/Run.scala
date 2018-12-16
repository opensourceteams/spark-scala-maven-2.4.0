package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_20_dataframe_sql_string

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select name,age ,age as age_2 from people")
    sqlDF.show()
//    +-------+----+-----+
//    |   name| age|age_2|
//    +-------+----+-----+
//    |Michael|null| null|
//    |   Andy|  30|   30|
//    | Justin|  19|   19|
//    |  Think|  30|   30|
//    |  Think|  35|   35|
//    +-------+----+-----+






    spark.stop()
  }

}
