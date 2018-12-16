package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_05_dataframe_selectExpr_column

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")


    //注意，所以列都需要加上$符
    df.selectExpr("name", "age+1").show()

//    +-------+---------+
//    |   name|(age + 1)|
//    +-------+---------+
//    |Michael|     null|
//    |   Andy|       31|
//    | Justin|       20|
//    |  Think|       31|
//    +-------+---------+

    df.selectExpr("name", "age+1 as age").show()

//    +-------+----+
//    |   name| age|
//    +-------+----+
//    |Michael|null|
//    |   Andy|  31|
//    | Justin|  20|
//    |  Think|  31|
//    +-------+----+





    spark.stop()
  }

}
