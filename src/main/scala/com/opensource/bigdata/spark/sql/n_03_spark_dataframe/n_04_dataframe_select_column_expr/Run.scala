package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_04_dataframe_select_column_expr

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")
    import spark.implicits._

    //注意，所以列都需要加上$符
    df.select($"name", $"age" + 1).show()

//    +-------+---------+
//    |   name|(age + 1)|
//    +-------+---------+
//    |Michael|     null|
//    |   Andy|       31|
//    | Justin|       20|
//    |  Think|       31|
//    +-------+---------+


    df.select($"name", $"age" + 1 ).show()











    spark.stop()
  }

}
