package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_13_dataframe_groupBy_max

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")
    import spark.implicits._

    //注意，所以列都需要加上$符
    df.groupBy($"name").max("age").show()
    //Think有两条记录，一条30,一行35,所以取最大值35的

//    +-------+--------+
//    |   name|max(age)|
//    +-------+--------+
//    |Michael|    null|
//    |  Think|      35|
//    |   Andy|      30|








    spark.stop()
  }

}
