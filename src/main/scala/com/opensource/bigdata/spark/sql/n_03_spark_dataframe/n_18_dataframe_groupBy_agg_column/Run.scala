package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_18_dataframe_groupBy_agg_column

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //注意，所以列都需要加上$符
    df.groupBy($"name").agg($"name", max("age"), min("age"), avg("age")).show()
    //Think有两条记录，一条30,一行35,(30 +35) /2 =32.5 所以取最小值30的

//    +-------+-------+--------+--------+--------+
//    |   name|   name|max(age)|min(age)|avg(age)|
//    +-------+-------+--------+--------+--------+
//    |Michael|Michael|    null|    null|    null|
//    |  Think|  Think|      35|      30|    32.5|
//    |   Andy|   Andy|      30|      30|    30.0|
//    | Justin| Justin|      19|      19|    19.0|
//    +-------+-------+--------+--------+--------+








    spark.stop()
  }

}
