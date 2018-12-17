package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_07_dataframe_filter_column

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")

    import spark.implicits._

    //注意，所以列都需要加上$符
    df.filter($"age" > 20).show()
//    +---+-----+
//    |age| name|
//    +---+-----+
//    | 30| Andy|
//    | 30|Think|
//    +---+-----+








    spark.stop()
  }

}
