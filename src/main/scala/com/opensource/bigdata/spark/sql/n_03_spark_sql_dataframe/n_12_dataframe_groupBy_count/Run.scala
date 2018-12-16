package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_12_dataframe_groupBy_count

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")
    import spark.implicits._

    //注意，所以列都需要加上$符
    df.groupBy($"age").count().show()
//    +----+-----+
//    | age|count|
//    +----+-----+
//    |  19|    1|
//    |null|    1|
//    |  30|    2|








    spark.stop()
  }

}
