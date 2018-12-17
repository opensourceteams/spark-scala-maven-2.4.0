package com.opensource.bigdata.spark.sql.n_06_spark_sql.n_03_sql_GlobalTempView

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/json/people.json")
    df.show()


//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//    |  30|   Andy|
//    |  19| Justin|
//    +----+-------+
    df.createOrReplaceGlobalTempView("people")

    val sqlDF = spark.sql("SELECT * FROM global_temp.people")
    val sqlDFNewSession = spark.newSession.sql("SELECT * FROM global_temp.people")
    sqlDF.show()
    //    +----+-------+
    //    | age|   name|
    //    +----+-------+
    //    |null|Michael|
    //    |  30|   Andy|
    //    |  19| Justin|
    //    +----+-------+

    sqlDFNewSession.show()
    //    +----+-------+
    //    | age|   name|
    //    +----+-------+
    //    |null|Michael|
    //    |  30|   Andy|
    //    |  19| Justin|
    //    +----+-------+











    spark.stop()
  }

}
