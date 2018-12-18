package com.opensource.bigdata.spark.sql.n_07_spark_udf.n_03_udf_str_toUpperCase


import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


/**
  * 自定义匿名函数
  * 功能: 得到某列数据长度的函数
  */
object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    val ds = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")
    ds.show()

//    +-------+------+
//    |   name|salary|
//    +-------+------+
//    |Michael|  3000|
//    |   Andy|  4500|
//    | Justin|  3500|
//    |  Berta|  4000|
//    +-------+------+

    import org.apache.spark.sql.functions._
    val strUpper = udf((str: String) => str.toUpperCase())

    import spark.implicits._
    ds.withColumn("toUpperCase", strUpper($"name")).show
//    +-------+------+-----------+
//    |   name|salary|toUpperCase|
//    +-------+------+-----------+
//    |Michael|  3000|    MICHAEL|
//    |   Andy|  4500|       ANDY|
//    | Justin|  3500|     JUSTIN|
//    |  Berta|  4000|      BERTA|
//    +-------+------+-----------+



    spark.stop()
  }
}
