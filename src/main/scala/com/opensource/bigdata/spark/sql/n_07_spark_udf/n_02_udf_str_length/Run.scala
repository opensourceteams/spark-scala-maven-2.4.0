package com.opensource.bigdata.spark.sql.n_07_spark_udf.n_02_udf_str_length

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

    def getStrLength(str:String):Long = {
      if(str.isEmpty) 0
      str.length
    }


    spark.udf.register("strLength",getStrLength _)

    ds.createOrReplaceTempView("employees")

    spark.sql("select name,salary,strLength(name) as name_Length from employees").show()

    spark.stop()
  }
}
