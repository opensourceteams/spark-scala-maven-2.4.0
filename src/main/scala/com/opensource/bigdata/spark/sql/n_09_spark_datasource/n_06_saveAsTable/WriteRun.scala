package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_06_saveAsTable

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object WriteRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    val sqlDF = spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/output/json/employ.json")
    sqlDF.show
    //+----+-------+
    //| age|   name|
    //+----+-------+
    //|null|Michael|
    //|  30|   Andy|
    //|  19| Justin|

    sqlDF.write.saveAsTable("people_bucketed")

    val sqlDF2 = spark.sql("select * from people_bucketed")
    sqlDF2.show

    spark.stop()
  }
}

// value write is not a member of Unit