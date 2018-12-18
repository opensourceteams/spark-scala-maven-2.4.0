package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_02_json

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/data/json/people.json").show
    //+----+-------+
    //| age|   name|
    //+----+-------+
    //|null|Michael|
    //|  30|   Andy|
    //|  19| Justin|
    //+----+-------+



    spark.stop()
  }
}

// value write is not a member of Unit