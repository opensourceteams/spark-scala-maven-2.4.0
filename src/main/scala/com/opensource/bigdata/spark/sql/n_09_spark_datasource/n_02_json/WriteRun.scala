package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_02_json

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object WriteRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    val ds = spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/data/json/people.json")
    ds.show
    //+----+-------+
    //| age|   name|
    //+----+-------+
    //|null|Michael|
    //|  30|   Andy|
    //|  19| Justin|
    //+----+-------+
    //保存json格式数据到hdfs上面
    ds.select("name", "age").write.format("json").save("hdfs://standalone.com:9000/home/liuwen/output/json/namesAndAges.json")
    //读取保存的数据
    spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/output/json/namesAndAges.json").show
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