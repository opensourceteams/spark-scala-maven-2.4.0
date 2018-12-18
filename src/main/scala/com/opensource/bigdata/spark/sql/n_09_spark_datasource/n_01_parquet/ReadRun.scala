package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_01_parquet

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)
    spark.read.load("hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet").show
    //+------+--------------+----------------+
    //|  name|favorite_color|favorite_numbers|
    //+------+--------------+----------------+
    //|Alyssa|          null|  [3, 9, 15, 20]|
    //|   Ben|           red|              []|
    //+------+--------------+----------------+
    spark.stop()
  }
}

// value write is not a member of Unit