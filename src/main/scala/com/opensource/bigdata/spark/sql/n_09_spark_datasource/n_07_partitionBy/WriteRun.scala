package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_07_partitionBy

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object WriteRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)
    val usersDF = spark.read.load("hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet")

    usersDF.show()
    //+------+--------------+----------------+
    //|  name|favorite_color|favorite_numbers|
    //+------+--------------+----------------+
    //|Alyssa|          null|  [3, 9, 15, 20]|
    //|   Ben|           red|              []|
    //+------+--------------+----------------+


    //保存在HDFS上 hdfs://standalone.com:9000/user/liuwen/namesPartByColor.parquet
    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
    spark.stop()
  }
}

// value write is not a member of Unit