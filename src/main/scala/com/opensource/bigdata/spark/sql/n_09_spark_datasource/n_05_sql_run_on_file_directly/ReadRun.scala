package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_05_sql_run_on_file_directly

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    val sqlDF = spark.sql("SELECT * FROM parquet.`hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet`")
    sqlDF.show
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