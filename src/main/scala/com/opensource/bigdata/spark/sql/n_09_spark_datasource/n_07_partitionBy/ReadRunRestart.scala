package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_07_partitionBy

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRunRestart extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    //val sqlDF = spark.sql("SELECT * FROM parquet.`file:/home/liuwen/spark-warehouse/people_bucketed`")
    val sqlDF = spark.sql("SELECT * FROM parquet.`hdfs://standalone.com:9000/user/liuwen/namesPartByColor.parquet`")
    sqlDF.show

    spark.stop()
  }
}

// value write is not a member of Unit