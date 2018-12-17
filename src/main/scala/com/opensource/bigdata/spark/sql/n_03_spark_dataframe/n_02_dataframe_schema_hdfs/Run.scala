package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_02_dataframe_schema_hdfs

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false)
    //返回dataFrame
    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/json/people.json")
    df.printSchema()

//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)











    spark.stop()
  }

}
