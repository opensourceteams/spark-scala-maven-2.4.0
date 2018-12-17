package com.opensource.bigdata.spark.sql.n_02_spark_dataset.action.n_19_dataset_describe

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/json/people.json")

    dataSet.describe("name","age").show()

//    +-------+-------+------------------+
//    |summary|   name|               age|
//    +-------+-------+------------------+
//    |  count|      3|                 2|
//    |   mean|   null|              24.5|
//    | stddev|   null|7.7781745930520225|
//    |    min|   Andy|                19|
//    |    max|Michael|                30|
//    +-------+-------+------------------+





    spark.stop()


  }
}

