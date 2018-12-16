package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.action.n_01_dataset_base

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")


    println(dataSet)





    spark.stop()


  }
}

