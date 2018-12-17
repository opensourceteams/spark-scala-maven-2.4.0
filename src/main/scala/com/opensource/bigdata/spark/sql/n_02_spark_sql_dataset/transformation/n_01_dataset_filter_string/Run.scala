package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.transformation.n_01_dataset_filter_string

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.cn.text")

    //age 是对列
    val result = dataSet.filter("age > 15")
    println(result.collect().mkString("\n"))





    spark.stop()


  }
}

