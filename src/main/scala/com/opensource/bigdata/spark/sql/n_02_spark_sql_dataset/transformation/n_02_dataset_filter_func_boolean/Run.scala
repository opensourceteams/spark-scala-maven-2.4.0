package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.transformation.n_02_dataset_filter_func_boolean

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.cn.txt")

    val result = dataSet.filter(line => line.contains("spark"))
    println(result.collect().mkString("\n\n"))





    spark.stop()


  }
}

