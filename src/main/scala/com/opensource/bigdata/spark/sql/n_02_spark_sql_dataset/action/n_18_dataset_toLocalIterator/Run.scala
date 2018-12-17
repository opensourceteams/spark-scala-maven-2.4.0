package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.action.n_18_dataset_toLocalIterator

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.text")



    val result = dataSet.toLocalIterator()
    while (result.hasNext) println(result.next())







    spark.stop()


  }
}

