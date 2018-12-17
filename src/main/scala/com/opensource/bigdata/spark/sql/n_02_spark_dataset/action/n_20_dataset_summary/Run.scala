package com.opensource.bigdata.spark.sql.n_02_spark_dataset.action.n_20_dataset_summary

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/json/people.json")

    dataSet.summary().show()
    dataSet.summary("count","max").show()







    spark.stop()


  }
}

