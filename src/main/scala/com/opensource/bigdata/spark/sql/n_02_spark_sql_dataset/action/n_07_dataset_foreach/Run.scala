package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.action.n_07_dataset_foreach

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run1$Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession(true)
    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")
    dataSet.foreach(println(_))





    spark.stop()


  }
}

