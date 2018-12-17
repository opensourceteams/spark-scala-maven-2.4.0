package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.action.n_12_dataset_show_n

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.text")

    /**
      * 以表格的形式显示前20行数据,(默认是取前20行数据)
      */

    val result = dataSet.show(3)
    println(result)





    spark.stop()


  }
}

