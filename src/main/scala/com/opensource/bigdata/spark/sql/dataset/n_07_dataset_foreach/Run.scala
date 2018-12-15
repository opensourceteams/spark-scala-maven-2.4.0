package com.opensource.bigdata.spark.sql.dataset.n_07_dataset_foreach

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")


    dataSet.foreach(println(_))





    spark.stop()


  }
}

