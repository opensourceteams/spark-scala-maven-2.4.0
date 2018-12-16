package com.opensource.bigdata.spark.sql.dataset.action.n_16_dataset_take

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.txt")



    val result = dataSet.take(10) //等于head(n)
    println(result.mkString("\n"))





    spark.stop()


  }
}

