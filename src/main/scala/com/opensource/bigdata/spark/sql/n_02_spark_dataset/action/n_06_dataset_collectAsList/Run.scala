package com.opensource.bigdata.spark.sql.n_02_spark_dataset.action.n_06_dataset_collectAsList

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession(true)

    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")
    println( dataSet.collectAsList())
    import scala.collection.JavaConversions._
    for( v <- dataSet.collectAsList()) println(v)
    spark.stop()


  }
}

