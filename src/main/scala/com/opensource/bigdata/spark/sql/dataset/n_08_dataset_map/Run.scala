package com.opensource.bigdata.spark.sql.dataset.n_08_dataset_map

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")
    import spark.implicits._
    val lineWordLength = dataSet.map( line => line.split(" ").size)

    println(lineWordLength.collect().mkString("\n"))





    spark.stop()


  }
}

