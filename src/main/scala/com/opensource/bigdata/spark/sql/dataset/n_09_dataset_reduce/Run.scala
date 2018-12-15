package com.opensource.bigdata.spark.sql.dataset.n_09_dataset_reduce

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.txt")

    /**
      * 行中最多有几个单词
      */
    import spark.implicits._
    val lineWordLength = dataSet.map( line => line.split(" ").size)

    val result = lineWordLength.reduce((a,b) => Math.max(a,b))

    println(result)





    spark.stop()


  }
}

