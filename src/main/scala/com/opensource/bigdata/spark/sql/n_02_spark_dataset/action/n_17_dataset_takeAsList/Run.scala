package com.opensource.bigdata.spark.sql.n_02_spark_dataset.action.n_17_dataset_takeAsList

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession(true)

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.txt")



    val result = dataSet.takeAsList(10) //等于head(n)
    println(result.toArray.mkString("\n"))



    import scala.collection.JavaConversions._
    for( v <- result) println(v)







    spark.stop()


  }
}

