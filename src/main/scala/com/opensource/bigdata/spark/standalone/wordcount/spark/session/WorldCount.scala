package com.opensource.bigdata.spark.standalone.wordcount.spark.session


import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.SparkSession



object WorldCount extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()
    import spark.implicits._

    val distFile = spark.read.textFile("/home/liuwen/data/a.txt")

    val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(identity).count()



    println(s"${dataset.collect().mkString("\n\n")}")




    spark.stop()


  }
}

