package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_03_csv

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    val peopleDFCsv = spark.read.format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("hdfs://m0:9000/home/liuwen/data/csv/people.csv")
    //peopleDFCsv: org.apache.spark.sql.DataFrame = [name: string, age: int ... 1 more field]
    peopleDFCsv.show
    // +-----+---+---------+
    //| name|age|      job|
    //+-----+---+---------+
    //|Jorge| 30|Developer|
    //|  Bob| 32|Developer|
    //+-----+---+---------+



    spark.stop()
  }
}

// value write is not a member of Unit