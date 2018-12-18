package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_03_csv

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object WriteRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    val peopleDFCsv = spark.read.format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("hdfs://m0:9000/home/liuwen/data/csv/people.csv")

    peopleDFCsv.select("name", "age").write.format("csv").save("hdfs://standalone.com:9000/home/liuwen/output/csv/people.csv")
    spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("hdfs://standalone.com:9000//home/liuwen/output/csv/people.csv").show
    //+-----+---+
    //|Jorge| 30|
    //+-----+---+
    //|  Bob| 32|
    //+-----+---+

    spark.stop()
  }
}

// value write is not a member of Unit