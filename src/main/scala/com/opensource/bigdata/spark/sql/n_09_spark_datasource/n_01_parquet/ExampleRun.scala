package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_01_parquet

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ExampleRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)


    val peopleDF = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/json/people.json")
    peopleDF.show
    //+----+-------+
    //| age|   name|
    //+----+-------+
    //|null|Michael|
    //|  30|   Andy|
    //|  19| Justin|

    peopleDF.write.parquet("people.parquet")
    val parquetFileDF = spark.read.parquet("people.parquet")

    parquetFileDF.show()
    //+----+-------+
    //| age|   name|
    //+----+-------+
    //|null|Michael|
    //|  30|   Andy|
    //|  19| Justin|

    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.show()
//    +------+
//    |  name|
//    +------+
//    |Justin|
//    +------+




    spark.stop()
  }
}

// value write is not a member of Unit