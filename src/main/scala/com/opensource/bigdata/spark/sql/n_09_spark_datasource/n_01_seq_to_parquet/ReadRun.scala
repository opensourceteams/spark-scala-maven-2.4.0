package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_01_seq_to_parquet

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)
    spark.read.load("/home/liuwen/output/parquet/student.parquet").show

//    +----+---+
//    |  _1| _2|
//    +----+---+
//    |小王| 25|
//    |小军| 30|
//    +----+---+
    spark.stop()
  }
}

// value write is not a member of Unit