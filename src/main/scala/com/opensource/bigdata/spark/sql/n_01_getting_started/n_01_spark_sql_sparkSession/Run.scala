package com.opensource.bigdata.spark.sql.n_01_getting_started.n_01_spark_sql_sparkSession

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    println(spark)

    spark.stop()
  }

}
