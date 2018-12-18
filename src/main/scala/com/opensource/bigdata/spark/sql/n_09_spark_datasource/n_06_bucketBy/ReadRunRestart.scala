package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_06_bucketBy

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRunRestart extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    //val sqlDF = spark.sql("SELECT * FROM parquet.`file:/home/liuwen/spark-warehouse/people_bucketed`")
    val sqlDF = spark.sql("SELECT * FROM parquet.`file:/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/spark-warehouse/people_bucketed`")
    sqlDF.show

    spark.stop()
  }
}

// value write is not a member of Unit