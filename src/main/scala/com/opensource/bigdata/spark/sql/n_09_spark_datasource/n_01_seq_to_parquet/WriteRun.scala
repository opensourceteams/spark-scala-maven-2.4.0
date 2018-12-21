package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_01_seq_to_parquet

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object WriteRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)
    import  spark.implicits._
    Seq(("小王",25),("小军" ,30)).toDS.write.format("parquet").save("/home/liuwen/output/parquet/student.parquet")

    spark.stop()
  }
}

