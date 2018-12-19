package com.opensource.bigdata.spark.sql.n_10_spark_hive.n_01_show_database

import java.io.File

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.SparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
     // .master("spark://standalone.com:7077")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("show databases").show()

//    +------------+
//    |databaseName|
//    +------------+
//    |     default|
//    |        test|
//    +------------+

    spark.stop()
  }

}
