package com.opensource.bigdata.spark.sql.n_10_spark_hive.n_01_create_table

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

    //import spark.implicits._
    import spark.sql

    //sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")

    //sql("LOAD DATA LOCAL INPATH 'spark-scala-maven-2.4.0/src/main/resource/data/text/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    spark.stop()
  }

}
