package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_22_dataframe_rdd_toDF_StructType

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    val rdd = spark.sparkContext.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")


    val schema = StructType(
      Seq(StructField("name", StringType, nullable = true),StructField("age", LongType, nullable = true)  ))

    val rowRDD = rdd
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim.toLong))


    spark.createDataFrame(rowRDD,schema).foreach( row =>
      println(row.getAs[String]("name") + " -> " + row.getAs[String]("age") ))







//    +-------+---+
//    |   name|age|
//    +-------+---+
//    |Michael| 29|
//    |   Andy| 30|
//    | Justin| 19|
//    |  Think| 30|
//    +-------+---+











    spark.stop()
  }

}
