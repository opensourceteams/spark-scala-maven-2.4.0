package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_22_dataframe_rdd_toDF

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    val rdd = spark.sparkContext.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")

    import spark.implicits._
    val ds = rdd.map(_.split(",")).map(attributes => Person(attributes(0),attributes(1).trim.toLong)).toDS()

    ds.createOrReplaceTempView("people")
    spark.sql("select * from people").show()




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
