package com.opensource.bigdata.spark.sql.n_03_spark_sql_dataframe.n_26_dataframe_to_class_foreach

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    val ds = spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")

    import spark.implicits._
    ds.map(line => Person(line.split(",")(0),line.split(",")(1).trim.toLong))

    .foreach(person => println(s"name:${person.name}\t age:${person.age}"))




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
