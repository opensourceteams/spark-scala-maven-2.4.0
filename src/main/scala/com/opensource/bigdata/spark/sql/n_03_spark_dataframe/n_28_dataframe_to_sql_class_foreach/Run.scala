package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_28_dataframe_to_sql_class_foreach

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    val rdd = spark.sparkContext.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")

    import spark.implicits._


    val peopleDF = rdd.map(line => Person(line.split(",")(0),line.split(",")(1).trim.toLong)).toDF()

    peopleDF.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.as[Person].foreach( r => println(s"name:${r.name}\tage:${r.age}"))

//    +-------+---+
//    |   name|age|
//    +-------+---+
//    |Michael| 29|
//    |   Andy| 30|
//    | Justin| 19|
//    |  Think| 30|
//    +-------+---+
//    name:Michael	age:29
//    name:Andy	age:30
//    name:Justin	age:19
//    name:Think	age:30
//
//
//
//






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
