package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_29_dataframe_to_sql_class_foreach_getValuesMap

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
    sqlDF.foreach( r => println(r.getValuesMap(Seq("name","age"))))

//    Map(name -> Michael, age -> 29)
//    Map(name -> Andy, age -> 30)
//    Map(name -> Justin, age -> 19)
//    Map(name -> Think, age -> 30)
//
//
//
//















    spark.stop()
  }

}
