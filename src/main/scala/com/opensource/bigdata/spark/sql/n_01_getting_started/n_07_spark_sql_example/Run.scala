package com.opensource.bigdata.spark.sql.n_01_getting_started.n_07_spark_sql_example

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    import  spark.implicits._

    val dfPerson = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json").as[Person]
    dfPerson.show()

    dfPerson.createOrReplaceTempView("people")
    val df = spark.sql("select name,age from people where age between  13 and 30 ")
    df.show()




    spark.stop()
  }

}
