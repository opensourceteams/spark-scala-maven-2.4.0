package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.create.n_04_dataset_seq_class_toDS_foreach

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {


    val spark = sparkSession(true)

    import spark.implicits._

    Seq(Person("Andy", 32) ,Person("Tom", 12)).toDS().foreach(people => println(s"name:${people.name},age:${people.age}"))

//    +----+---+
//    |name|age|
//    +----+---+
//    |Andy| 32|
//    | Tom| 12|
//    +----+---+
//


    spark.stop()


  }
}

