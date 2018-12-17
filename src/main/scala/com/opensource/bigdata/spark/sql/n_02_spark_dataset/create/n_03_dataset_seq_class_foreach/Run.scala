package com.opensource.bigdata.spark.sql.n_02_spark_dataset.create.n_03_dataset_seq_class_foreach

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {


    val spark = sparkSession()



    Seq(Person("Andy", 32) ,Person("Tom", 12)).foreach(people => println(s"name:${people.name},age:${people.age}"))


//    name:Andy,age:32
//    name:Tom,age:12

//


    spark.stop()


  }
}

