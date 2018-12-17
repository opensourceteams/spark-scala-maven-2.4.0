package com.opensource.bigdata.spark.sql.n_02_spark_dataset.action.n_07_dataset_foreach_class

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run1 extends BaseSparkSession{

  case class Person(name: String, age: Long)


  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    import spark.implicits._
    spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
      .map(line => Person(line.split(",")(0),line.split(" ")(1).trim.toLong))
        .foreach( person => println(s"name:${person.name}\t age:${person.age}"))

    spark.stop()


  }
}

