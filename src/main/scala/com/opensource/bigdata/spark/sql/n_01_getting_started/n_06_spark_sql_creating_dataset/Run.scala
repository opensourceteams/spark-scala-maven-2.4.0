package com.opensource.bigdata.spark.sql.n_01_getting_started.n_06_spark_sql_creating_dataset

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    import  spark.implicits._


    val caseClassDS = Seq(Person("小明",5),Person("小军",15)).toDS()
    caseClassDS.show()


    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    primitiveDS.map(_ + 1).show()


    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")
    val dfPerson = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json").as[Person]

    df.show()
    dfPerson.show()




    spark.stop()
  }

}
