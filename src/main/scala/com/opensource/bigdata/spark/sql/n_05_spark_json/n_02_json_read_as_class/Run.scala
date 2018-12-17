package com.opensource.bigdata.spark.sql.n_05_spark_json.n_02_json_read_as_class

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    import spark.implicits._
    spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json").as[Person].show()



//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//    |  30|   Andy|
//    |  19| Justin|
//    |  30|  Think|
//    |  35|  Think|












    spark.stop()
  }

}
