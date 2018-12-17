package com.opensource.bigdata.spark.sql.n_05_spark_json.n_04_json_read_as_class_foreach_null

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    import spark.implicits._




    //删掉空行数据
    spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")
      .na.drop.as[Person]foreach(x => println(s"姓名:${x.name}\t 年龄:${x.age}"))


    //    姓名:Andy	 年龄:30
//    姓名:Justin	 年龄:19
//    姓名:Think	 年龄:30
//    姓名:Think	 年龄:35
//
//
//
//












    spark.stop()
  }

}
