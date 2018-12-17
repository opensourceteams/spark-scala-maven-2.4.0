package com.opensource.bigdata.spark.sql.n_03_spark_dataframe.n_24_dataframe_sql_map_fieldName

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    val rdd = spark.sparkContext.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")

    import spark.implicits._
    val ds = rdd.map(_.split(",")).map(attributes => Person(attributes(0),attributes(1).trim.toLong)).toDS()

    ds.createOrReplaceTempView("people")
    spark.sql("select * from people WHERE age BETWEEN 13 AND 19").map(people => "name:" + people.getAs[String]("name") + "\tage:" + people.getAs[Long]("age")).show()




//    +------------------+
//    |             value|
//    +------------------+
//    |name:Justin	age:19|
//    +------------------+
//
//
//











    spark.stop()
  }

}
