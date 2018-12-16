package com.opensource.bigdata.spark.sql.n_01_getting_started.n_05_spark_sql_GlobalTempView

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    //val df = spark.read.json("file:///opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/src/main/resource/people.json")
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")


    df.createGlobalTempView("people")

    val sqlDF = spark.sql("select * from global_temp.people")
    val sqlDF2 = spark.sql("select name,age from global_temp.people")
    val sqlDF3 = spark.sql("select name,(age + 1) as age from global_temp.people")



    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:global_temp_view$

    sqlDF.show()
    sqlDF2.show()
    sqlDF3.show()


    spark.stop()
  }

}
