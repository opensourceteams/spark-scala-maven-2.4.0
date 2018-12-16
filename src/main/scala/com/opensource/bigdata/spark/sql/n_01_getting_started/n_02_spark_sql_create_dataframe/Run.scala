package com.opensource.bigdata.spark.sql.n_01_getting_started.n_02_spark_sql_create_dataframe

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //val df = spark.read.json("file:///opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/src/main/resource/people.json")
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/people.json")
    df.show()



    spark.stop()
  }

}
