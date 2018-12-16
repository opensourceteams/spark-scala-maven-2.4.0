package com.opensource.bigdata.spark.sql.n_01_getting_started.n_03_spark_sql_untyped_dataset_operations

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    import spark.implicits._

    //val df = spark.read.json("file:///opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/src/main/resource/people.json")
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")

    //打应有哪些列(类型)
    df.printSchema()

    //显示name列的数据
    df.select("name").show()


    //显示name列,age列的数据
    df.select("name", "age").show()


    //显示name列,age列的数据,支持对列的表达式处理,进行计算
    df.selectExpr("name", "age +1" ).show()


    println("======filter")

    df.filter("age >21").show
    df.filter($"age" > 21).show

    println("======where")
    df.where("age >21").show
    df.where($"age" >21).show



    println("======groupBy")

    df.groupBy("age").count().show()


    println("======show")

    df.show()



    spark.stop()
  }

}
