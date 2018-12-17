package com.opensource.bigdata.spark.sql.n_02_spark_dataset.user_defined_untype_aggregation.n_01_uduta_MyAverage

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Encoder, Encoders}

object Run extends BaseSparkSession{

  case class Employee(name: String, salary: Long)
  case class Average(var sum: Long, var count: Long)


  object MyAverage extends UserDefinedAggregateFunction{




  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    val ds = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")
    ds.show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//    |  30|   Andy|
//    |  19| Justin|
//    +----+-------+

    val averageColumn = MyAverage.toColumn.name("平均值")
    ds.select(averageColumn).show()





    spark.stop()
  }

}
