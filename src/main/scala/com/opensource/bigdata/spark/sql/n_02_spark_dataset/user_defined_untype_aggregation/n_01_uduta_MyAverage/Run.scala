package com.opensource.bigdata.spark.sql.n_02_spark_dataset.user_defined_untype_aggregation.n_01_uduta_MyAverage

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object Run extends BaseSparkSession{



  object MyAverage extends UserDefinedAggregateFunction{

    //聚合函数的输入参数数据类型
    def inputSchema: StructType = {
      StructType(StructField("inputColumn",LongType) :: Nil)
    }

    def bufferSchema: StructType = {
      StructType(StructField("sum",LongType) :: StructField("count",LongType) :: Nil)
    }

    def dataType: DataType = DoubleType

    def deterministic: Boolean = true

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }


    def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
      if(!input.isNullAt(0)){
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit={
      buffer1(0) = buffer1.getLong(0) +buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) +buffer2.getLong(1)
    }


    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)


  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    spark.udf.register("MyAverage",MyAverage)

    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")
    df.createOrReplaceTempView("employees")
    val sqlDF = spark.sql("select MyAverage(salary)  as average_slary from employees  ")

    sqlDF.show()

    spark.stop()
  }

}
