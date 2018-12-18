package com.opensource.bigdata.spark.sql.n_08_spark_udaf.n_02_spark_udaf_max

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * ).initialize()方法,初使使，即没数据时的值
  * ).update() 方法把每一行的数据进行计算，放到缓冲对象中
  * ).merge() 把每个分区，缓冲对象进行合并
  * ).evaluate()计算结果表达式，把缓冲对象中的数据进行最终计算
  */
object Run extends BaseSparkSession{



  object CustomerMax extends UserDefinedAggregateFunction{

    //聚合函数的输入参数数据类型
    def inputSchema: StructType = {
      StructType(StructField("inputColumn",LongType) :: Nil)
    }

    //中间缓存的数据类型
    def bufferSchema: StructType = {
      StructType(StructField("sum",LongType) :: StructField("count",LongType) :: Nil)
    }

    //最终输出结果的数据类型
    def dataType: DataType = DoubleType

    def deterministic: Boolean = true

    //初始值，要是DataSet没有数据，就返回该值
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
    }


    /**
      *
      * @param buffer  相当于把当前分区的，每行数据都需要进行计算，计算的结果保存到buffer中
      * @param input
      */
    def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
      if(!input.isNullAt(0)){
        if(input.getLong(0) > buffer.getLong(0)){
          buffer(0) = input.getLong(0)
        }
      }
    }

    /**
      * 相当于把每个分区的数据进行汇总
      * @param buffer1  分区一的数据
      * @param buffer2  分区二的数据
      */
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit={
      if( buffer2.getLong(0) >  buffer1.getLong(0)) buffer1(0) = buffer2.getLong(0)
    }


    //计算最终的结果
    def evaluate(buffer: Row): Double = buffer.getLong(0)


  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    spark.udf.register("customerMax",CustomerMax)

    val df = spark.read.json("hdfs://standalone.com:9000/home/liuwen/data/employees.json")
    df.createOrReplaceTempView("employees")
    val sqlDF = spark.sql("select customerMax(salary)  as average_salary from employees  ")

    df.show
//    +-------+------+
//    |   name|salary|
//    +-------+------+
//    |Michael|  3000|
//    |   Andy|  4500|
//    | Justin|  3500|
//    |  Berta|  4000|
//    +-------+------+

    sqlDF.show()


//    +--------------+
//    |average_salary|
//    +--------------+
//    |        4500.0|
//    +--------------+

    spark.stop()
  }

}
