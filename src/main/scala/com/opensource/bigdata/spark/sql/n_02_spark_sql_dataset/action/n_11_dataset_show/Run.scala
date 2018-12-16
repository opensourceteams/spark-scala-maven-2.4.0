package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.action.n_11_dataset_show

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.cn.txt")

    /**
      * 以表格的形式显示前3行数据
      * numRows是显示前几行的数据
      */

    val result = dataSet.show()
    println(result)





    spark.stop()


  }
}

