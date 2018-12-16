package com.opensource.bigdata.spark.sql.dataset.action.n_15_dataset_show_truncate_vertical

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.txt")

    /**
      * 以表格的形式显示前10行数据
      *
      * @param numRows Number of rows to show
      * @param truncate If set to more than 0, truncates strings to `truncate` characters and
      *                    all cells will be aligned right.
      * @param vertical If set to true, prints output rows vertically (one line per column value).
      */

    val result = dataSet.show(10,10,false)
    println(result)





    spark.stop()


  }
}

