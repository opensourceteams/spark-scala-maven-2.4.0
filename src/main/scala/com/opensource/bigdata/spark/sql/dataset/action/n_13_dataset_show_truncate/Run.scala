package com.opensource.bigdata.spark.sql.dataset.action.n_13_dataset_show_truncate

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.cn.txt")

    /**
      * 以表格的形式显示前3行数据
      * numRows是显示前几行的数据
      * 默认情况是会只取结果行的前20个字符(不是20个字节)
      */

    val result = dataSet.show(10)
    println(result)





    spark.stop()


  }
}

