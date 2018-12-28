package com.opensource.bigdata.spark.standalone.sql.dataset.n_01_show

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run_dataset_show extends BaseSparkSession{


  appName = "Dataset textFile show"

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false,false,false)
    //返回dataFrame
    val df = spark.read.textFile("data/text/line.txt")
    df.show()



    spark.stop()
  }

}
