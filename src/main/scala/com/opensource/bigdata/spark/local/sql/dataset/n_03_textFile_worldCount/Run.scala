package com.opensource.bigdata.spark.local.sql.dataset.n_03_textFile_worldCount

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true,false,false,7)
    val dataSet = spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/line.txt")
    import spark.implicits._
    val result = dataSet.flatMap( line => line.split(" ")).groupByKey(x => x ).count()

    val str = result.collect().mkString("\n")
    println(s"结果:${str}")




    spark.stop()
  }

}
