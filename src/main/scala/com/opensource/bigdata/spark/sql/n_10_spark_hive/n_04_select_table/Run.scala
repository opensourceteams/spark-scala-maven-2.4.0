package com.opensource.bigdata.spark.sql.n_10_spark_hive.n_04_select_table



import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{

  appName = "查询学生"

  def main(args: Array[String]): Unit = {
    //val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = sparkSession(false,true)

    import spark.sql

    sql("use test")
    sql("select * from students").show()



    spark.stop()
  }

}
