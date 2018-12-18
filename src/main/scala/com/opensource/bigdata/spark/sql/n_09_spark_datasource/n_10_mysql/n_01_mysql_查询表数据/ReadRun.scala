package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_10_mysql.n_01_mysql_查询表数据

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRun  extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    val jdbcDF =spark.read.format("jdbc").option("url","jdbc:mysql://mysql.com:3306/test")
        .option("dbtable","test.test2")
        .option("user","admin")
        .option("password","000000")
        .load()

    jdbcDF.show()



    spark.stop()
  }

}
