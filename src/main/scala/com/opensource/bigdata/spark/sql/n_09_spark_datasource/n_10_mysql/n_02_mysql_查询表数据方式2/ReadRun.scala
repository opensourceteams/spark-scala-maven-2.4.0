package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_10_mysql.n_02_mysql_查询表数据方式2

import java.util.Properties

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRun  extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)



    val connectionProperties = new Properties()
    connectionProperties.put("user","admin")
    connectionProperties.put("password","000000")

    val jdbcDF = spark.read.jdbc("jdbc:mysql://mysql.com:3306/test","test.test2",connectionProperties)

    jdbcDF.show()



    spark.stop()
  }

}
