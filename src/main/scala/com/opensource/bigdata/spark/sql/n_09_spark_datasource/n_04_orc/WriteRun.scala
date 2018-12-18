package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_04_orc

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object WriteRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)
    val usersDF = spark.read.load("hdfs://standalone.com:9000/home/liuwen/data/parquest/users.parquet")
    usersDF.show
    //+------+--------------+----------------+
    //|  name|favorite_color|favorite_numbers|
    //+------+--------------+----------------+
    //|Alyssa|          null|  [3, 9, 15, 20]|
    //|   Ben|           red|              []|
    //+------+--------------+----------------+
    usersDF.write.format("orc").option("orc.bloom.filter.columns", "favorite_color").option("orc.dictionary.key.threshold", "1.0").save("hdfs://standalone.com:9000/home/liuwen/output/orc/users_with_options.orc")
    spark.stop()
  }
}
// value write is not a member of Unit