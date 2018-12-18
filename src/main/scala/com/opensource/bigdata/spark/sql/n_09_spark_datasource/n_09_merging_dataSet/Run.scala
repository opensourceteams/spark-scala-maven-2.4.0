package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_09_merging_dataSet

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    import spark.implicits._
    val df1 = Seq(1,2,3,5).map(x => (x,x * x)).toDF("a","b")
    val df2 = Seq(10,20,30,50).map(x => (x,x * x)).toDF("a","b")

    df1.write.parquet("data/test_table/key=1")
    df1.show()
//    +---+---+
//    |  a|  b|
//    +---+---+
//    |  1|  1|
//    |  2|  4|
//    |  3|  9|
//    |  5| 25|
//    +---+---+
    df2.write.parquet("data/test_table/key=2")
    df2.show()
//    +---+----+
//    |  a|   b|
//    +---+----+
//    | 10| 100|
//    | 20| 400|
//    | 30| 900|
//    | 50|2500|
//    +---+----+

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()
//    root
//    |-- a: integer (nullable = true)
//    |-- b: integer (nullable = true)
//    |-- key: integer (nullable = true)


    mergedDF.show()

//    +---+----+---+
//    |  a|   b|key|
//    +---+----+---+
//    | 10| 100|  2|
//    | 20| 400|  2|
//    | 30| 900|  2|
//    | 50|2500|  2|
//    |  1|   1|  1|
//    |  2|   4|  1|
//    |  3|   9|  1|
//    |  5|  25|  1|
//    +---+----+---+


    spark.stop()
  }

}
