package com.opensource.bigdata.spark.sql.n_02_spark_sql_dataset.create.n_01_dataset_seq_toDS

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    import spark.implicits._

    Seq(1,2,3).toDS().show()




    spark.stop()


  }
}

