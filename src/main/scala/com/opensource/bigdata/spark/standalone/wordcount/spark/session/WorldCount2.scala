package com.opensource.bigdata.spark.standalone.wordcount.spark.session


import com.opensource.bigdata.spark.standalone.base.BaseSparkSession



object WorldCount2 extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession(true)
    import spark.implicits._

    val distFile = spark.read.textFile("hdfs://standalone.com:9000/home/liuwen/data/a.txt")

    val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(x => x ).count()
    //val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(identity ).count()



    dataset.show()




    spark.stop()


  }
}

