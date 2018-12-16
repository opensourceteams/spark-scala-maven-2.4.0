package com.opensource.bigdata.spark.standalone.wordcount.spark.session


import com.opensource.bigdata.spark.standalone.base.BaseSparkSession



object WorldCount2 extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession(true)
    import spark.implicits._

    val distFile = spark.read.textFile("hdfs://standalone.com:9000/home/liuwen/data/word.txt")

    //方式一
    //val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(x => x ).count()


    //方式二
    val dataset = distFile.flatMap( line => line.split(" ")).map(x => (x,1)).groupByKey(x => x).reduceGroups((a,b) => (a._1,a._2+b._2))

    //方式三
    //val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(identity ).count()

    dataset.show()






    spark.stop()


  }
}

