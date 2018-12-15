package com.opensource.bigdata.spark.standalone.wordcount.spark.session


import org.apache.spark.sql.SparkSession



object WorldCount {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("spark://standalone.com:7077")
      .appName("SparkSessionWordCount")
      .config("spark.eventLog.enabled","true")
      .config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
      .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")
      .getOrCreate()

    // ()

    spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
    import spark.implicits._

    val distFile = spark.read.textFile("/home/liuwen/data/a.txt")

    val rdd = distFile.flatMap( line => line.split(" ")).groupByKey(identity).count()


    println("=====")
    println(rdd.first())
    println(rdd.show())


    spark.stop()


  }
}

