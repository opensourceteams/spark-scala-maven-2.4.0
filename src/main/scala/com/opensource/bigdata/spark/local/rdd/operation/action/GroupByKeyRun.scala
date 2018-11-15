package com.opensource.bigdata.spark.local.rdd.operation.action

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyRun {

  var appName = "worldcount-3"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array(("A",1),("B",1),("A",1),("C",1)),2)

    val r3 = r1.groupBy( a => a._1  )
    //val r4 = r3.map(x => (x._1,x._2))
    val r4 = r3.flatMap(x => x._1)

    println("结果r3:"+ r3.collect().mkString)
    println("结果r4:"+ r4.collect().mkString)




    sc.stop()
  }

  def pre(): SparkContext ={
    var startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.history.fs.logDirectory","/opt/bigdata/spark-1.6.0-cdh5.15.0/rundata/historyEventLog")
    conf.set("spark.eventLog.dir","/home/spark/log/eventLog")
    conf.setJars(Array("D:\\workspaces\\bigdata\\spark-scala-maven\\target\\spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)
    sc
  }
}
