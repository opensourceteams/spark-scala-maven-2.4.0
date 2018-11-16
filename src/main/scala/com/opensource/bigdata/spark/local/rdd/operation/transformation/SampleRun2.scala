package com.opensource.bigdata.spark.local.rdd.operation.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 样例数据
  */
object SampleRun2 {

  var appName = "worldcount-3"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    println("==========开始了")
    val sc = pre()
    val r1 = sc.parallelize(getArrayDataArray(100),2)
    val r3 = r1.sample(false,0.1)
    //val r2 = r1.distinct().collect().mkString

    val result = r1.collect()
    println(s"原始数据:${result.mkString}  长度:${result.length}" )
    println(s"结果:${r3.collect().mkString}  长度:${r3.collect().length}" )



    sc.stop()
  }
  def getArrayDataArray(length:Int):Array[String] = {
    val dataArray = Array("a","b","c")
    val array:Array[String] = new Array[String](length);
    for(i <- 0 until length){
      val random = Random.nextInt(dataArray.length )
      array(i) = dataArray(random)
    }
    array
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
