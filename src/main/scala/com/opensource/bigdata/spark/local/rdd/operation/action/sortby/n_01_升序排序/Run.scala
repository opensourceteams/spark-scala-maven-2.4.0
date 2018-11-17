package com.opensource.bigdata.spark.local.rdd.operation.action.sortby.n_01_升序排序

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object TakeRun extends BaseScalaSparkContext{

  var appName = "local"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array("A","V","B","V","C","V","W"),2)
    val r2= r1.sortBy(x => x,true)

    println(s"r2结果:"+ r2.collect().mkString(" "))



    sc.stop()
  }


}
