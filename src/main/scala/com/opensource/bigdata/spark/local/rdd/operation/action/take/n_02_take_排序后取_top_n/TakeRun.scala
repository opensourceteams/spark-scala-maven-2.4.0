package com.opensource.bigdata.spark.local.rdd.operation.action.take.n_02_take_排序后取_top_n

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object TakeRun extends BaseScalaSparkContext{

  var appName = "worldcount-3"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array("A","V","B","V","C","V","W"),2)
    val r2= r1.sortBy(x => x,true)
    val r3 = r2.take(2).mkString

    println(s"r2结果:"+ r2.collect().mkString(" "))
    println("结果:"+ r3)



    sc.stop()
  }


}
