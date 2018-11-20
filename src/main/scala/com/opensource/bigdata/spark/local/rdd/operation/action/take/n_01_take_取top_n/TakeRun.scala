package com.opensource.bigdata.spark.local.rdd.operation.action.take.n_01_take_取top_n

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object TakeRun extends BaseScalaSparkContext{


  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array("A","V","B","V","C","V","W"),2)
    val r2 = r1.take(2).mkString

    println("结果:"+ r2)



    sc.stop()
  }


}
