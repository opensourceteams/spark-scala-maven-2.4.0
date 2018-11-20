package com.opensource.bigdata.spark.local.rdd.operation.action.sortby.n_02_降序排序

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object TakeRun extends BaseScalaSparkContext{



  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array("A","V","B","V","C","V","W"),2)
    val r2= r1.sortBy(x => x,false)

    println(s"r2结果:"+ r2.collect().mkString(" "))



    sc.stop()
  }


}
