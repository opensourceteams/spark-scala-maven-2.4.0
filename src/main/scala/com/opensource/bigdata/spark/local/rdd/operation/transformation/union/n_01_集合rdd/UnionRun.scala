package com.opensource.bigdata.spark.local.rdd.operation.transformation.union.n_01_集合rdd

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext


/**
  * 并集
  */
object UnionRun  extends BaseScalaSparkContext{



  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array("A","B"),2)
    val r2 = sc.parallelize(Array("C","D"),2)
    //val r3 = r1.distinct().union(r2.distinct())
    val r3 = r1.union(r2)

    r3.foreach(x => println(s"行:${x}"))




    sc.stop()
  }


}
