package com.opensource.bigdata.spark.local.rdd.operation.dependency.shuffle.n_01_ShuffleDependency

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List(('c',1),('b',1),('a',1),('a',1)),2)
    val rdd2 =rdd1.reduceByKey((a,b) => a + b)



    println("rdd2\n" + rdd2.collect().mkString("\n"))

    sc.stop()
  }

}
