package com.opensource.bigdata.spark.local.rdd.operation.dependency.shuffle.n_01_ShuffleDependency

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run2  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List(('c',1),('b',1),('a',1),('a',1)),2)
    val rdd2 =rdd1.reduceByKey((a,b) => a + b)
    val rdd3 =rdd2.sortByKey()

    //rdd2.dependencies.foreach(x => x.asInstanceOf[ShuffleDependency].mapSideCombine = false)


    println("rdd3\n" + rdd3.collect().mkString("\n"))

    sc.stop()
  }

}
