package com.opensource.bigdata.spark.local.rdd.operation.dependency.narrow.n_02_RangeDependency

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run3 extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val rdd1 = sc.textFile("/opt/data/2/c.txt",2)
    val rdd2 = sc.textFile("/opt/data/2/a.txt",2)
    val rdd3 = rdd1.union(rdd2)

    println(rdd3.collect().mkString("\n"))

    sc.stop()
  }

}
