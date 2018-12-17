package com.opensource.bigdata.spark.local.rdd.operation.dependency.narrow.n_01_OneToOneDependency

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val rdd1 = sc.textFile("/opt/data/line.text",3)
    val rdd2 = rdd1.map( x => x + s"(${x.length})" )

    println(rdd2.collect().mkString("\n"))

    sc.stop()
  }

}
