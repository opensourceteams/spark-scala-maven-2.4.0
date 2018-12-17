package com.opensource.bigdata.spark.local.rdd.operation.transformation.union.n_02_文件rdd

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {
    val sc = pre()

    val rdd1 = sc.textFile("/opt/data/a.text",2)
    val rdd2 = sc.textFile("/opt/data/b.text",2)

    val rdd3 = rdd1.union(rdd2)

    val result = rdd3.collect().mkString(" ")

    println(s"结果: ${result}" )



    sc.stop()
  }

}
