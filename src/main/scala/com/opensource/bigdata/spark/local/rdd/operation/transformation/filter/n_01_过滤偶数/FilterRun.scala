package com.opensource.bigdata.spark.local.rdd.operation.transformation.filter.n_01_过滤偶数

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object FilterRun extends BaseScalaSparkContext{

  var appName = "worldcount-4"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array(1,3,5,6,10),2)
    val r2 = r1.filter(_ % 2 == 0 ).collect().mkString(" ")

    println("结果:"+ r2)



    sc.stop()
  }


}
