package com.opensource.bigdata.spark.local.rdd.operation.dependency.narrow.n_03_pruneDependency.n_03_filterByRange_filter

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List(("a",2),("d",1),("b",8),("d",3)),2)  //ParallelCollectionRDD
    val rdd2 =rdd1.filterByRange("a","b")  //MapParttionsRDD

    println("rdd \n" + rdd2.collect().mkString("\n"))

    sc.stop()
  }

}
