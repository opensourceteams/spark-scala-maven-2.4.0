package com.opensource.bigdata.spark.local.rdd.operation.partition.n_05_partitionPruningRDDPartition

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List((1,2),(4,1),(2,8)))
    val rdd1Sort = rdd1.sortByKey()
    val rdd2 =rdd1Sort.filterByRange(1,2)

    println("rdd1Sort \n" + rdd1Sort.collect().mkString("\n"))
    println("rdd2 \n" + rdd2.collect().mkString("\n"))

    sc.stop()
  }

}
