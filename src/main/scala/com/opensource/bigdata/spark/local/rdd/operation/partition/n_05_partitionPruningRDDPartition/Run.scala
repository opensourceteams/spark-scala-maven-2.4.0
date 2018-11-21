package com.opensource.bigdata.spark.local.rdd.operation.partition.n_05_partitionPruningRDDPartition

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List((1,2),(4,1),(2,8))).sortByKey()
    val rdd2 =rdd1.filterByRange(1,2)

    println(rdd2.collect().mkString(" "))

    sc.stop()
  }

}
