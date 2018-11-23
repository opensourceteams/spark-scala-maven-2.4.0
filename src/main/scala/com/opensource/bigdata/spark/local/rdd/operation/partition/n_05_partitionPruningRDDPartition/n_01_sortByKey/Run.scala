package com.opensource.bigdata.spark.local.rdd.operation.partition.n_05_partitionPruningRDDPartition.n_01_sortByKey

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List(("a",2),("d",1),("b",8),("d",3)),2)  //ParallelCollectionRDD
    val rdd1Sort = rdd1.sortByKey()   //ShuffleRDD

    println("rdd1Sort \n" + rdd1Sort.collect().mkString("\n"))


    sc.stop()
  }

}
