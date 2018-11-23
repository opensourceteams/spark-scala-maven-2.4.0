package com.opensource.bigdata.spark.local.rdd.operation.partition.n_05_partitionPruningRDDPartition.n_02_filterByRange

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List(("a",2),("d",1),("b",8),("d",3)),2)  //ParallelCollectionRDD
    val rdd1Sort = rdd1.sortByKey()   //ShuffleRDD
    val rdd2 =rdd1Sort.filterByRange("a","b")  //MapParttionsRDD

    println("rdd1Sort \n" + rdd1Sort.collect().mkString("\n"))
    println("rdd2 \n" + rdd2.collect().mkString("\n"))

    sc.stop()
  }

}
