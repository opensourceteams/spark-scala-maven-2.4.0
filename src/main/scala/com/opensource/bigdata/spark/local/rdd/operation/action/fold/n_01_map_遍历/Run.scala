package com.opensource.bigdata.spark.local.rdd.operation.action.fold.n_01_map_遍历

object Run {

  def main(args: Array[String]): Unit = {
    val map = Map("a" -> 1, "b" -> 2,"c" -> 3)

    println(map.mkString(" "))
  }
}
