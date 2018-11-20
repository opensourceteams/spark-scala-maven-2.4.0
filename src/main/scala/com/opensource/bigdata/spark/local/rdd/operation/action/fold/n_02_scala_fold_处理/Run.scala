package com.opensource.bigdata.spark.local.rdd.operation.action.fold.n_02_scala_fold_处理

object Run {

  def main(args: Array[String]): Unit = {
    val map = Map("a" -> 1, "b" -> 2,"c" -> 3,"f" -> 5)

    /**
      *
      */
    val result = map.fold("d" -> 4)((a,b) => (a._1 + b._1  ,a._2 + b._2))
    println(map.mkString(" "))
    println("结果")
    println(s"${result}")
  }
}
