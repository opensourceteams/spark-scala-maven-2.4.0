package com.opensource.bigdata.spark.local.rdd.operation.action.fold.n_03_rdd_fold

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run extends BaseScalaSparkContext{


  def main(args: Array[String]): Unit = {
    val sc = pre()

    /**
      * numSlices:1   DDABC => (DA  -> DAB -> DABC) -> DDABC
      * numSlices:2   DDADBC  => (DA) ->
      *                          (DB -> DBC)
      *                          D DA DBC
      * numSlices:3   DDADBDC =>
      *                          (DA) ->
      *                          (DB) ->
      *                          (DC) ->
      *                          D DA DB DC
      *
      * numSlices:4   DDDADBDC   =>
      *                        (DA) ->
      *                        (DB) ->
      *                        (DC) ->
      *                        DD DA DB DC
      * numSlices:5  DDDADDBDC   =>
      * *                        (DA) ->
      * *                        (DB) ->
      * *                        (DC) ->
      * *                        DD DDDA  DDDAD ->  DDDADDB -> DDDADDBDC
      *numSlices:6  DDDADDBDDC
      */
    val r1 = sc.parallelize(Array("A","B","C"),5)
    val r3 = r1.fold("D" )((x,y) => {println(s"\n\n"+Thread.currentThread().getName+s"  x:${x} + y:${y} =" +(x +y)) ;x +y}) ;

    println("结果:"+ r3)



    sc.stop()
  }

}
