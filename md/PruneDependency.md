# PruneDependency 依赖关系 Filter

-   Represents a dependency between the PartitionPruningRDD and its parent. In this
  case, the child RDD contains a subset of partitions of the parents'.

## youtub视频演示
  - https://youtu.be/5ZCNiEhO_Qg
  - github: https://github.com/opensourceteams/spark-scala-maven
  
## 输入数据

```shell
List(("a",2),("d",1),("b",8),("d",3)
```


## 处理程序scala
```scala
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


```

## 数据处理图



[![PruneDependency依赖关系图](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/pruneDependency%E4%BE%9D%E8%B5%96%E5%85%B3%E7%B3%BB.png "PruneDependency依赖关系图")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/pruneDependency%E4%BE%9D%E8%B5%96%E5%85%B3%E7%B3%BB.png "PruneDependency依赖关系图")
