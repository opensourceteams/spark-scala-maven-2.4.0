# Spark PruneDependency 依赖关系 Filter

-   Represents a dependency between the PartitionPruningRDD and its parent. In this
  case, the child RDD contains a subset of partitions of the parents'.


## 更多资源
- SPARK 源码分析技术分享(bilibilid视频汇总套装视频): https://www.bilibili.com/video/av37442139/
- github: https://github.com/opensourceteams/spark-scala-maven
- csdn(汇总视频在线看): https://blog.csdn.net/thinktothings/article/details/84726769

## youtub视频演示
  - https://youtu.be/5ZCNiEhO_Qg (youtube视频)
  - https://www.bilibili.com/video/av37442139/?p=3 (bilibili)
  
<iframe src="//player.bilibili.com/player.html?aid=37442139&cid=65822402&page=3" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

  
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
