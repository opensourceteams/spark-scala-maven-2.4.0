# Spark PruneDependency 依赖关系 RangePartitioner

-   Represents a dependency between the PartitionPruningRDD and its parent. In this
  case, the child RDD contains a subset of partitions of the parents'.

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven
- csdn(汇总视频在线看): https://blog.csdn.net/thinktothings/article/details/84726769


## youtub视频演示
  - https://youtu.be/YRQ6OaOXmPY  (youtube视频)
  - https://www.bilibili.com/video/av37442139/?p=4  (bilibile视频)

<iframe src="//player.bilibili.com/player.html?aid=37442139&cid=65838008&page=4" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

  
## 输入数据

```shell
List(("a",2),("d",1),("b",8),("d",3)
```


## 处理程序scala
```scala

package com.opensource.bigdata.spark.local.rdd.operation.dependency.narrow.n_03_pruneDependency.n_02_filterByRange

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List(("a",2),("d",1),("b",8),("d",3)),2)  //ParallelCollectionRDD
    val rdd1Sort = rdd1.sortByKey()   //ShuffleRDD
    val rdd2 =rdd1Sort.filterByRange("a","b")  //MapParttionsRDD

    println("rdd \n" + rdd2.collect().mkString("\n"))

    sc.stop()
  }

}


```

## 数据处理图



[![PruneDependency依赖关系图](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/pruneDependency%E4%BE%9D%E8%B5%96%E5%85%B3%E7%B3%BB.png "PruneDependency依赖关系图")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/pruneDependency%E4%BE%9D%E8%B5%96%E5%85%B3%E7%B3%BB.png "PruneDependency依赖关系图")

[![RangePartitioner范围分区器](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/RangePartitioner%E8%8C%83%E5%9B%B4%E5%88%86%E5%8C%BA%E5%99%A8.png "RangePartitioner范围分区器")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/RangePartitioner%E8%8C%83%E5%9B%B4%E5%88%86%E5%8C%BA%E5%99%A8.png "RangePartitioner范围分区器")
