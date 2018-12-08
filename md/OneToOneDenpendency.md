# Spark OneToOneDependency 一对一依赖关系

-  Represents a one-to-one dependency between partitions of the parent and child RDDs.


## 更多资源
- SPARK 源码分析技术分享(bilibilid视频汇总套装视频): https://www.bilibili.com/video/av37442139/
- github: https://github.com/opensourceteams/spark-scala-maven
- csdn(汇总视频在线看): https://blog.csdn.net/thinktothings/article/details/84726769


## 视频演示
  - https://youtu.be/Tohv00GJ5AQ(youtube视频)
  - https://www.bilibili.com/video/av37442139/(bilibili视频)

<iframe   width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=65822237&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


  
## 输入数据

```shell
a bc
a  
```

## 处理程序scala
```scala
package com.opensource.bigdata.spark.local.rdd.operation.dependency.narrow.n_02_RangeDependency

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run1 extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val rdd1 = sc.textFile("/opt/data/2/c.txt",2)

    println(rdd1.collect().mkString("\n"))

    //rdd1.partitions(0).asInstanceOf[org.apache.spark.rdd.HadoopPartition]

    sc.stop()
  }

}
```

## 数据处理图
[![一对一依赖关系](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/oneToOneDenpendency_2.png "一对一依赖关系")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/oneToOneDenpendency_2.png "一对一依赖关系")



