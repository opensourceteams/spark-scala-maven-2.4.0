# Spark ShuffleDependency Shuffle依赖关系

-    Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,the RDD is transient since we don't need it on the executor side.

## 更多资源
- SPARK 源码分析技术分享(bilibilid视频汇总套装视频): https://www.bilibili.com/video/av37442139/
- github: https://github.com/opensourceteams/spark-scala-maven
- csdn(汇总视频在线看): https://blog.csdn.net/thinktothings/article/details/84726769


## youtub视频演示
  - https://youtu.be/8T6PyHuf_wQ  (youtube视频)
  - https://www.bilibili.com/video/av37442139/?p=5  (bilibili视频)
  - github: https://github.com/opensourceteams/spark-scala-maven

<iframe src="//player.bilibili.com/player.html?aid=37442139&cid=65822917&page=5" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
  
## 输入数据

```shell
List(('c',1),('b',1),('a',1),('a',1)
```


## 处理程序scala
```scala

package com.opensource.bigdata.spark.local.rdd.operation.dependency.shuffle.n_01_ShuffleDependency

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List(('c',1),('b',1),('a',1),('a',1)),2)
    val rdd2 =rdd1.reduceByKey((a,b) => a + b)



    println("rdd2\n" + rdd2.collect().mkString("\n"))

    sc.stop()
  }

}


```

## 数据处理图



[![Shuffle依赖关系](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/ShuffleDependency.md "Shuffle依赖关系")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/ShuffleDependency.md "Shuffle依赖关系")
