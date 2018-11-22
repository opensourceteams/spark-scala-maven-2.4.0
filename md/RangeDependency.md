# RangeDependency 区间依赖关系

-  Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.

## youtub视频演示
  - https://youtu.be/_4DeWWPQubc
  - github: https://github.com/opensourceteams/spark-scala-maven
  
## 输入数据
c.txt
```shell
a bc
a  
```
a.txt
```shell
a b
c a

```
## 处理程序scala
```scala
package com.opensource.bigdata.spark.local.rdd.operation.dependency.narrow.n_02_RangeDependency

import com.opensource.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run3 extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val rdd1 = sc.textFile("/opt/data/2/c.txt",2)
    val rdd2 = sc.textFile("/opt/data/2/a.txt",2)
    val rdd3 = rdd1.union(rdd2)

    println(rdd3.collect().mkString("\n"))

    sc.stop()
  }

}

```

## 数据处理图
[![RangeDependency依赖关系图](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/RangeDependency-%E5%8C%BA%E9%97%B4%E4%BE%9D%E8%B5%96.png "RangeDependency依赖关系图")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/RangeDependency-%E5%8C%BA%E9%97%B4%E4%BE%9D%E8%B5%96.png "RangeDependency依赖关系图")


