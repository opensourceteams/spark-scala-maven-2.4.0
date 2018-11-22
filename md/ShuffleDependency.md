# ShuffleDependency Shuffle依赖关系

-    Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,the RDD is transient since we don't need it on the executor side.

## youtub视频演示
  - https://youtu.be/_4DeWWPQubc
  - github: https://github.com/opensourceteams/spark-scala-maven
  
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
