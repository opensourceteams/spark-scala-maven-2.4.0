# OneToOneDependency
## youtub视频演示
  - https://youtu.be/Tohv00GJ5AQ
  
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
[![一对一依赖关系](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/oneToOneDenpendency.png "一对一依赖关系")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/rdd.denpendency/oneToOneDenpendency.png "一对一依赖关系")



