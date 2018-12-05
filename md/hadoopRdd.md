### hadoopRDD源码分析

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven


#### 视频
- [HadoopRdd源码分析-读取本地文件需求分析-01](https://youtu.be/PtNo5S3g3zc "HadoopRdd源码分析-读取本地文件需求分析-01") 
- [HadoopRDD源码分析-文件拆分partition划分-02](https://youtu.be/kesUJxGBWFA "HadoopRDD源码分析-文件拆分partition划分-02")
- [HadoopRdd源码分析 本地文件读取源码分析 03](https://youtu.be/EuNaoJhK-x4 "HadoopRdd源码分析 本地文件读取源码分析 03")
- [HadoopRdd源码分析 本地文件读取源码分析 04](https://youtu.be/GcPi9b-iltE "HadoopRdd源码分析 本地文件读取源码分析 04")






[程序处理]
-------------------------
```scala
  def main(args: Array[String]): Unit = {
    val sc = pre()
    val rdd1 = sc.textFile("/opt/data/line.txt",3)
    val rdd2 = rdd1.map( x => x + s"(${x.length})" )

    println(rdd2.collect().mkString("\n"))

    sc.stop()
  }
```
-------------------------
[输入数据] line.txt
-------------------------
    a
    bb
    ccc
-------------------------

## [输出数据]

    a(1)
    bb(2)
    ccc(3)

-------------------------

## [问题分析]

).原数据文件line.txt 文件 8 Byte的数据,分成4个任务后，输入数据变成20 Byte,为什么？
).partition 划分，数据拆分，指定minPartition=3，实际为什么划分为4个partition
).[设计缺陷] 输入数据重复读取
).[代码混乱] org.apache.hadoop.util.LineReader.readDefaultLine ,不同情况读取文件数据内容逻辑放在一起，容易混乱
).每个任务实际读取了哪些数据(为什么有的任务输出数据为0)
  文件拆分算法
  totalSize:文件的总大小
  numSplits:客户端指定的 minPartition 数
  goalSize: 每个分片的数据大小 
  long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);







