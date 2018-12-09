# Hadoop RDD 读取文件分析

## HDFS数据文件

```shell
a b k l j
c a n m o
```

## HDFS 数据文件图解

[![](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HDFS%E6%95%B0%E6%8D%AE%E6%96%87%E4%BB%B6%E5%9B%BE%E8%A7%A3.png)](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HDFS%E6%95%B0%E6%8D%AE%E6%96%87%E4%BB%B6%E5%9B%BE%E8%A7%A3.png)


## HadoopRDD partition划分原理
- 每个partition的长度= 文件的总长度 / 最小的分区数(默认分区数为2)  //注意，是除，结果会取整, 即 goalSize = totalSize / numSplits
- 示例中每个partition的长度 = 20 / 2 =10  // 即为10个byte
- 然后依次从0开始划分10个byte长度为一个partition,最后一个小于等于10个byte的为最后一个partition
- 所以  parition(0) = hdfs文件(0 + 10) //即从文件偏移量为0开始，共10byte,0 <= 值 < 10
- 所以  parition(1) = hdfs文件(10 + 10) //即从文件偏移量为10开始，共10byte,10 <= 值 < 20
