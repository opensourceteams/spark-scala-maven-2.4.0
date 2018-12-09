# Hadoop RDD partition划分图解

## HDFS数据文件

```shell
a b k l j
c a n m o
```

## HDFS 数据文件图解

[![](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_1.png)](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_1.png)

## HDFS 数据文件图解(对比）
### 图一
[![](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_2.png)](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_2.png)
### 图二
[![](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_3.png)](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/example/HadoopRdd/HadoopRddPartitionDivide_3.png)

## HadoopRDD partition预划分方式(实际会有小的调整)
- 每个partition的长度= 文件的总长度 / 最小的分区数(默认分区数为2)  //注意，是除，结果会取整, 即 goalSize = totalSize / numSplits
- 示例中每个partition的长度 = 20 / 2 =10  // 即为10个byte
- 然后依次从0开始划分10个byte长度为一个partition,最后一个小于等于10个byte的为最后一个partition
- 所以  parition(0) = hdfs文件(0 + 10) //即从文件偏移量为0开始，共10byte,0 <= 值 < 10
- 所以  parition(1) = hdfs文件(10 + 10) //即从文件偏移量为10开始，共10byte,10 <= 值 < 20
- 即 partition(i) = hdfs文件( i * goalSize + 10 )

## HadoopRDD partition划分原理
- 由于需要考虑，每个partition谁先执行是不确定的，所以每个partition执行时，都需要可明确计算当前partition的数据范围
- 由于直接按partition预划分方式，会把有的一行数据拆分，有些场景不适合
- 所以需要按行做为最小的数据划分单元，来进行partition的数据范围划分
- HadoopRDD是这样划分的partition,还是按partition预划分方式进行预先划分，不过在计算时会进行调整
- 对于首个partition,也就是partition(0),分区数据范围的开始位置就是从0开始(0 + goalSize )
- 对于非首个partition，的开始位置需要从新计算，从预划分的当前partition的开始位置开始找第一个换行符位置(indexNewLine),当前partition的开始位置为= indexNewLine + 1,长度还是goalSize
- 对于首个partition一定能分到数据(只要HDFS文件有数据)
- 非首个partition,有可能分不到数据的情况，分不到数据的情况，就是数据被上一个partition划分完了，即当前partition的第一个换行符
