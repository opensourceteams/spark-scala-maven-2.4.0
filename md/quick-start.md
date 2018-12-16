# Spark 2.4.0 编程指南--快速入门

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0

## 文档
- (官网文档): http://spark.apache.org/docs/2.4.0/quick-start.html
- (英译中)(官网文档)Spark 2.4.0 编程指南(快速入门)(pdf): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/module/pdf/%E7%BC%96%E7%A8%8B%E6%8C%87%E5%8D%97%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8%20-%20Spark%202.4.0%E6%96%87%E6%A1%A3.pdf

## 前置条件
- 已安装好java(选用的是java 1.8.0_191)
- 已安装好scala(选用的是scala  2.11.121)
- 已安装好hadoop(选用的是Hadoop 3.1.1)
- 已安装好spark(选用的是spark 2.4.0)

## 技能标签
- Spark 2.4.0 Spark session available as 'spark'
- 在Spark 2.0之后，RDD被数据集(Dataset)取代 
- Spark session 读取HDFS文件做为数据集
- 数据集函数，count(),first(),filter(),reduce()
- 统计所有行单词总个数
- 计算行中最多单词的个数
- 计算最多单词个数的行
- 按单词分组统计个数(WordCount)
- 官网: http://spark.apache.org/docs/2.4.0/quick-start.html

## 示例

- Spark session 读取HDFS文件做为数据集

```
 val dataSet = spark.read.textFile("/home/liuwen/data/a.txt")
```

- 数据集调用count()函数

```
 dataSet.count()

```

- 数据集调用first()函数

```
 //其实调用的是head()函数
 dataSet.first()
```
- 数据集调用show()函数

```
 dataSet.show()  //默认取前20行数据，并进行20个字符的截断
 dataSet.show(10,false)   //取前20行数据，并且不进行截断
```

- 数据集调用filter()函数

```
 dataSet.filter(line => line.contains("spark"))
```

- 统计所有行单词总个数


```
 import spark.implicits._
val lineWordLength = dataSet.map( line => line.split(" ").size)
val result = lineWordLength.reduce((a,b) => a + b)

```

- 计算行中最多有多少个单词


```
import spark.implicits._
val lineWordLength = dataSet.map( line => line.split(" ").size)
val result = lineWordLength.reduce((a,b) => Math.max(a,b))

```

- 计算最多单词个数的行

```
import spark.implicits._
val result = dataSet.reduce((a,b) => {
  if(a.split(" ").size > b.split(" ").size) a  else b
})

```

- 按单词分组统计单词个数(WorldCount)

```
import spark.implicits._

    val distFile = spark.read.textFile("hdfs://standalone.com:9000/home/liuwen/data/word.txt")

    //方式一
    //val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(x => x ).count()


    //方式二
    val dataset = distFile.flatMap( line => line.split(" ")).map(x => (x,1)).groupByKey(x => x).reduceGroups((a,b) => (a._1,a._2+b._2))

    //方式三
    //val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(identity ).count()


```



end