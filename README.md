
# SPARK 2.4.0 学习笔记分享
- 【本站点正在持续更新中......2018-12-28......】
- 微博: https://weibo.com/thinktothings
- SPARK 2.4.0 学习笔记分享(bilibili整套视频): https://www.bilibili.com/video/av38193405/

## 更多资源
- SPARK 1.6.0-cdh5.15.0 源码分析: https://github.com/opensourceteams/spark-scala-maven

## Spark2.4.0源码分析时序图
- (starUML): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/module/mdj/Spark2.4.0%E6%BA%90%E7%A0%81%E5%88%86%E6%9E%90%E6%97%B6%E5%BA%8F%E5%9B%BE.mdj

## 前置条件
- Hadoop版本: hadoop-2.9.2
- Spark版本: spark-2.4.0-bin-hadoop2.7
- Hive版本: apache-hive-3.1.1-bin
- JDK.1.8.0_191
- scala2.11.12



## Spark 环境配置

### 大数据开发工具介绍
   - 大数据开发工具介绍(文档说明):  https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/tool.md

#### Spark 2.4.0 standalone 模式安装
   - Spark 2.4.0 standalone 模式安装（详细说明文档)： https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/standaloneInstall.md


## Spark 2.4.0 编程指南

### 快速入门(Quick Start)
- a quick introduction to the Spark API; start here!
- 快速介绍Spark API;从这里开始

#### Spark 2.4.0 编程指南--快速入门    
- Spark 2.4.0 编程指南--快速入门(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/quick-start.md
- Spark 2.4.0 编程指南--快速入门(bilibili视频) : https://www.bilibili.com/video/av38193405/?p=2

<iframe width="800" height="500" src="//player.bilibili.com/player.html?aid=38193405&cid=67137841&page=2" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

### Spark SQL, Datasets, and DataFrames
- processing structured data with relational queries (newer API than RDDs)
- 使用关系查询处理结构化数据（比RDD更新的API）

####  Spark 2.4.0编程指南--spark sql入门
- Spark 2.4.0编程指南--spark dataSet action(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/spark-sql-dataSet-action.md
- Spark 2.4.0编程指南--spark dataSet action(bilibili视频) : https://www.bilibili.com/video/av38193405/?p=3

<iframe width="800" height="500" src="//player.bilibili.com/player.html?aid=38193405&cid=67137841&page=3" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


####  Spark 2.4.0编程指南--Spark SQL UDF和UDAF
- Spark 2.4.0编程指南--Spark SQL UDF和UDAF(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/spark-sql-UDF-UDAF.md

####  Spark 2.4.0 集成Hive 2.3.4
- Spark 2.4.0 集成Hive 2.3.4(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/SparkAndHive.md
        
####  Spark 2.4.0编程指南--Spark DataSources
-  Spark 2.4.0编程指南--Spark DataSources(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/spark-data-source.md
   

## Spark 2.4.0 源码分析(建设中)

#### Spark2.4.0 Dataset head 源码分析
- Spark2.4.0 Dataset head 源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/Dataset-head.md
<iframe width="800" height="500" src="//player.bilibili.com/player.html?aid=38193405&cid=68636905&page=6" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
<iframe width="800" height="500" src="//player.bilibili.com/player.html?aid=38193405&cid=68636905&page=7" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

#### Spark2.4.0 SparkEnv 源码分析
- Spark2.4.0 SparkEnv 源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/SparkEnv.md

#### Spark2.4.0 SparkContext 源码分析
- Spark2.4.0 SparkContext 源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/SparkContext.md

#### Spark2.4.0 SparkSession 源码分析
- Spark2.4.0 SparkSession 源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/SparkSession.md


#### Spark2.4.0 QueryExecution 源码分析
- LogicalPlan => analyzed => optimizedPlan => sparkPlan => executedPlan

- Spark2.4.0 QueryExecution(LogicalPlan) 源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/QueryExecution-LogicalPlan.md
- Spark2.4.0 QueryExecution(OptimizedPlan) 源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/QueryExecution-OptimizedPlan.md
- Spark2.4.0 QueryExecution(SparkPlan) 源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/QueryExecution-SparkPlan.md
- Spark2.4.0 QueryExecution(ExecutedPlan) 源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/QueryExecution-ExecutedPlan.md


#### Spark2.4.0 Spark2.4.0源码分析之 Dataset.count
- Spark2.4.0 Spark2.4.0源码分析之 Dataset.count FinalRDD构建(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/Dataset-count-rdd-build.md
-  Spark2.4.0源码分析之Dataset.count 作业提交源码分析(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/Dataset-count-job-handler.md


### Spark2.4.0 WorldCount 源码分析
- Spark2.4.0源码分析之WorldCount FinalRdd构建(一)(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/worldCount/Dataset-WorldCount-FinalRDD-build.md
- Spark2.4.0源码分析之WorldCount 触发作业提交(二)(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/worldCount/Dataset-WorldCount-trigger-job-submit.md
- Spark2.4.0源码分析之WorldCount 事件循环处理器(三)(文档说明): https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/sql/dataset/worldCount/DAGSchedulerEventProcessLoop.md