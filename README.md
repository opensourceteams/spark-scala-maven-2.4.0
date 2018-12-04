# SPARK 源码分析技术分享 (带视频)
【本站点正在持续更新中......】

SPARK 1.6.0-cdh5.15.0
Hadoop 2.6.0-cdh5.15.0
spark-scala-maven 
微信(技术交流) : thinktothings



## Youtub 视频
- [HadoopRdd源码分析-读取本地文件需求分析-01](https://youtu.be/PtNo5S3g3zc "HadoopRdd源码分析-读取本地文件需求分析-01") 
- [HadoopRDD源码分析-文件拆分partition划分-02](https://youtu.be/kesUJxGBWFA "HadoopRDD源码分析-文件拆分partition划分-02")
- [HadoopRdd源码分析 本地文件读取源码分析 03](https://youtu.be/EuNaoJhK-x4 "HadoopRdd源码分析 本地文件读取源码分析 03")
- [HadoopRdd源码分析 本地文件读取源码分析 04](https://youtu.be/GcPi9b-iltE "HadoopRdd源码分析 本地文件读取源码分析 04")

### SparkContext 分析
 - youtube 视频（SparkContext原理分析）: https://youtu.be/euIuutjAB4I
 - Youtub  视频 (Spark源码分析详解):  https://youtu.be/tUH7QnCcwgg
 - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/SparkContext.md

### Spark 通信原理分析
 - Youtub  视频 (Spark通信原理分析):  https://youtu.be/3vUVwbEGf1E
 - 详细说明文档： https://thinktothings.gitbook.io/spark/yuan-ma-fen-xi/spark-tong-xin-fa-jian-xiang-he-shou-jian-xiang


### Spark Master启动源码分析
 - Youtub  视频 (Master启动源码分析):  https://youtu.be/74q1nddoaiY​
 - Master启动源码分析详细说明文档： https://thinktothings.gitbook.io/spark/yuan-ma-fen-xi/master-yuan-ma-fen-xi​
 
 - Spark Master资源调度--worker向master注册(Youtube视频):  https://youtu.be/74q1nddoaiY​
 - Spark Master资源调试--worker向master注册(文档详解)： https://thinktothings.gitbook.io/spark/yuan-ma-fen-xi/master-zi-yuan-tiao-du-worker-xiang-master-zhu-ce

 - Spark Master资源调度--SparkContext向所有master注册(Youtube视频):  https://youtu.be/AXxCnCc5Mh0​ 
 - Spark Master资源调度--SparkContext向所有master注册(文档详解): https://thinktothings.gitbook.io/spark/yuan-ma-fen-xi/master-zi-yuan-tiao-du-sparkcontext-xiang-suo-you-master-zhu-ce-registerapplication-xiao-xi​


### Spark Worker启动源码分析
 - Spark Worker 启动源码分析(Youtube视频):   https://youtu.be/ll_Ae6rP7II​​
 - Spark Worker 启动源码分析(文档详解):  https://thinktothings.gitbook.io/spark/yuan-ma-fen-xi/worker-yuan-ma-fen-xi​
 
 
### Spark Executor启动源码分析
  - Spark Executor启动源码分析(Youtube视频):   https://youtu.be/1qg4UMPV3pQ
  - Spark Executor启动源码分析(文档详解)：  https://thinktothings.gitbook.io/spark/yuan-ma-fen-xi/coarsegrainedexecutorbackend-qi-dong-yuan-ma-fen-xi
 
 
### Spark 触发Job提交
 - Spark 触发Job提交(youtube视频) : https://youtu.be/X49RIqz2AjM
 - Spark 触发Job提交(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/jobSubmitTrigger.md
 
### Spark DAG调度器事件循环处理器
 - Spark DAG调度器事件循环处理器(Youtube视频) : https://youtu.be/fT-dpf0KFOA
 - Spark DAG调度器事件循环处理器(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/DAGSchedulerEventProcessLoop.md 
 
### Spark FinalStage处理(Stage划分)
 - Spark FinalStage处理(Stage划分)(Youtube视频) : https://youtu.be/yFJugOV0Fak
 - Spark FinalStage处理(Stage划分)(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/FinalStage.md 
 
### Spark Stage提交
 - Spark Stage提交(Youtube视频) :  https://youtu.be/NI8-_X6mbl4
 - Spark Stage提交(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/SubmitStage.md 
 




### RDD依赖 Dependency

#### NarrowDependency
- OneToOneDependency
   - youtube 视频:  https://youtu.be/Tohv00GJ5AQ
   - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/OneToOneDenpendency.md
- RangeDependency
   - youtube 视频:  https://youtu.be/_4DeWWPQubc
   - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/RangeDependency.md
- PruneDependency filter
   - youtube 视频:  https://youtu.be/5ZCNiEhO_Qg
   - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/PruneDependency.md
- PruneDependency RangePartitioner
   - youtube 视频:  https://youtu.be/YRQ6OaOXmPY
   - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/PruneDependencyRangePartitioner.md


#### ShuffleDependency
   - youtube 视频:  https://youtu.be/8T6PyHuf_wQ
   - 详细说明文档：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/ShuffleDependency.md



#### 相关链接

- [HadoopRDD源码分析说明文档详细](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/hadoopRdd.md "HadoopRDD源码分析说明文档")
