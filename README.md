


# SPARK 源码分析技术分享 (带bilibili视频)
【本站点正在持续更新中......2018-12-08......】

- SPARK 1.6.0-cdh5.15.0
- Hadoop 2.6.0-cdh5.15.0
- spark-scala-maven 
- 微信(技术交流) : thinktothings
- SPARK 源码分析技术分享(视频汇总套装视频): https://www.bilibili.com/video/av37442139/
- SPARK 源码分析技术分享(视频汇总在线看):https://blog.csdn.net/thinktothings/article/details/84726769
- SPARK 源码分析技术分享 (github) :  https://github.com/opensourceteams/spark-scala-maven


### RDD依赖 Dependency

#### NarrowDependency
- OneToOneDependency
   - bilibili 视频:  https://www.bilibili.com/video/av37442139/?p=1
   - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/OneToOneDenpendency.md
   
   <iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=65822237&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
   
   
- RangeDependency
   - bilibili 视频:  https://www.bilibili.com/video/av37442139/?p=2 
   - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/RangeDependency.md
   
   <iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=65822237&page=2" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
   
- PruneDependency filter
   - bilibili 视频:  https://www.bilibili.com/video/av37442139/?p=3
   - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/PruneDependency.md
   
   <iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=65822237&page=3" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
   
- PruneDependency RangePartitioner
   - bilibili 视频:  https://www.bilibili.com/video/av37442139/?p=4
   - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/PruneDependencyRangePartitioner.md
   
   <iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=65822237&page=4" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


#### ShuffleDependency
   - bilibili 视频:  https://www.bilibili.com/video/av37442139/?p=5
   - 详细说明文档：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/ShuffleDependency.md
   
   <iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=65822237&page=5" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
   

### SparkContext 分析
 - SparkContext原理分析(Youtube视频): https://youtu.be/euIuutjAB4I
 - SparkContext源码分析(Youtube视频):  https://youtu.be/tUH7QnCcwgg
 - SparkContext源码分析(文档详解）： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/SparkContext.md
 
 <iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442161&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
 
 

### Spark 通信原理分析
 - Spark通信原理分析(Youtube视频):  https://youtu.be/3vUVwbEGf1E
 - Spark通信原理分析（文档详解）： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/OutBoxAndInBox.md
- bilibili : https://www.bilibili.com/video/av37442199/

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442199&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


### Spark Master启动源码分析
 - Spark Master启动源码分析(Youtube视频):  https://youtu.be/74q1nddoaiY​
 - Spark Master启动源码分析(Bilibili视频):   https://www.bilibili.com/video/av37442271/
 - Master启动源码分析详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/StartMaster.md

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442271&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

 
 - Spark Master资源调度--worker向master注册(Youtube视频):  https://youtu.be/74q1nddoaiY​
  - Spark Master资源调度--worker向master注册(Bilibili视频):   https://www.bilibili.com/video/av37442280/
 - Spark Master资源调试--worker向master注册(文档详解)： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/MasterScheduler_workerRegisterMaster.md

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442280&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


 - Spark Master资源调度--SparkContext向所有master注册(Youtube视频):  https://youtu.be/AXxCnCc5Mh0​ 
  - Spark Master资源调度--SparkContext向所有master注册(Bilibili视频):  https://www.bilibili.com/video/av37442295/
 - Spark Master资源调度--SparkContext向所有master注册(文档详解): https://github.com/opensourceteams/spark-scala-maven/blob/master/md/MasterScheduler_SparkContextRegisterMaster.md

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442295&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>



### Spark Worker启动源码分析
 - Spark Worker 启动源码分析(Youtube视频):   https://youtu.be/ll_Ae6rP7II​​
 - Spark Worker 启动源码分析(Bilibili视频):   https://www.bilibili.com/video/av37442247/
 - Spark Worker 启动源码分析(文档详解):  https://github.com/opensourceteams/spark-scala-maven/blob/master/md/StartWorker.md

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442247&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
 
 
### Spark Executor启动源码分析
  - Spark Executor启动源码分析(Youtube视频):   https://youtu.be/1qg4UMPV3pQ
  - Spark Executor启动源码分析(Bilibili视频):    https://www.bilibili.com/video/av37442311/
  - Spark Executor启动源码分析(文档详解)：   https://github.com/opensourceteams/spark-scala-maven/blob/master/md/CoarseGrainedExecutorBackend_start.md

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442311&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
 
 
### Spark 触发Job提交
 - Spark 触发Job提交(youtube视频) : https://youtu.be/X49RIqz2AjM
 - Spark 触发Job提交(bilibili视频) :  https://www.bilibili.com/video/av37445008/
 - Spark 触发Job提交(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/jobSubmitTrigger.md

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37445008&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

 
### Spark DAG调度器事件循环处理器
 - Spark DAG调度器事件循环处理器(Youtube视频) : https://youtu.be/fT-dpf0KFOA
 - Spark DAG调度器事件循环处理器(Bilibili视频) :  https://www.bilibili.com/video/av37445034/
 - Spark DAG调度器事件循环处理器(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/DAGSchedulerEventProcessLoop.md 

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37445034&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

 
### Spark DAGScheduler FinalStage处理(Stage划分)
 - Spark FinalStage处理(Stage划分)(Youtube视频) : https://youtu.be/yFJugOV0Fak
 - Spark FinalStage处理(Stage划分)(Bilibili视频) :  https://www.bilibili.com/video/av37445057/
 - Spark FinalStage处理(Stage划分)(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/FinalStage.md 

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37445057&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
 
### Spark DAGScheduler Stage提交
 - Spark Stage提交(Youtube视频) :  https://youtu.be/NI8-_X6mbl4
 - Spark Stage提交(Bilibili视频) :   https://www.bilibili.com/video/av37445077/
 - Spark Stage提交(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/SubmitStage.md 
 

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37445077&page=1" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>



### Spark DAGScheduler把stage转换成TaskSet的过程
 - Spark DAGScheduler把stage转换成TaskSet的过程(Bilibili视频) https://www.bilibili.com/video/av37442139/?p=18
 - Spark DAGScheduler把stage转换成TaskSet的过程(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/DAGScheduler_stageToTaskSet.md
 
 <iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=65893099&page=18" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe> 


### Spark TaskSchedulerImpl 任务调度方式(FIFO)
- Spark TaskSchedulerImpl 任务调度方式(FIFO)(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/SchedulerFIFO.md
- Spark TaskSchedulerImpl 任务调度方式(FIFO)(bilibili视频) : https://www.bilibili.com/video/av37442139/?p=19

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=66005253&page=19" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


### Spark TaskSchedulerImpl TaskSet处理
- Spark TaskSchedulerImpl TaskSet处理(文档详解)：https://github.com/opensourceteams/spark-scala-maven/blob/master/md/TaskSchedulerTaskSetDeal.md
- Spark TaskSchedulerImpl TaskSet原理分析(bilibili视频) : https://www.bilibili.com/video/av37442139/?p=20
- Spark TaskSchedulerImpl TaskSet原码分析(bilibili视频) : https://www.bilibili.com/video/av37442139/?p=21


<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=66006637&page=20" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=66008946&page=21" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


### Spark 源码分析之ShuffleMapTask处理
- Spark 源码分析之ShuffleMapTask处理(文档详解): https://github.com/opensourceteams/spark-scala-maven/blob/master/md/ShuffleMapTask.md
- Spark 源码分析之ShuffleMapTask处理原理分析图解 (bilibili视频): https://www.bilibili.com/video/av37442139/?p=22
- Spark 源码分析之ShuffleMapTask处理源码分析 (bilibili视频): https://www.bilibili.com/video/av37442139/?p=23
- Spark 源码分析之ShuffleMapTask处理原理分析图解 (youtube视频): https://youtu.be/datHorBipMc
- Spark 源码分析之ShuffleMapTask处理源码分析 (youtube视频): https://youtu.be/cRW_MZ0k5Lw


<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=66008946&page=22" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=66008946&page=23" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>



### Spark 源码分析之ResultTask处理
- Spark 源码分析之ResultTask处理(文档详情):https://github.com/opensourceteams/spark-scala-maven/blob/master/md/ResultTask.md
- Spark 源码分析之ResultTask原理分析图解(bilibili视频):https://www.bilibili.com/video/av37442139/?p=24
- Spark 源码分析之ResultTask处理(bilibili视频):https://www.bilibili.com/video/av37442139/?p=25

<iframe width="800" height="500"  src="//player.bilibili.com/player.html?aid=37442139&cid=66008946&page=24" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>

<iframe width="800" height="500"  src="//player.bilibili.com/player.html?aid=37442139&cid=66008946&page=25" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>


### Hadoop RDD
- HadoopRdd partition的开始位置计算(文档详情): https://github.com/opensourceteams/spark-scala-maven/blob/master/md/HadoopRddPartitionDivide.md

- Hadoop RDD 读取文件(文档详情): https://github.com/opensourceteams/spark-scala-maven/blob/master/md/HadoopRDD_2.md


<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=66303785&page=26" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=66303785&page=27" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>
<iframe  width="800" height="500" src="//player.bilibili.com/player.html?aid=37442139&cid=66303785&page=28" scrolling="no" border="0" frameborder="no" framespacing="0" allowfullscreen="true"> </iframe>










======================================================================================


### Spark 远程调试
 - 详细说明文档： https://github.com/opensourceteams/spark-scala-maven/blob/master/md/SparkRemoteDebug.md




## 相关链接

- [HadoopRDD源码分析说明文档详细](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/hadoopRdd.md "HadoopRDD源码分析说明文档")



### Youtub 视频
- [HadoopRdd源码分析-读取本地文件需求分析-01](https://youtu.be/PtNo5S3g3zc "HadoopRdd源码分析-读取本地文件需求分析-01") 
- [HadoopRDD源码分析-文件拆分partition划分-02](https://youtu.be/kesUJxGBWFA "HadoopRDD源码分析-文件拆分partition划分-02")
- [HadoopRdd源码分析 本地文件读取源码分析 03](https://youtu.be/EuNaoJhK-x4 "HadoopRdd源码分析 本地文件读取源码分析 03")
- [HadoopRdd源码分析 本地文件读取源码分析 04](https://youtu.be/GcPi9b-iltE "HadoopRdd源码分析 本地文件读取源码分析 04")
