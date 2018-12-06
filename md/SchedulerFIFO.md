# Spark TaskSchedulerImpl 任务调度方式(FIFO)


## 更多资源
- SPARK 源码分析技术分享(bilibilid视频汇总套装视频): https://www.bilibili.com/video/av37442139/
- github: https://github.com/opensourceteams/spark-scala-maven
- csdn(汇总视频在线看): https://blog.csdn.net/thinktothings/article/details/84726769

## 图解
[![FIFO](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/spark/%E4%BB%BB%E5%8A%A1%E8%B0%83%E5%BA%A6%E4%B8%AD%E7%9A%84%E8%B0%83%E5%BA%A6%E5%99%A8FIFO%E7%AE%97%E6%B3%95.png "FIFO")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/spark/%E4%BB%BB%E5%8A%A1%E8%B0%83%E5%BA%A6%E4%B8%AD%E7%9A%84%E8%B0%83%E5%BA%A6%E5%99%A8FIFO%E7%AE%97%E6%B3%95.png "FIFO")

## TaskSchedulerImpl提交任务集

- 在DAGScheduler.scal中件中的submitMissingTasks()方法中调用  taskScheduler.submitTasks
- 把任务集通过任务调度器进行提交

```scala
 taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
```


- 任务调度器实现
```scala
override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }
```


- 把任务集放到TaskSetManager(任务集管理器)中
- TaskSetManager(任务集管理器)继承 Schedulable，（可调度元素，就是把到调度池队列中的一个元素，供调度使用）

```scala
val manager = createTaskSetManager(taskSet, maxTaskFailures)
```

- 把任务集管理器增加到指定调度类型(FIFO,PAIR)的调度池中，也就是调度池中的调度队列中schedulableQueue
- 此时，相当于需要调度的任务已有了，存放在调度池中，下面是用具体的调度算法，按指定的顺序调度池中的任务

```scala
schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
```

- 任务调度器的submitTasks()方法中调用  backend.reviveOffers()方法,backend为SparkDeploySchedulerBackend,继承CoarseGrainedSchedulerBackend,所以调用的是CoarseGrainedSchedulerBackend中的reviveOffers()方法


```scala
 backend.reviveOffers()
```


- 相当于是给Driver发送消息ReviveOffers
```scala
   override def reviveOffers() {
    driverEndpoint.send(ReviveOffers)
  }
```

- driverEndpoint 中receive()方法处理消息,调用makeOffers()方法

```scala
     case ReviveOffers =>
        makeOffers()
```

- scheduler.resourceOffers(workOffers)会计算出需要启动的任务序列
- resourceOffers()方法中调用方法得到调度任务的队列（按指定顺序的） rootPool.getSortedTaskSetQueue()
- launchTasks()方法把启动任务消息发送给executor
```scala
   // Make fake resource offers on all executors
    private def makeOffers() {
      // Filter out executors under killing
      val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
      val workOffers = activeExecutors.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toIndexedSeq
      launchTasks(scheduler.resourceOffers(workOffers))
    }
```

- 按指定的调度算法，对调度池中的调度任务进行排序
- 返回排序后调度队列

```scala
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }
```

### FIFO调度算法的实现
- 默认的调度算法FIFO
- 按作业id进行比较，id小的放在前，也就是先进来的作业先处理
- 如果作业id相同，就按stageId比较，StageId小的放在前，也就是从第一个Stage依次开始排列

```scala

private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    if (res < 0) {
      true
    } else {
      false
    }
  }
}
```




## 定时任务处理调度池中的任务
- DriverEndpoint 的 onStart()方法中会每秒调用一次处理调度池中调度任务的方法
-  通过发送Driver消息ReviveOffers 来触发

```scala
   override def onStart() {
      // Periodically revive offers to allow delay scheduling to work
      val reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s")

      reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          Option(self).foreach(_.send(ReviveOffers))
        }
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
    }
```

