# Spark2.4.0源码分析之WorldCount Stage划分(DAGScheduler)(四)

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0
 
## 时序图
- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/worldCount.FinallStage.jpg
![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/worldCount.FinallStage.jpg)


## 主要内容描述
- 理解FinalStage的转化(即Stage的划分)

## Stage 图解
- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/dagVisualization/worldCount-stage.png
![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/dagVisualization/worldCount-stage.png)



## 源码分析

### DAGSchedulerEventProcessLoop.onReceive
- 调用DAGSchedulerEventProcessLoop.doOnReceive()函数处理事件
```
  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

```

### DAGSchedulerEventProcessLoop.onReceive
- 处理JobSubmitted事件,即调用dagScheduler.handleJobSubmitted()函数处理
```
private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val workerLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, workerLost)

    case WorkerRemoved(workerId, host, message) =>
      dagScheduler.handleWorkerRemoved(workerId, host, message)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case SpeculativeTaskSubmitted(task) =>
      dagScheduler.handleSpeculativeTaskSubmitted(task)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

```

### dagScheduler.handleJobSubmitted()
- JobSubmitted事件处理的方法
- 本节内容关注该函数中FinalStage是怎么得到的，即Stage的划分,调用函数createResultStage(FinalRDD)得到FinalStage
- 得到FinalStage之后，再调用DagScheduler.submitStage(FinalStage)函数对FinalStage进行提交处理(其它内容分析)

```
private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: BarrierJobSlotsNumberCheckFailed =>
        logWarning(s"The job $jobId requires to run a barrier stage that requires more slots " +
          "than the total number of slots in the cluster currently.")
        // If jobId doesn't exist in the map, Scala coverts its value null to 0: Int automatically.
        val numCheckFailures = barrierJobIdToNumTasksCheckFailures.compute(jobId,
          new BiFunction[Int, Int, Int] {
            override def apply(key: Int, value: Int): Int = value + 1
          })
        if (numCheckFailures <= maxFailureNumTasksCheck) {
          messageScheduler.schedule(
            new Runnable {
              override def run(): Unit = eventProcessLoop.post(JobSubmitted(jobId, finalRDD, func,
                partitions, callSite, listener, properties))
            },
            timeIntervalNumTasksCheck,
            TimeUnit.SECONDS
          )
          return
        } else {
          // Job failed, clear internal data.
          barrierJobIdToNumTasksCheckFailures.remove(jobId)
          listener.jobFailed(e)
          return
        }

      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    // Job submitted, clear internal data.
    barrierJobIdToNumTasksCheckFailures.remove(jobId)

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)
  }

```


### DagScheduler.createResultStage
- 该函数创建FinalStage,即FinalStage一定是ResultStage
- ResultStage是依赖上级Stage，所以计算ResultStage之前，是先计算ResultStage的上级Stage
- 调用函数DagScheduler.getOrCreateParentStages(RDD)得到上级Stage列表
```
/**
   * Create a ResultStage associated with the provided jobId.
   */
  private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd)
    checkBarrierStageWithRDDChainPattern(rdd, partitions.toSet.size)
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }
```

### DagScheduler.getOrCreateParentStages
- 调用函当我getShuffleDependencies得到FinalRDD上级链的第一个ShuffleDependency
- 调用DAGScheduler.getOrCreateShuffleMapStage(ShuffleDependency)函数得到ShuffleMapStage

```
  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   */
  private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }
```

### DAGScheduler.getOrCreateShuffleMapStage
- 根据shuffleId做为key,缓存数据中shuffleIdToMapStage，如果有就直接返回缓存中的数据
- 如果缓存中没有，说明还没有被计算过，调用函数DAGScheduler.getMissingAncestorShuffleDependencies继续判断上级RDD还有没有ShuffleDependency,如果上级RDD还有ShuffleDependency,就先创建上级ShuffleMapStage,如此类推，就变成，先从顶级ShuffleDependency开如，创建他的ShuffleMapStage,一直到最后一个ShuffleDependency和对应的ShuffleMapStage,这样就从FinalStage从始到顶上所有的Stage都已创建好了
- 调用DAGScheduler.createShuffleMapStage(ShuffleDep)函数创建ShuffleMapStage

```
 /**
   * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
   * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
   * addition to any missing ancestor shuffle map stages.
   */
  private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage

      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }
```

### DAGScheduler.createShuffleMapStage(ShuffleDep)
- 先计算上级Stage,调用函数DAGScheduler.getOrCreateParentStages(RDD)
- new ShuffleMapStage(ParentStage）

```
/**
   * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
   * previously run stage generated the same shuffle data, this function will copy the output
   * locations that are still available from the previous shuffle to avoid unnecessarily
   * regenerating data.
   */
  def createShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd)
    checkBarrierStageWithRDDChainPattern(rdd, rdd.getNumPartitions)
    val numTasks = rdd.partitions.length
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(
      id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep, mapOutputTracker)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)

    if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }
```


### DAGScheduler.getOrCreateParentStages(RDD)
- 等于是找上级RDD还有没有ShuffleDependency,如果有，就先处理上级ShuffleDependency,即给上级ShuffleDependency，找到对应的ShuffleMapStage
- 如果上级RDD没有ShuffleDependency,即为Nil,即ShuffleMapStage的上级Stage为Nil,此时的ShuffleMapStage为顶级Stage
- 向上返回DAGScheduler.createResultStage

```
  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   */
  private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }
```

### DAGScheduler.createResultStage
- 函数DAGScheduler.getOrCreateParentStages()得到的为ShuffleMapStage
- new ResultStage(ParentStage)作为FinalStage
- 到这里FinalStage已经计算完成，(即Stage划分已经完成)

```
  /**
   * Create a ResultStage associated with the provided jobId.
   */
  private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd)
    checkBarrierStageWithRDDChainPattern(rdd, partitions.toSet.size)
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

```

end