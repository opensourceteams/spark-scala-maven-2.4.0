# Spark2.4.0源码分析之WorldCount 事件循环处理器(三)

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0
 
## 时序图
- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/DAGSchedulerEventProcessLoop.jpg
![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/DAGSchedulerEventProcessLoop.jpg)


## 主要内容描述
- 理解DAG事件循环处理器处理事件流程


## 源码分析

### DAGScheduler.submitJob
- 调用DAGSchedulerEventProcessLoop.post进行JobSubmitted事件提交


```
 /**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

```

### DAGSchedulerEventProcessLoop.post
- DAGSchedulerEventProcessLoop继承EventLoop[DAGSchedulerEvent]
- DAGSchedulerEventProcessLoop中没有定义post函数，就等于调用EventLoop.post()函数

```
  /**
   * Put the event into the event queue. The event thread will process it later.
   */
  def post(event: E): Unit = {
    eventQueue.put(event)
  }

```

### EventLoop.start
- DAGScheduler类的末尾调用eventProcessLoop.start()
- DAGSchedulerEventProcessLoop中没有定义start()函数
- 等于调用EventLoop.start()函数,也就是说DAGScheduler进行实例化时，已经调用函数EventLoop.start
- 调用eventThread.start()函数,触发线程的run()函数

```
  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart()
    eventThread.start()
  }

```

### EventLoop
- 列表阻塞队列LinkedBlockingDeque，存放事件
- 实例化后就死循环调用了事件阻塞队列中的事件，取到事件后调用EventLoop.onReceive()函数,该函数没有实现，调用子类，即DAGSchedulerEventProcessLoop.onReceive()函数


```

private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

 // Exposed for testing.
  private[spark] val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      try {
        while (!stopped.get) {
          val event = eventQueue.take()
          try {
            onReceive(event)
          } catch {
            case NonFatal(e) =>
              try {
                onError(e)
              } catch {
                case NonFatal(e) => logError("Unexpected error in " + name, e)
              }
          }
        }
      } catch {
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) => logError("Unexpected error in " + name, e)
      }
    }

  }

```

### DAGSchedulerEventProcessLoop.onReceive()
- 调用DAGSchedulerEventProcessLoop.doOnReceive()对不同的事件类型进行匹配，用相应的事件处理方法进行处理

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


### DAGSchedulerEventProcessLoop.doOnReceive()
- JobSubmitted事件就调用dagScheduler.handleJobSubmitted()函数进行处理
- 支持如下事件

```
可以处理多种事件
).JobSubmitted
).MapStageSubmitted
).StageCancelled
).JobCancelled
).JobGroupCancelled
).AllJobsCancelled
).ExecutorAdded
).ExecutorLost
).WorkerRemoved
).BeginEvent
).SpeculativeTaskSubmitted
).GettingResultEvent
).completion: CompletionEvent
).TaskSetFailed
).ResubmitFailedStages

```


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



end