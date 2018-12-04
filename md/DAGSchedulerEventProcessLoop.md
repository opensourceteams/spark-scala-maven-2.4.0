# DAG调度器事件循环处理器

## DAGSchedulerEventProcessLoop.scala
- DAGSchedulerEventProcessLoop(DAG调度器事件循环处理器)继承抽象类EventLoop(事件循环器)
- 当调用 post方法增加事件时，实际上是往EventLoop中的列表阻塞队列eventQueue增加元素
- EventLoop在DAGScheduler类中结尾处调用  eventProcessLoop.start()
-  EventLoop中start()方法会调用eventThread中的run()方法，无限循环处理阻塞队列中的事件,并调用抽像方法onReceive()
- 子类 DAGSchedulerEventProcessLoop 中进行onReceive()的实现，所以会调用此方法进行实际事件处理操作
- 处理的事件如下
  - JobSubmitted
  - MapStageSubmitted
  - StageCancelled
  - JobCancelled
  - JobGroupCancelled
  - AllJobsCancelled
  - ExecutorAdded
  - ExecutorLost
  - BeginEvent
  - GettingResultEvent
  - completion @ CompletionEvent
  - TaskSetFailed
  - ResubmitFailedStages


```scala
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

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

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId) =>
      dagScheduler.handleExecutorLost(execId, fetchFailed = false)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion @ CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stop()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}
```