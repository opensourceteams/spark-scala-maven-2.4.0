### Spark 源码分析之ShuffleMapTask处理


## 粗粒度执行器处理LaunchTask消息
- CoarseGrainedExecutorBackend的receive()方法收到任务调度器发送过来的启动任务的消息，并进行消息处理： LaunchTask()
- 该方法中调用 Executor.launchTask()方法

```scala
    case LaunchTask(data) =>
      if (executor == null) {
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
          taskDesc.name, taskDesc.serializedTask)
      }
```
- Executor.launchTask()方法
- 用线程池来启动Task，这样保证任务可以排队等候
- 当线程池中的任务被执行时调用 TaskRunner.run()方法

```scala

  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner
   // Start worker thread pool
  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("Executor task launch worker")
  
 def launchTask(
      context: ExecutorBackend,
      taskId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer): Unit = {
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }

```

- TaskRunner.run() 方法
- 调用Task的实现类，进行任务处理
- 实现类(ShuffleMapTask或ResutlTask)处理任务完成后，发送任务状态为TaskState.FINISHED  的消息

```scala
override def run(): Unit = {
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTime = System.currentTimeMillis()
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      startGCTime = computeTotalGcTime()

      try {
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
        updateDependencies(taskFiles, taskJars)
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        if (killed) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }

        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.
        taskStart = System.currentTimeMillis()
        var threwException = true
        val (value, accumUpdates) = try {
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()
          if (freedMemory > 0) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false) && !threwException) {
              throw new SparkException(errMsg)
            } else {
              logError(errMsg)
            }
          }

          if (releasedLocks.nonEmpty) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
              releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false) && !threwException) {
              throw new SparkException(errMsg)
            } else {
              logError(errMsg)
            }
          }
        }
        val taskFinish = System.currentTimeMillis()

        // If the task has been killed, let's fail it.
        if (task.killed) {
          throw new TaskKilledException
        }

        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        for (m <- task.metrics) {
          // Deserialization happens in two parts: first, we deserialize a Task object, which
          // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
          m.setExecutorDeserializeTime(
            (taskStart - deserializeStartTime) + task.executorDeserializeTime)
          // We need to subtract Task.run()'s deserialization time to avoid double-counting
          m.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
          m.setJvmGCTime(computeTotalGcTime() - startGCTime)
          m.setResultSerializationTime(afterSerialization - beforeSerialization)
          m.updateAccumulators()
        }

        val directResult = new DirectTaskResult(valueBytes, accumUpdates, task.metrics.orNull)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case ffe: FetchFailedException =>
          val reason = ffe.toTaskFailedReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case _: TaskKilledException | _: InterruptedException if task.killed =>
          logInfo(s"Executor killed $taskName (TID $taskId)")
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskFailedReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // SPARK-20904: Do not report failure to driver if if happened during shut down. Because
          // libraries may set up shutdown hooks that race with running tasks during shutdown,
          // spurious failures may occur and can result in improper accounting in the driver (e.g.
          // the task failure would not be ignored if the shutdown happened because of premption,
          // instead of an app issue).
          if (!ShutdownHookManager.inShutdown()) {
            val metrics: Option[TaskMetrics] = Option(task).flatMap { task =>
              task.metrics.map { m =>
                m.setExecutorRunTime(System.currentTimeMillis() - taskStart)
                m.setJvmGCTime(computeTotalGcTime() - startGCTime)
                m.updateAccumulators()
                m
              }
            }
            val serializedTaskEndReason = {
              try {
                ser.serialize(new ExceptionFailure(t, metrics))
              } catch {
                case _: NotSerializableException =>
                  // t is not serializable so just send the stacktrace
                  ser.serialize(new ExceptionFailure(t, metrics, false))
              }
            }
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            SparkUncaughtExceptionHandler.uncaughtException(t)
          }

      } finally {
        runningTasks.remove(taskId)
      }
    }
  }
```

## ShuflleMapTask的处理进程

