# Spark2.4.0源码分析之WorldCount ShuffleMapTask处理(八)

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0


## 时序图
- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/n_08_1_worldCount.executor.task.jpg
![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/n_08_1_worldCount.executor.task.jpg)


- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/n_08_2_worldCount.shuffleMapTask.jpg
![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/n_08_2_worldCount.shuffleMapTask.jpg)


 
## 主要内容描述
- 理解Executor中是如何调用Task的过程
## 输入数据

```
a b a a
c a a

```


## 程序
### BaseSparkSession
```
package com.opensource.bigdata.spark.standalone.base

import java.io.File

import org.apache.spark.sql.SparkSession

/**
  * 得到SparkSession
  * 首先 extends BaseSparkSession
  * 本地: val spark = sparkSession(true)
  * 集群:  val spark = sparkSession()
  */
class BaseSparkSession {

  var appName = "sparkSession"
  var master = "spark://standalone.com:7077" //本地模式:local     standalone:spark://master:7077


  def sparkSession(): SparkSession = {
    val spark = SparkSession.builder
      .master(master)
      .appName(appName)
      .config("spark.eventLog.enabled","true")
      .config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
      .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")
      .getOrCreate()
    spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
    //import spark.implicits._
    spark
  }

  /**
    *
    * @param isLocal
    * @param isHiveSupport
    * @param remoteDebug
    * @param maxPartitionBytes  -1 不设置，否则设置分片大小
    * @return
    */

  def sparkSession(isLocal:Boolean = false, isHiveSupport:Boolean = false, remoteDebug:Boolean=false,maxPartitionBytes:Int = -1): SparkSession = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    if(isLocal){
      master = "local[1]"
      var builder = SparkSession.builder
        .master(master)
        .appName(appName)
        .config("spark.sql.warehouse.dir",warehouseLocation)

      if(isHiveSupport){
        builder = builder.enableHiveSupport()
          //.config("spark.sql.hive.metastore.version","2.3.3")
      }

      //调置分区大小(分区文件块大小)
      if(maxPartitionBytes != -1){
        builder.config("spark.sql.files.maxPartitionBytes",maxPartitionBytes) //32
      }

      builder.config("spark.executor.heartbeatInterval","10000s") //心跳间隔，超时设置
      builder.config("spark.network.timeout","100000s") //网络间隔，超时设置


      val spark = builder.getOrCreate()

      //spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
      //import spark.implicits._
      spark
    }else{

      var builder = SparkSession.builder
        .master(master)
        .appName(appName)
        .config("spark.sql.warehouse.dir",warehouseLocation)

        .config("spark.eventLog.enabled","true")
        .config("spark.eventLog.compress","true")
        .config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
        .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")


        //调置分区大小(分区文件块大小)
        if(maxPartitionBytes != -1){
          builder.config("spark.sql.files.maxPartitionBytes",maxPartitionBytes) //32
        }



       // .config("spark.sql.shuffle.partitions",2)

       //executor debug,是在提交作的地方读取
        if(remoteDebug){

          builder.config("spark.executor.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002")

          builder.config("spark.executor.heartbeatInterval","10000s") //心跳间隔，超时设置
          builder.config("spark.network.timeout","100000s") //网络间隔，超时设置
        }



      if(isHiveSupport){
        builder = builder.enableHiveSupport()
        //.config("spark.sql.hive.metastore.version","2.3.3")
      }

      val spark = builder.getOrCreate()
      //需要有jar才可以在远程执行
      spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")



      spark
    }

  }


  /**
    * 得到当前工程的路径
    * @return
    */
  def getProjectPath:String=System.getProperty("user.dir")
}

```



### WorldCount程序

```
package com.opensource.bigdata.spark.standalone.wordcount.spark.session.n.n_04_group_collect

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {
    appName = "WorldCount"


   val spark = sparkSession(false,false,false,-1)
    import spark.implicits._
    val distFile = spark.read.textFile("data/text/worldCount.txt")
    val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(x => x ).count()
    println("结果:"+dataset.collect().mkString("\n"))


    spark.stop()

  }
}



```



### executor中任务的启动

#### CoarseGrainedSchedulerBackend.DriverEndpoint.launchTasks
- 任务调度器，通过资源调度算法，算出需要在executor启动的任务
- 调用executor启动任务,给executor发送消息LaunchTask来启动任务

```
    // Launch tasks returned by a set of resource offers
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        val serializedTask = TaskDescription.encode(task)
        if (serializedTask.limit() >= maxRpcMessageSize) {
          Option(scheduler.taskIdToTaskSetManager.get(task.taskId)).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.rpc.message.maxSize (%d bytes). Consider increasing " +
                "spark.rpc.message.maxSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit(), maxRpcMessageSize)
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK

          logDebug(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: " +
            s"${executorData.executorHost}.")

          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

```


#### CoarseGrainedExecutorBackend.receive处理消息
- 处理发送给executor的消息
- 当接收到LaunchTask消息时，调用 executor.launchTask()函数来处理

```
override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }

    case RegisterExecutorFailed(message) =>
      exitExecutor(1, "Slave registration failed: " + message)

    case LaunchTask(data) =>
      if (executor == null) {
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        val taskDesc = TaskDescription.decode(data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskDesc)
      }

    case KillTask(taskId, _, interruptThread, reason) =>
      if (executor == null) {
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        executor.killTask(taskId, interruptThread, reason)
      }

    case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
      // a message to self to actually do the shutdown.
      self.send(Shutdown)

    case Shutdown =>
      stopping.set(true)
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
          // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
          // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
          // Therefore, we put this line in a new thread.
          executor.stop()
        }
      }.start()

    case UpdateDelegationTokens(tokenBytes) =>
      logInfo(s"Received tokens of ${tokenBytes.length} bytes")
      SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)
  }

```

#### Executor.launchTask()
- 线程TaskRunner来处理实际的任务
- 将任务放到线程池中，进行调用
- threadPool.execute()调用TaskRunner.run()函数

```
  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
    val tr = new TaskRunner(context, taskDescription)
    runningTasks.put(taskDescription.taskId, tr)
    threadPool.execute(tr)
  }

```

#### TaskRunner.run()
- 调用Task.run()具体的运行任务类，这里有两种任务(ShuffleMapTask,ResultTask)
- 得到结果value,对结果进行处理

```
 override def run(): Unit = {
      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)
      val threadMXBean = ManagementFactory.getThreadMXBean
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTime = System.currentTimeMillis()
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStartTime: Long = 0
      var taskStartCpu: Long = 0
      startGCTime = computeTotalGcTime()

      try {
        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        Executor.taskDeserializationProps.set(taskDescription.properties)

        updateDependencies(taskDescription.addedFiles, taskDescription.addedJars)
        task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        task.localProperties = taskDescription.properties
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        val killReason = reasonIfKilled
        if (killReason.isDefined) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException(killReason.get)
        }

        // The purpose of updating the epoch here is to invalidate executor map output status cache
        // in case FetchFailures have occurred. In local mode `env.mapOutputTracker` will be
        // MapOutputTrackerMaster and its cache invalidation is not based on epoch numbers so
        // we don't need to make any special calls here.
        if (!isLocal) {
          logDebug("Task " + taskId + "'s epoch is " + task.epoch)
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].updateEpoch(task.epoch)
        }

        // Run the actual task and measure its runtime.
        taskStartTime = System.currentTimeMillis()
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        val value = Utils.tryWithSafeFinally {
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } {
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logInfo(errMsg)
            }
          }
        }
        task.context.fetchFailed.foreach { fetchFailure =>
          // uh-oh.  it appears the user code has caught the fetch-failure without throwing any
          // other exceptions.  Its *possible* this is what the user meant to do (though highly
          // unlikely).  So we will log an error and keep going.
          logError(s"TID ${taskId} completed successfully though internally it encountered " +
            s"unrecoverable fetch failures!  Most likely this means user code is incorrectly " +
            s"swallowing Spark's internal ${classOf[FetchFailedException]}", fetchFailure)
        }
        val taskFinish = System.currentTimeMillis()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // If the task has been killed, let's fail it.
        task.context.killTaskIfInterrupted()

        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        task.metrics.setExecutorDeserializeTime(
          (taskStartTime - deserializeStartTime) + task.executorDeserializeTime)
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        task.metrics.setExecutorRunTime((taskFinish - taskStartTime) - task.executorDeserializeTime)
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)

        // Expose task metrics using the Dropwizard metrics system.
        // Update task metrics counters
        executorSource.METRIC_CPU_TIME.inc(task.metrics.executorCpuTime)
        executorSource.METRIC_RUN_TIME.inc(task.metrics.executorRunTime)
        executorSource.METRIC_JVM_GC_TIME.inc(task.metrics.jvmGCTime)
        executorSource.METRIC_DESERIALIZE_TIME.inc(task.metrics.executorDeserializeTime)
        executorSource.METRIC_DESERIALIZE_CPU_TIME.inc(task.metrics.executorDeserializeCpuTime)
        executorSource.METRIC_RESULT_SERIALIZE_TIME.inc(task.metrics.resultSerializationTime)
        executorSource.METRIC_SHUFFLE_FETCH_WAIT_TIME
          .inc(task.metrics.shuffleReadMetrics.fetchWaitTime)
        executorSource.METRIC_SHUFFLE_WRITE_TIME.inc(task.metrics.shuffleWriteMetrics.writeTime)
        executorSource.METRIC_SHUFFLE_TOTAL_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.totalBytesRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.remoteBytesRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK
          .inc(task.metrics.shuffleReadMetrics.remoteBytesReadToDisk)
        executorSource.METRIC_SHUFFLE_LOCAL_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.localBytesRead)
        executorSource.METRIC_SHUFFLE_RECORDS_READ
          .inc(task.metrics.shuffleReadMetrics.recordsRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED
          .inc(task.metrics.shuffleReadMetrics.remoteBlocksFetched)
        executorSource.METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED
          .inc(task.metrics.shuffleReadMetrics.localBlocksFetched)
        executorSource.METRIC_SHUFFLE_BYTES_WRITTEN
          .inc(task.metrics.shuffleWriteMetrics.bytesWritten)
        executorSource.METRIC_SHUFFLE_RECORDS_WRITTEN
          .inc(task.metrics.shuffleWriteMetrics.recordsWritten)
        executorSource.METRIC_INPUT_BYTES_READ
          .inc(task.metrics.inputMetrics.bytesRead)
        executorSource.METRIC_INPUT_RECORDS_READ
          .inc(task.metrics.inputMetrics.recordsRead)
        executorSource.METRIC_OUTPUT_BYTES_WRITTEN
          .inc(task.metrics.outputMetrics.bytesWritten)
        executorSource.METRIC_OUTPUT_RECORDS_WRITTEN
          .inc(task.metrics.outputMetrics.recordsWritten)
        executorSource.METRIC_RESULT_SIZE.inc(task.metrics.resultSize)
        executorSource.METRIC_DISK_BYTES_SPILLED.inc(task.metrics.diskBytesSpilled)
        executorSource.METRIC_MEMORY_BYTES_SPILLED.inc(task.metrics.memoryBytesSpilled)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit()

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        setTaskFinishedAndClearInterruptStatus()
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case t: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId), reason: ${t.reason}")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTime)
          val serializedTK = ser.serialize(TaskKilled(t.reason, accUpdates, accums))
          execBackend.statusUpdate(taskId, TaskState.KILLED, serializedTK)

        case _: InterruptedException | NonFatal(_) if
            task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId), reason: $killReason")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTime)
          val serializedTK = ser.serialize(TaskKilled(killReason, accUpdates, accums))
          execBackend.statusUpdate(taskId, TaskState.KILLED, serializedTK)

        case t: Throwable if hasFetchFailure && !Utils.isFatalError(t) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // there was a fetch failure in the task, but some user code wrapped that exception
            // and threw something else.  Regardless, we treat it as a fetch failure.
            val fetchFailedCls = classOf[FetchFailedException].getName
            logWarning(s"TID ${taskId} encountered a ${fetchFailedCls} and " +
              s"failed, but the ${fetchFailedCls} was hidden by another " +
              s"exception.  Spark is handling this like a fetch failure and ignoring the " +
              s"other exception: $t")
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskCommitDeniedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

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
            val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTime)

            val serializedTaskEndReason = {
              try {
                ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
              } catch {
                case _: NotSerializableException =>
                  // t is not serializable so just send the stacktrace
                  ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
              }
            }
            setTaskFinishedAndClearInterruptStatus()
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (!t.isInstanceOf[SparkOutOfMemoryError] && Utils.isFatalError(t)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }
      } finally {
        runningTasks.remove(taskId)
      }
    }


```
### ShuffleMapTaskt 源码分析

#### ShuffleMapTaskt 处理
- 通过广播变量taskBinary得到RDD返序列化，ShuffleDependency
- 通过ShuffleDependency,partitionId得到BypassMergeSortShuffleWriter
- 迭代当前partitions对应的RDD,调用lazy()函数FileSourceScanExec.inputRDD
- 调用BypassMergeSortShuffleWriter.write()函数处理

```
override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
```


#### FileSourceScanExec.inputRDD
- 调用FileFormat.buildReaderWithPartitionValues()函数

```
private lazy val inputRDD: RDD[InternalRow] = {
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    relation.bucketSpec match {
      case Some(bucketing) if relation.sparkSession.sessionState.conf.bucketingEnabled =>
        createBucketedReadRDD(bucketing, readFile, selectedPartitions, relation)
      case _ =>
        createNonBucketedReadRDD(readFile, selectedPartitions, relation)
    }
  }

```

#### 用FileFormat.buildReaderWithPartitionValues()
- 调用用FileFormat.buildReader

```
 /**
   * Exactly the same as [[buildReader]] except that the reader function returned by this method
   * appends partition values to [[InternalRow]]s produced by the reader function [[buildReader]]
   * returns.
   */
  def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val dataReader = buildReader(
      sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)

    new (PartitionedFile => Iterator[InternalRow]) with Serializable {
      private val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes

      private val joinedRow = new JoinedRow()

      // Using lazy val to avoid serialization
      private lazy val appendPartitionColumns =
        GenerateUnsafeProjection.generate(fullSchema, fullSchema)

      override def apply(file: PartitionedFile): Iterator[InternalRow] = {
        // Using local val to avoid per-row lazy val check (pre-mature optimization?...)
        val converter = appendPartitionColumns

        // Note that we have to apply the converter even though `file.partitionValues` is empty.
        // This is because the converter is also responsible for converting safe `InternalRow`s into
        // `UnsafeRow`s.
        dataReader(file).map { dataRow =>
          converter(joinedRow(dataRow, file.partitionValues))
        }
      }
    }
  }
```

#### FileFormat.buildReader
- 调用TextFileFormat.buildReader
```
override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    assert(
      requiredSchema.length <= 1,
      "Text data source only produces a single data column named \"value\".")
    val textOptions = new TextOptions(options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    readToUnsafeMem(broadcastedHadoopConf, requiredSchema, textOptions)
  }
```

#### TextFileFormat.readToUnsafeMem
- 读取HDFS上的文件，一行一行读取
- 把读到的每一行数据，都转化为UnSafeRow对象

```
private def readToUnsafeMem(
      conf: Broadcast[SerializableConfiguration],
      requiredSchema: StructType,
      textOptions: TextOptions): (PartitionedFile) => Iterator[UnsafeRow] = {

    (file: PartitionedFile) => {
      val confValue = conf.value.value
      val reader = if (!textOptions.wholeText) {
        new HadoopFileLinesReader(file, textOptions.lineSeparatorInRead, confValue)
      } else {
        new HadoopFileWholeTextReader(file, confValue)
      }
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => reader.close()))
      if (requiredSchema.isEmpty) {
        val emptyUnsafeRow = new UnsafeRow(0)
        reader.map(_ => emptyUnsafeRow)
      } else {
        val unsafeRowWriter = new UnsafeRowWriter(1)

        reader.map { line =>
          // Writes to an UnsafeRow directly
          unsafeRowWriter.reset()
          unsafeRowWriter.write(0, line.getBytes, 0, line.getLength)
          unsafeRowWriter.getRow()
        }
      }
    }
  }

```

### Iterator[InternalRow] 构建
- 构建ShuffleMapTask的Iterator,这个迭代器是已经在本地合并过的数据,就是已经group,count在本地的数据(key,value)
- WholeStageCodegenExec.doExecute()在这个函数中构建的
- 即从FileScanRDD读到HDFS上一行一行的数据后,最后生成一个可迭代对象buffer
- buffer对象生成，是经过本地聚合，定义的一系列需要处理的函数，已经处理过的

```
  rdds.head.mapPartitionsWithIndex { (index, iter) =>
        val (clazz, _) = CodeGenerator.compile(cleanedSource)
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(index, Array(iter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val v = buffer.hasNext
            if (!v) durationMs += buffer.durationMs()
            v
          }
          override def next: InternalRow = buffer.next()
        }
      }
```


#### BypassMergeSortShuffleWriter.write()
- partitionWriters:DiskBlockObjectWriter,往临时数据文件中写入数据(key,value),数据是经过序列化的，temp_shuffle_2651d1d9-7d23-478e-8e7a-0a9d0210b723
- partitionWriterSegments：FileSegment 记录每个partition中数据的长度
- output:File 最终数据文件,格式shuffle_0_0_0.data,（个数由ShuffleId,MapId决定）
- tmp:FIle 最终临时数据输出文件，格式如:shuffle_0_0_0.data.f8eaccb1-9def-490a-9200-f33d64cba4db
- 调用函数BypassMergeSortShuffleWriter.writePartitionedFile() 合并当前mapId所有的临时数据文件到 最终临时数据输出文件shuffle_0_0_0.data.f8eaccb1-9def-490a-9200-f33d64cba4db 中，并返回所有临时数据文件中数据的长度做为数组
- 调用IndexShuffleBlockResolver.writeIndexFileAndCommit()函数
- IndexShuffleBlockResolver.writeIndexFileAndCommit()
    - 新建临时数据文件对应的索引文件和临时索引文件
    - 索引文件格式: shuffle_0_0_0.index  
    - 临时索引文件格式: shuffle_0_0_0.index.785e9418-0076-46b0-b81a-34dc8c5c5b89
    - 把每个临时数据文件的经长度转化后的位置偏移量存入临时索引文件格式，最终复制索引文件临时文件到索引文件，和最终临时数据文件到最终数据文件，并删除临时文件

- 返回MapStatus(BlockManagerId,partitionLengths)
- 相当于此时已写入数据到数据文件shuffle_0_0_0.data(文件中的数据是序列化压缩后的数据)
```
(c,1)
(b,1)
(a,5)
```
- 相当于此时已写入索引文件shuffle_0_0_0.index (文件中的数据是序列化压缩后的数据),共200个此时numPartitions为200
```
0
0
0
......
offset += length (length=70) 
......
offset += length (length=70) 
......
offset += length (length=71) 
......


```


```


@Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    if (!records.hasNext()) {
      partitionLengths = new long[numPartitions];
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      return;
    }
    final SerializerInstance serInstance = serializer.newInstance();
    final long openStartTime = System.nanoTime();
    partitionWriters = new DiskBlockObjectWriter[numPartitions];
    partitionWriterSegments = new FileSegment[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
        blockManager.diskBlockManager().createTempShuffleBlock();
      final File file = tempShuffleBlockIdPlusFile._2();
      final BlockId blockId = tempShuffleBlockIdPlusFile._1();
      partitionWriters[i] =
        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
    }
    // Creating the file to write to and creating a disk writer both involve interacting with
    // the disk, and can take a long time in aggregate when we open many files, so should be
    // included in the shuffle write time.
    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final K key = record._1();
      partitionWriters[partitioner.getPartition(key)].write(key, record._2());
    }

    for (int i = 0; i < numPartitions; i++) {
      final DiskBlockObjectWriter writer = partitionWriters[i];
      partitionWriterSegments[i] = writer.commitAndGet();
      writer.close();
    }

    File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    File tmp = Utils.tempFileWith(output);
    try {
      partitionLengths = writePartitionedFile(tmp);
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

```

#### BypassMergeSortShuffleWriter.writePartitionedFile()
- 调用函数BypassMergeSortShuffleWriter.writePartitionedFile() 合并当前mapId所有的临时数据文件到 最终临时数据输出文件shuffle_0_0_0.data.f8eaccb1-9def-490a-9200-f33d64cba4db 中，并返回所有临时数据文件中数据的长度做为数组
```
/**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedFile(File outputFile) throws IOException {
    // Track location of the partition starts in the output file
    final long[] lengths = new long[numPartitions];
    if (partitionWriters == null) {
      // We were passed an empty iterator
      return lengths;
    }

    final FileOutputStream out = new FileOutputStream(outputFile, true);
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    try {
      for (int i = 0; i < numPartitions; i++) {
        final File file = partitionWriterSegments[i].file();
        if (file.exists()) {
          final FileInputStream in = new FileInputStream(file);
          boolean copyThrewException = true;
          try {
            lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
            copyThrewException = false;
          } finally {
            Closeables.close(in, copyThrewException);
          }
          if (!file.delete()) {
            logger.error("Unable to delete file for partition {}", i);
          }
        }
      }
      threwException = false;
    } finally {
      Closeables.close(out, threwException);
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
    partitionWriters = null;
    return lengths;
  }


```


#### IndexShuffleBlockResolver.writeIndexFileAndCommit()
- 新建临时数据文件对应的索引文件和临时索引文件
- 索引文件格式: shuffle_0_0_0.index  
- 临时索引文件格式: shuffle_0_0_0.index.785e9418-0076-46b0-b81a-34dc8c5c5b89
- 把每个临时数据文件的经长度转化后的位置偏移量存入临时索引文件格式，最终复制索引文件临时文件到索引文件，和最终临时数据文件到最终数据文件，并删除临时文件

```
/**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = Utils.tempFileWith(indexFile)
    try {
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
          Utils.tryWithSafeFinally {
            // We take in lengths of each block, need to convert it to offsets.
            var offset = 0L
            out.writeLong(offset)
            for (length <- lengths) {
              offset += length
              out.writeLong(offset)
            }
          } {
            out.close()
          }

          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }


```

end