# Spark MapOutputTracker源码分析


## 更多资源分享
- SPARK 源码分析技术分享(视频汇总套装视频): https://www.bilibili.com/video/av37442139/
- github: https://github.com/opensourceteams/spark-scala-maven
- csdn(汇总视频在线看): https://blog.csdn.net/thinktothings/article/details/84726769


## 前置条件
- Hadoop版本: Hadoop 2.6.0-cdh5.15.0
- Spark版本: SPARK 1.6.0-cdh5.15.0
- JDK.1.8.0_191
- scala2.10.7

## 技能标签
- Spark ShuffleMapTask处理完成后，把MapStatus数据(BlockManagerId,[compressSize])发送给MapOutputTrackerMaster.mapStatuses保存
- ResultTask对ShuffleMapTask输出结果迭代ShuffleBlockFetcherIterator需要用到MapStatus


## ShuffleMapTask

### MapStatus

- MapStatus 数据(BlockManagerId,[compressSize])

## ShuffleRDD.compute()
- 调用BlockStoreShuffleReader.read()方法
- 

```

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }



```

## BlockStoreShuffleReader.read
- 调用 mapOutputTracker.getMapSizesByExecutorId

```

override def read(): Iterator[Product2[K, C]] = {
    val streamWrapper: (BlockId, InputStream) => InputStream = { (blockId, in) =>
      blockManager.wrapForCompression(blockId,
        CryptoStreamUtils.wrapForEncryption(in, blockManager.conf))
    }

    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      streamWrapper,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true))

    val ser = Serializer.getSerializer(dep.serializer)
    val serializerInstance = ser.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map(record => {
        readMetrics.incRecordsRead(1)
        record
      }),
      context.taskMetrics().updateShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.internalMetricsToAccumulators(
          InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }

```

## MapOutputTracker.getMapSizesByExecutorId
- 调用 MapOutputTracker.getStatuses()方法

```

/**
   * Called from executors to get the server URIs and output sizes for each shuffle block that
   * needs to be read from a given range of map output partitions (startPartition is included but
   * endPartition is excluded from the range).
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  def getMapSizesByExecutorId(shuffleId: Int, startPartition: Int, endPartition: Int)
      : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
    val statuses = getStatuses(shuffleId)
    // Synchronize on the returned array because, on the driver, it gets mutated in place
    statuses.synchronized {
      return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses)
    }
  }

```

## MapOutputTracker.getStatuses()

- 发送消息  askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
- 消息通过Outbox和Inbox进行发送和收取，最后调用MapOutputTracker.receiveAndReply处理消息
- 接收消息 : MapOutputTracker.receiveAndReply

```

 /**
   * Get or fetch the array of MapStatuses for a given shuffle ID. NOTE: clients MUST synchronize
   * on this array when reading it, because on the driver, we may be changing it in place.
   *
   * (It would be nice to remove this restriction in the future.)
   */
  private def getStatuses(shuffleId: Int): Array[MapStatus] = {
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      val startTime = System.currentTimeMillis
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {
        // Someone else is fetching it; wait for them to be done
        while (fetching.contains(shuffleId)) {
          try {
            fetching.wait()
          } catch {
            case e: InterruptedException =>
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the statuses; do so
        logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
        s"${System.currentTimeMillis - startTime} ms")

      if (fetchedStatuses != null) {
        return fetchedStatuses
      } else {
        logError("Missing all output locations for shuffle " + shuffleId)
        throw new MetadataFetchFailedException(
          shuffleId, -1, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      return statuses
    }
  }

```

## MapOutputTracker.receiveAndReply
- 调用方法tracker.post(new GetMapOutputMessage(shuffleId, context))

```


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = context.senderAddress.hostPort
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      val mapOutputStatuses = tracker.post(new GetMapOutputMessage(shuffleId, context))

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
  

```

## MapOutputTrackerMaster.post


```

  // requests for map output statuses
  private val mapOutputRequests = new LinkedBlockingQueue[GetMapOutputMessage]
  
  def post(message: GetMapOutputMessage): Unit = {
    mapOutputRequests.offer(message)
  }

```


- MapOutputTrackerMaster.MessageLoop
- 循环处理阻塞队列中的消息mapOutputRequests
- 调用方法 MapOutputTrackerMaster.getSerializedMapOutputStatuses()得到


```

 /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = mapOutputRequests.take()
             if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              mapOutputRequests.offer(PoisonPill)
              return
            }
            val context = data.context
            val shuffleId = data.shuffleId
            val hostPort = context.senderAddress.hostPort
            logDebug("Handling request to send map output locations for shuffle " + shuffleId +
              " to " + hostPort)
            val mapOutputStatuses = getSerializedMapOutputStatuses(shuffleId)
            context.reply(mapOutputStatuses)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }


```

- MapOutputTrackerMaster.getSerializedMapOutputStatuses
- 调用 MapOutputTrackerMaster.getSerializedMapOutputStatuses
- 反向推变量mapStatuses在哪里被调用，赋值

```

def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
    var statuses: Array[MapStatus] = null
    var retBytes: Array[Byte] = null
    var epochGotten: Long = -1

    // Check to see if we have a cached version, returns true if it does
    // and has side effect of setting retBytes.  If not returns false
    // with side effect of setting statuses
    def checkCachedStatuses(): Boolean = {
      epochLock.synchronized {
        if (epoch > cacheEpoch) {
          cachedSerializedStatuses.clear()
          clearCachedBroadcast()
          cacheEpoch = epoch
        }
        cachedSerializedStatuses.get(shuffleId) match {
          case Some(bytes) =>
            retBytes = bytes
            true
          case None =>
            logDebug("cached status not found for : " + shuffleId)
            //此时的mapStatuses中已有值，存储的是(shuffleId,[{BlockManagerId,[compressSize]}])
            statuses = mapStatuses.getOrElse(shuffleId, Array[MapStatus]())
            epochGotten = epoch
            false
        }
      }
    }

    if (checkCachedStatuses()) return retBytes
    var shuffleIdLock = shuffleIdLocks.get(shuffleId)
    if (null == shuffleIdLock) {
      val newLock = new Object()
      // in general, this condition should be false - but good to be paranoid
      val prevLock = shuffleIdLocks.putIfAbsent(shuffleId, newLock)
      shuffleIdLock = if (null != prevLock) prevLock else newLock
    }
    // synchronize so we only serialize/broadcast it once since multiple threads call
    // in parallel
    shuffleIdLock.synchronized {
      // double check to make sure someone else didn't serialize and cache the same
      // mapstatus while we were waiting on the synchronize
      if (checkCachedStatuses()) return retBytes

      // If we got here, we failed to find the serialized locations in the cache, so we pulled
      // out a snapshot of the locations as "statuses"; let's serialize and return that
      val (bytes, bcast) = MapOutputTracker.serializeMapStatuses(statuses, broadcastManager,
        isLocal, minSizeForBroadcast)
      logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
      // Add them into the table only if the epoch hasn't changed while we were working
      epochLock.synchronized {
        if (epoch == epochGotten) {
          cachedSerializedStatuses(shuffleId) = bytes
          if (null != bcast) cachedSerializedBroadcast(shuffleId) = bcast
        } else {
          logInfo("Epoch changed, not caching!")
          removeBroadcast(bcast)
        }
      }
      bytes
    }
  }

```

## 反向推mapStatuses
- MapOutputTrackerMaster里的变量mapStatuses在哪里被调用
- MapOutputTrackerMaster.registerMapOutputs
- 被DAGScheduler.handleTaskCompletion()方法调用

```
protected val mapStatuses = new TimeStampedHashMap[Int, Array[MapStatus]]()

  /** Register multiple map output information for the given shuffle */
  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
    mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
    if (changeEpoch) {
      incrementEpoch()
    }
  }
  
```

### DAGScheduler.handleTaskCompletion()
- ShuffleMapTask任务完成后匹配该项
- shuffleStage.addOutputLoc(smt.partitionId, status)得到ShuffleMapTask的返回值
- val status = event.result.asInstanceOf[MapStatus]
- ShuffleMapTask完成时返回MapStage: (BlockManagerId,[compressSize])
- DAGScheduler.handleTaskCompletion()被调用DAGScheduler.doOnReceive()方法中的消息类型匹配: completion @ CompletionEvent
- completion @ CompletionEvent被发出: DAGScheduler.taskEnded
- DAGScheduler.taskEnded被调用 TaskSetManager.handleSuccessfulTask()
- TaskSetManager.handleSuccessfulTask()被调用:TaskSchedulerImpl.handleSuccessfulTask()
- TaskSchedulerImpl.handleSuccessfulTask()被调用:TaskResultGetter.enqueueSuccessfulTask
- TaskResultGetter.enqueueSuccessfulTask被调用: TaskSchedulerImpl.statusUpdate()方法，此时的任务状态为TaskState.FINISHED
- TaskSchedulerImpl.statusUpdate()方法由executor中任务完成后发送给DriverEndpoint来触发

```
 case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (stageIdToStage(task.stageId).latestInfo.attemptId == task.stageAttemptId) {
              // This task was for the currently running attempt of the stage. Since the task
              // completed successfully from the perspective of the TaskSetManager, mark it as
              // no longer pending (the TaskSetManager may consider the task complete even
              // when the output needs to be ignored because the task's epoch is too small below.
              // In this case, when pending partitions is empty, there will still be missing
              // output locations, which will cause the DAGScheduler to resubmit the stage below.)
              shuffleStage.pendingPartitions -= task.partitionId
            }
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              // The epoch of the task is acceptable (i.e., the task was launched after the most
              // recent failure we're aware of for the executor), so mark the task's output as
              // available.
              shuffleStage.addOutputLoc(smt.partitionId, status)
              // Remove the task's partition from pending partitions. This may have already been
              // done above, but will not have been done yet in cases where the task attempt was
              // from an earlier attempt of the stage (i.e., not the attempt that's currently
              // running).  This allows the DAGScheduler to mark the stage as complete when one
              // copy of each task has finished successfully, even if the currently active stage
              // still has tasks running.
              shuffleStage.pendingPartitions -= task.partitionId
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              //shuffleStage.outputLocInMapOutputTrackerFormat()得到ShuffleMapTask的返回值
              //ShuffleMapTask完成时返回(BlockManagerId,[compressSize])
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocInMapOutputTrackerFormat(),
                changeEpoch = true)

              clearCacheLocs()

              if (!shuffleStage.isAvailable) {
                // Some tasks had failed; let's resubmit this shuffleStage.
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                submitStage(shuffleStage)
              } else {
                // Mark any map-stage jobs waiting on this stage as finished
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  for (job <- shuffleStage.mapStageJobs) {
                    markMapStageJobAsFinished(job, stats)
                  }
                }
              }

              // Note: newly runnable stages will be submitted below when we submit waiting stages
            }
        }

```








end