# Spark2.4.0源码分析之WorldCount 任务调度器(七)

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0


## 时序图
- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/worldCount.taskScheduler.jpg
![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/worldCount.taskScheduler.jpg)


 
## 主要内容描述
- 理解TaskSet是如何提交到任务调度器池，任务集如何被调度
- 理解Worker可用资源算法，Worker可用资源分配任务调度池中的任务
- 任务发送给executor去执行


## 程序

### TaskSchedulerImpl.submitTasks
- 任务调度器，处理任务集
- 将任务集转化成TaskSetManager,因为TaskSetManager继承Schedulable，调度池中放的元素为Schedulable,调度池来调度任务，所以需要将TaskSet转化成可调度的对象TaskSetManager
```
val manager = createTaskSetManager(taskSet, maxTaskFailures)
```

```
  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt)
  }

```
- TaskSetManager加到调度池中，供任务调度器调度，也就是由高度池决定，TaskSet里边的任务什么时候被调用
- SparkContext对象构建时，已经构建了默认的FIFO调度模式，就是先进先出，先来的先开始调度
```
schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
```

- 15秒后开始执行，如果hasLaunchedTask = true,说明任务调度器已经分配当前TaskSet中的任务，发送给Executor去执行
- hasLaunchedTask = false,说明15秒后，当前TaskSet中的任务还没有发送给Executor去执行，说明没有可用的资源分配，所以任务调度器才没有把任务分配出去，所以就进行集群没有可用的资源分配的提示

```
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
```

- StandaloneSchedulerBackend.reviveOffers()调度，StandaloneSchedulerBackend没有重写reviveOffers()函数，所以调用CoarseGrainedSchedulerBackend.reviveOffers
```
 backend.reviveOffers()
```

- TaskSchedulerImpl.submitTasks函数

```
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

### CoarseGrainedSchedulerBackend.reviveOffers

- 给Driver发送消息:ReviveOffers
- DriverEndpoint.receive()函数会接收消息，进行消息类型匹配，匹配上后就进行处理

```
  override def reviveOffers() {
    driverEndpoint.send(ReviveOffers)
  }
```

### CoarseGrainedSchedulerBackend.DriverEndpoint.recieve
- DriverEndpoint.receive()接收到消息:ReviveOffers
- 调用CoarseGrainedSchedulerBackend.DriverEndpoint.makeOffers()函数，来计算可用的资源，去分配任务

```
override def receive: PartialFunction[Any, Unit] = {
      case StatusUpdate(executorId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              executorInfo.freeCores += scheduler.CPUS_PER_TASK
              makeOffers(executorId)
            case None =>
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                s"from unknown executor with ID $executorId")
          }
        }

      case ReviveOffers =>
        makeOffers()

      case KillTask(taskId, executorId, interruptThread, reason) =>
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(
              KillTask(taskId, executorId, interruptThread, reason))
          case None =>
            // Ignoring the task kill since the executor is not registered.
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }

      case KillExecutorsOnHost(host) =>
        scheduler.getExecutorsAliveOnHost(host).foreach { exec =>
          killExecutors(exec.toSeq, adjustTargetNumExecutors = false, countFailures = false,
            force = true)
        }

      case UpdateDelegationTokens(newDelegationTokens) =>
        executorDataMap.values.foreach { ed =>
          ed.executorEndpoint.send(UpdateDelegationTokens(newDelegationTokens))
        }

      case RemoveExecutor(executorId, reason) =>
        // We will remove the executor's state and cannot restore it. However, the connection
        // between the driver and the executor may be still alive so that the executor won't exit
        // automatically, so try to tell the executor to stop itself. See SPARK-13519.
        executorDataMap.get(executorId).foreach(_.executorEndpoint.send(StopExecutor))
        removeExecutor(executorId, reason)
    }


```

### CoarseGrainedSchedulerBackend.DriverEndpoint.makeOffers()
- 过滤有效的executor
```
 val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
```
- 有效的executor计算可用的Worker资源
```
 val workOffers = activeExecutors.map {
          case (id, executorData) =>
            new WorkerOffer(id, executorData.executorHost, executorData.freeCores,
              Some(executorData.executorAddress.hostPort))
        }.toIndexedSeq
```
- scheduler.resourceOffers(workOffers),调度器为TaskSchedulerImpl，该函数内部执行，在可用的worker上去分配任务，会返回待分配的任务
- CoarseGrainedSchedulerBackend.DriverEndpoint
.launchTasks()函数，会给executor去发送消息:LaunchTask,Executor收到该消息，会去启动该任务，并运行，相当于执行该任务

```
    // Make fake resource offers on all executors
    private def makeOffers() {
      // Make sure no executor is killed while some task is launching on it
      val taskDescs = CoarseGrainedSchedulerBackend.this.synchronized {
        // Filter out executors under killing
        val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
        val workOffers = activeExecutors.map {
          case (id, executorData) =>
            new WorkerOffer(id, executorData.executorHost, executorData.freeCores,
              Some(executorData.executorAddress.hostPort))
        }.toIndexedSeq
        scheduler.resourceOffers(workOffers)
      }
      if (!taskDescs.isEmpty) {
        launchTasks(taskDescs)
      }
    }


```


### TaskSchedulerImpl.resourceOffers

- 对worker资源进行黑名单过滤

```
 val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
      offers.filter { offer =>
        !blacklistTracker.isNodeBlacklisted(offer.host) &&
          !blacklistTracker.isExecutorBlacklisted(offer.executorId)
      }
    }.getOrElse(offers)
```
- 对worker资源进行打散，使所有的worker都更能均匀的分配到任务
```
val shuffledOffers = shuffleOffers(filteredOffers)
```

- 计算worker上还剩多少可用的cpu core
```
val availableCpus = shuffledOffers.map(o => o.cores).toArray
```
- 从任务调度池中取出已排好序的所有的可调度元素(TaskSetManager)
```
val sortedTaskSets = rootPool.getSortedTaskSetQueue
```
- 用的默认FIFO调度算法，先来的任务先分配
```
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }

```


- 返回对象 Vector(ArrayBuffer,ArrayBuffer),理解为，每台worker分配几个任务，这个时修还没有开始分配，只是先实例化对象
```
 val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
```

- 循环分配TaskSet中的任务给tasks变量，分配任务的规则,遍历所有可用的worker资源，首先每台worker上分配任务集中的一个任务，如果资源没分配完，会再循环一次，再给可用的worker每台分配一个任务，直至，可用的资源分配完了，或任务集中的任务分配完了，就本次分配完成，把分配好的tasks变量返回出去

```
 var launchedTaskAtCurrentMaxLocality = false
          do {
            launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(taskSet,
              currentMaxLocality, shuffledOffers, availableCpus, tasks, addressesWithDescs)
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality
          } while (launchedTaskAtCurrentMaxLocality)
```






- TaskSchedulerImpl.resourceOffers函数

```
 /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Before making any offers, remove any nodes from the blacklist whose blacklist has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the blacklist is only relevant when task offers are being made.
    blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())

    val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
      offers.filter { offer =>
        !blacklistTracker.isNodeBlacklisted(offer.host) &&
          !blacklistTracker.isExecutorBlacklisted(offer.executorId)
      }
    }.getOrElse(offers)

    val shuffledOffers = shuffleOffers(filteredOffers)
    // Build a list of tasks to assign to each worker.
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val availableSlots = shuffledOffers.map(o => o.cores / CPUS_PER_TASK).sum
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    for (taskSet <- sortedTaskSets) {
      // Skip the barrier taskSet if the available slots are less than the number of pending tasks.
      if (taskSet.isBarrier && availableSlots < taskSet.numTasks) {
        // Skip the launch process.
        // TODO SPARK-24819 If the job requires more slots than available (both busy and free
        // slots), fail the job on submit.
        logInfo(s"Skip current round of resource offers for barrier stage ${taskSet.stageId} " +
          s"because the barrier taskSet requires ${taskSet.numTasks} slots, while the total " +
          s"number of available slots is $availableSlots.")
      } else {
        var launchedAnyTask = false
        // Record all the executor IDs assigned barrier tasks on.
        val addressesWithDescs = ArrayBuffer[(String, TaskDescription)]()
        for (currentMaxLocality <- taskSet.myLocalityLevels) {
          var launchedTaskAtCurrentMaxLocality = false
          do {
            launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(taskSet,
              currentMaxLocality, shuffledOffers, availableCpus, tasks, addressesWithDescs)
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality
          } while (launchedTaskAtCurrentMaxLocality)
        }
        if (!launchedAnyTask) {
          taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
        }
        if (launchedAnyTask && taskSet.isBarrier) {
          // Check whether the barrier tasks are partially launched.
          // TODO SPARK-24818 handle the assert failure case (that can happen when some locality
          // requirements are not fulfilled, and we should revert the launched tasks).
          require(addressesWithDescs.size == taskSet.numTasks,
            s"Skip current round of resource offers for barrier stage ${taskSet.stageId} " +
              s"because only ${addressesWithDescs.size} out of a total number of " +
              s"${taskSet.numTasks} tasks got resource offers. The resource offers may have " +
              "been blacklisted or cannot fulfill task locality requirements.")

          // materialize the barrier coordinator.
          maybeInitBarrierCoordinator()

          // Update the taskInfos into all the barrier task properties.
          val addressesStr = addressesWithDescs
            // Addresses ordered by partitionId
            .sortBy(_._2.partitionId)
            .map(_._1)
            .mkString(",")
          addressesWithDescs.foreach(_._2.properties.setProperty("addresses", addressesStr))

          logInfo(s"Successfully scheduled all the ${addressesWithDescs.size} tasks for barrier " +
            s"stage ${taskSet.stageId}.")
        }
      }
    }

    // TODO SPARK-24823 Cancel a job that contains barrier stage(s) if the barrier tasks don't get
    // launched within a configured time.
    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

```

### TaskSchedulerImpl.resourceOfferSingleTaskSet
- 遍历所有的可用worker资源，进行TaskSet中的任务分配，每个worker分配一个任务，分配完后，返回，如果还可以继续分配，下次循环再分配，如此，分配完所有的worker可用资源，或者是分配完所有的TaskSet中的任务

```
 private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]],
      addressesWithDescs: ArrayBuffer[(String, TaskDescription)]) : Boolean = {
    var launchedTask = false
    // nodes and executors that are blacklisted for the entire application have already been
    // filtered out by this point
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager.put(tid, taskSet)
            taskIdToExecutorId(tid) = execId
            executorIdToRunningTaskIds(execId).add(tid)
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            // Only update hosts for a barrier task.
            if (taskSet.isBarrier) {
              // The executor address is expected to be non empty.
              addressesWithDescs += (shuffledOffers(i).address.get -> task)
            }
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

```


### CoarseGrainedSchedulerBackend.DriverEndpoint.launchTasks
- 循环所有的任务，依次把任务发送给executor执行
- 到这里任务集转化成TaskSetManager做为可调度元素，经调度器默认FIFO算法调度，对worker上的可用资源分配任务，把任务分配给executor上去执行，任务调度器任务调度的流程已完成


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

end