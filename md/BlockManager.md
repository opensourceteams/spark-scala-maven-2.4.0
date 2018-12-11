# Spark BlockManager


## 描述

## ShuffleRDD读取数据
- ShuffleRDD读到的数据是上一个RDD输出数据，即ShuffleMapTask的输出结果(一个分区数据文件和一个分区对应的偏移量文件)
- 分区数据文件是有序的，先按分区数从小到大排序，再按(key,value)中的key从小到大进行排序
- 通过BlockStoreShuffleReader.read()对Block数据进行读取
- 通过 MapOutputTracker得到[(BlockManagerId,[(ShuffleBlockId,长度)])],相当于有多少台机器进行executor运算，就有多少个BlockManagerId(即有多少个BlockManager进行管理),每个BlockManager管理着多个数据分区(即多个ShuffleBlockId)
- 通过 ShuffleBlockFetcherIterator()进行本地Block和远程Block数据进行处理，会得到一个 (BlockID, InputStream)这样元素的迭代器，然后进行flatMap()操作，返序列化流对象并转化为KeyValueIterator迭代器，通过两个迭代器就可以遍历当前分区的所有输入数据，由于Block数据块默认的大小是128m，也就是每个BlockID最大数据大小是128m，而且是一次只处理一个BlockID数据，所以此时是不会内存益处的
- 然后对迭代器中的(key,value)元素进行遍历，并插入ExternalAppendOnlyMap对象，如果数据满足插入临时文件的条件，会把内存中的数据插入到临时文件，并保存DiskMapIterator对象到变量spilledMaps中，就是防止数据太大，内存溢出

## 本地Block
- 本地Block是BlockManager负责拉取
- 即ShuffleMapTask和ResultTask在同一台机器上运行时，这个时候只需要通过

## 远程Block
- 远和Block是ShuffleClient负责调用，BlockTransferService负责拉取


### 
- ResultTask读取ShuffleMapTask输出的数据，ResultTask读取的RDD分区文件分两种，一种中本台机器上的数据文件(也就是ResultTask和ShuffleMapTask在同一台机器上),这时能过BlockManager就可以读到对应分区的数据文件


### ShuffleRDD

- ShuffleRDD.compute()方法

```

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    //SortShuffleManager.getReader()方法反回BlockStoreShuffleReader
    //调用BlockStoreShuffleReader.read()方法
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }
  
```

- BlockStoreShuffleReader.read()方法
- 读到当前reduce task 的当前分区的需要合并的key-values

```
/** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val streamWrapper: (BlockId, InputStream) => InputStream = { (blockId, in) =>
      blockManager.wrapForCompression(blockId,
        CryptoStreamUtils.wrapForEncryption(in, blockManager.conf))
    }

    //通过 ShuffleBlockFetcherIterator()进行本地Block和远程Block数据进行处理，会得到一个 (BlockID, InputStream)这样元素的迭代器，然后进行flatMap()操作，返序列化流对象并转化为KeyValueIterator迭代器，通过两个迭代器就可以遍历当前分区的所有输入数据，由于Block数据块默认的大小是128m，也就是每个BlockID最大数据大小是128m，而且是一次只处理一个BlockID数据，所以此时是不会内存益处的
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      //通过 MapOutputTracker得到[(BlockManagerId,[(ShuffleBlockId,长度)])],相当于有多少台机器进行executor运算，就有多少个BlockManagerId(即有多少个BlockManager进行管理),每个BlockManager管理着多个数据分区(即多个ShuffleBlockId)
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
        //然后对迭代器中的(key,value)元素进行遍历，并插入ExternalAppendOnlyMap对象，如果数据满足插入临时文件的条件，会把内存中的数据插入到临时文件，并保存DiskMapIterator对象到变量spilledMaps中，就是防止数据太大，内存溢出
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
    //如查定义的排序就进行排序操作，如果没有就不排序
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

- ShuffleBlockFetcherIterator.initialize()

```

 private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    //拆分本地Block 和 远程Block
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    
    //随机取远程的
    fetchRequests ++= Utils.randomize(remoteRequests)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    //处理本地Block
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }
  
```
### 本地Block处理

- ShuffleBlockFetcherIterator.fetchLocalBlocks()
- 拉取本地Block
- 对localBlocks进行迭代，

```
  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * [[ManagedBuffer]]'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
      //BlockManager.getBlockData 
      //buf等于  (ShuffleBlockId,Block数据文件偏移量，长度，数据文件位置)
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }

```

- BlockManager.getBlockData  

```


 /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
    //shuffleManager.shuffleBlockResolver得到 IndexShuffleBlockResolver
    //调用IndexShuffleBlockResolver.getBlockData
    //得到 FileSegmentManagedBuffer的对象，该对象存放在当前(ShuffleBlockId,Block数据文件偏移量，长度，数据文件位置)
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      val blockBytesOpt = doGetLocal(blockId, asBlockResult = false)
        .asInstanceOf[Option[ByteBuffer]]
      if (blockBytesOpt.isDefined) {
        val buffer = blockBytesOpt.get
        new NioManagedBuffer(buffer)
      } else {
        throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

```

- IndexShuffleBlockResolver.getBlockData

```

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      ByteStreams.skipFully(in, blockId.reduceId * 8)
      val offset = in.readLong()
      val nextOffset = in.readLong()
      //(ShuffleBlockId,Block数据文件偏移量，长度，数据文件位置)
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset)
    } finally {
      in.close()
    }
  }
  

```

### 远程Block处理

- ShuffleBlockFetcherIterator.initialize

```

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    // 折分本地Block和远程Block
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

```

- ShuffleBlockFetcherIterator.splitLocalRemoteBlocks

```

private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          if (curRequestSize >= targetRequestSize) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }



```

end