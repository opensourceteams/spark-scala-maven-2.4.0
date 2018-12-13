# Spark 源码分析之ShuffleMapTask内存数据Spill和合并

## 前置条件
## 技能标签
## 内存中数据Spill到磁盘

- ShuffleMapTask进行当前分区的数据读取(此时读的是HDFS的当前分区,注意还有一个reduce分区，也就是ShuffleMapTask输出文件是已经按Reduce分区处理好的)
- SparkEnv指定默认的SortShuffleManager,getWriter()中匹配BaseShuffleHandle对象，返回SortShuffleWriter对象
- SortShuffleWriter，用的是ExternalSorter(外部排序对象进行排序处理),会把rdd.iterator(partition, context)的数据通过iterator插入到ExternalSorter中PartitionedAppendOnlyMap对象中做为内存中的map对象数据,每插入一条(key,value)的数据后，会对当前的内存中的集合进行判断，如果满足溢出文件的条件，就会把内存中的数据写入到SpillFile文件中
- 满中溢出文件的条件是，每插入32条数据，并且，当前集合中的数据估值大于等于5m时，进行一次判断，会通过算法验证对内存的影响，确定是否可以溢出内存中的数据到文件，如果满足就把当前内存中的所有数据写到磁盘spillFile文件中
- SpillFile调用org.apache.spark.util.collection.ExternalSorter.SpillableIterator.spill()方法处理
- WritablePartitionedIterator迭代对象对内存中的数据进行迭代,DiskBlockObjectWriter对象写入磁盘，写入的数据格式为(key,value),不带partition的
- ExternalSorter.spillMemoryIteratorToDisk()这个方法将内存数据迭代对象WritablePartitionedIterator写入到一个临时文件，SpillFile临时文件用DiskBlockObjectWriter对象来写入数据
- 临时文件的格式temp_local_+UUID
- 遍历内存中的数据写入到临时文件，会记录每个临时文件中每个分区的(key,value)各有多少个，elementsPerPartition(partitionId) += 1
  如果说数据很大的话，会每默认每10000条数据进行Flush()一次数据到文件中，会记录每一次Flush的数据大小batchSizes入到ArrayBuffer中保存
- 并且在数据写入前，会进行排序，先按key的hash分区，先按partition的升序排序，再按key的升序排序，这样来写入文件中，以保证读取临时文件时可以分隔开每个临时文件的每个分区的数据,对于一个临时文件中一个分区的数据量比较大的话，会按流一批10000个(key,value)进行读取，读取的大小讯出在batchSizes数据中，就样读取的时候就非常方便了

## 内存数据Spill和合并
- 把数据insertAll()到ExternalSorter中，完成后，此时如果数据大的话，会进行溢出到临时文件的操作，数据写到临时文件后
- 把当前内存中的数据和临时文件中的数据进行合并数据文件,合并后的文件只包含(key,value)，并且是按partition升序排序，然后按key升序排序，输出文件名称：ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID) + UUID 即:"shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data" + UUID,reduceId为默认值0
- 还会有一份索引文件： "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index" + "." +UUID,索引文件依次存储每个partition的位置偏移量
- 数据文件的写入分两种情况，一种是直接内存写入，没有溢出临时文件到磁盘中，这种是直接在内存中操作的（数据量相对小些），另外单独分析
- 一种是有磁盘溢出文件的，这种情况是本文重点分析的情况
- ExternalSorter.partitionedIterator()方法可以处理所有磁盘中的临时文件和内存中的文件，返回一个可迭代的对象，里边放的元素为reduce用到的(partition,Iterator(key,value)),迭代器中的数据是按key升序排序的
- 具体是通过ExternalSorter.mergeWithAggregation(),遍历每一个临时文件中当前partition的数据和内存中当前partition的数据，注意，临时文件数据读取时是按partition为0开始依次遍历的


## 源码分析(内存中数据Spill到磁盘)
### ShuffleMapTask
- 调用ShuffleMapTask.runTask()方法处理当前HDFS分区数据
- 调用SparkEnv.get.shuffleManager得到SortShuffleManager
- SortShuffleManager.getWriter()得到SortShuffleWriter
- 调用SortShuffleWriter.write()方法

- SparkEnv.create()
```
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager",
      "tungsten-sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager")
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

```
```
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
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
### SortShuffleWriter
- 调用SortShuffleWriter.write()方法
- 根据RDDDependency中mapSideCombine是否在map端合并，这个是由算子决定，reduceByKey中mapSideCombine为true,groupByKey中mapSideCombine为false,会new ExternalSorter()外部排序对象进行排序
- 然后把records中的数据插入ExternalSorter对象sorter中，数据来源是HDFS当前的分区

```
/** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }
```

- ExternalSorter.insertAll()方法
- 该方法会把迭代器records中的数据插入到外部排序对象中
- ExternalSorter中的数据是不进行排序的，是以数组的形式存储的,健存的为(partition,key)，值为Shuffle之前的RDD链计算结果
  在内存中会对相同的key,进行合并操作，就是map端本地合并，合并的函数就是reduceByKey(_+_)这个算子中定义的函数
- maybeSpillCollection方法会判断是否满足磁盘溢出到临时文件，满足条件，会把当前内存中的数据写到磁盘中,写到磁盘中的数据是按partition升序排序，再按key升序排序，就是(key,value)的临时文件，不带partition,但是会记录每个分区的数量elementsPerPartition(partitionId- 记录每一次Flush的数据大小batchSizes入到ArrayBuffer中保存
- 内存中的数据存在PartitionedAppendOnlyMap，记住这个对象，后面排序用到了这个里边的排序算法

```
@volatile private var map = new PartitionedAppendOnlyMap[K, C]

def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()
        kv = records.next()
        map.changeValue((getPartition(kv._1), kv._1), update)
        maybeSpillCollection(usingMap = true)
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
      }
    }
  }

```

- ExternalSorter.maybeSpillCollection
- estimatedSize当前内存中数据预估占内存大小
- maybeSpill满足Spill条件就把内存中的数据写入到临时文件中
- 调用ExternalSorter.maybeSpill()

```
/**
   * Spill the current in-memory collection to disk if needed.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }
```

- ExternalSorter.maybeSpill()
- 对内存中的数据遍历时，每遍历32个元素，进行判断，当前内存是否大于5m，如果大于5m，再进行内存的计算，如果满足就把内存中的数据写到临时文件中
- 如果满足条件，调用ExternalSorter.spill()方法，将内存中的数据写入临时文件

```

 /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = acquireOnHeapMemory(amountToRequest)
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) {
      _spillCount += 1
      logSpillage(currentMemory)
      spill(collection)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      releaseMemory()
    }
    shouldSpill
  }

```

- ExternalSorter.spill()
- 调用方法collection.destructiveSortedWritablePartitionedIterator进行排序，即调用PartitionedAppendOnlyMap.destructiveSortedWritablePartitionedIterator进行排序()方法排序,最终会调用WritablePartitionedPairCollection.destructiveSortedWritablePartitionedIterator()排序，调用方法WritablePartitionedPairCollection.partitionedDestructiveSortedIterator()，没有实现，调用子类PartitionedAppendOnlyMap.partitionedDestructiveSortedIterator()方法
- 调用方法ExternalSorter.spillMemoryIteratorToDisk() 将磁盘中的数据写入到spillFile临时文件中

```
  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    spills.append(spillFile)
  }
```

- PartitionedAppendOnlyMap.partitionedDestructiveSortedIterator()调用排序算法WritablePartitionedPairCollection.partitionKeyComparator
- 即先按分区数的升序排序，再按key的升序排序
```
/**
 * Implementation of WritablePartitionedPairCollection that wraps a map in which the keys are tuples
 * of (partition ID, K)
 */
private[spark] class PartitionedAppendOnlyMap[K, V]
  extends SizeTrackingAppendOnlyMap[(Int, K), V] with WritablePartitionedPairCollection[K, V] {

  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    destructiveSortedIterator(comparator)
  }

  def insert(partition: Int, key: K, value: V): Unit = {
    update((partition, key), value)
  }
}

  /**
   * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering.
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] = {
    new Comparator[(Int, K)] {
      override def compare(a: (Int, K), b: (Int, K)): Int = {
        val partitionDiff = a._1 - b._1
        if (partitionDiff != 0) {
          partitionDiff
        } else {
          keyComparator.compare(a._2, b._2)
        }
      }
    }
  }
}
```

- ExternalSorter.spillMemoryIteratorToDisk()
- 创建blockId : temp_shuffle_ + UUID
- 溢出到磁盘临时文件: temp_shuffle_ + UUID
- 遍历内存数据inMemoryIterator写入到磁盘临时文件spillFile
- 遍历内存中的数据写入到临时文件，会记录每个临时文件中每个分区的(key,value)各有多少个，elementsPerPartition(partitionId) 如果说数据很大的话，会每默认每10000条数据进行Flush()一次数据到文件中，会记录每一次Flush的数据大小batchSizes入到ArrayBuffer中保存

```
/**
   * Spill contents of in-memory iterator to a temporary file on disk.
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator)
      : SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    var spillMetrics: ShuffleWriteMetrics = null
    var writer: DiskBlockObjectWriter = null
    def openWriter(): Unit = {
      assert (writer == null && spillMetrics == null)
      spillMetrics = new ShuffleWriteMetrics
      writer = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)
    }
    openWriter()

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is closed at the end of this process, and cannot be reused.
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += spillMetrics.shuffleBytesWritten
      batchSizes.append(spillMetrics.shuffleBytesWritten)
      spillMetrics = null
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        inMemoryIterator.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
          openWriter()
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        w.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }


```

## 源码分析(内存数据Spill合并)

### SortShuffleWriter.insertAll
- 即内存中的数据，如果有溢出，写入到临时文件后，可能会有多个临时文件(看数据的大小)
- 这时要开始从所有的临时文件中，shuffle出按给reduce输入数据(partition,Iterator),相当于要对多个临时文件进行合成一个文件，合成的结果按partition升序排序，再按Key升序排序


- SortShuffleWriter.write
- 得到合成文件shuffleBlockResolver.getDataFile : 格式如  "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data" + "." + UUID,reduceId为默认的0
- 调用关键方法ExternalSorter的sorter.writePartitionedFile，这才是真正合成文件的方法
- 返回值partitionLengths，即为数据文件中对应索引文件按分区从0到最大分区，每个分区的数据大小的数组



```
 /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

```

- ExternalSorter.writePartitionedFile
- 按方法名直译，把数据写入已分区的文件中
- 如果没有spill文件，直接按ExternalSorter在内存中排序，用的是TimSort排序算法排序，单独合出来讲，这里不详细讲
- 如果有spill文件，是我们重点分析的，这个时候，调用this.partitionedIterator按回按[(partition,Iterator)],按分区升序排序，按(key,value)中key升序排序的数据,并键中方法this.partitionedIterator()
- 写入合并文件中，并返回写入合并文件中每个分区的长度，放到lengths数组中，数组索引就是partition

```
/**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  def writePartitionedFile(
      blockId: BlockId,
      outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)

    if (spills.isEmpty) {
      // Case where we only have in-memory data
      val collection = if (aggregator.isDefined) map else buffer
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
          context.taskMetrics.shuffleWriteMetrics.get)
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        writer.commitAndClose()
        val segment = writer.fileSegment()
        lengths(partitionId) = segment.length
      }
    } else {
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
            context.taskMetrics.shuffleWriteMetrics.get)
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          writer.commitAndClose()
          val segment = writer.fileSegment()
          lengths(id) = segment.length
        }
      }
    }

    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.internalMetricsToAccumulators(
      InternalAccumulator.PEAK_EXECUTION_MEMORY).add(peakMemoryUsedBytes)

    lengths
  }

```

- this.partitionedIterator()
- 直接调用ExternalSorter.merge()方法
- 临时文件参数spills
- 内存文件排序算法在这里调用collection.partitionedDestructiveSortedIterator(comparator)，实际调的是PartitionedAppendOnlyMap.partitionedDestructiveSortedIterator,定义了排序算法partitionKeyComparator，即按partition升序排序，再按key升序排序


```
/**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   * Exposed for testing.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (!ordering.isDefined) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(None)))
      } else {
        // We do need to sort by both partition ID and key
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      // Merge spilled and in-memory data
      merge(spills, destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }
  
- ExternalSorter.merge()方法
- 0 until numPartitions 从0到numPartitions(不包含)分区循环调用
- IteratorForPartition(p, inMemBuffered),每次取内存中的p分区的数据
- readers是每个分区是读所有的临时文件(因为每份临时文件，都有可能包含p分区的数据)，
- readers.map(_.readNextPartition())该方法内部用的是每次调一个分区的数据，从0开始，刚好对应的是p分区的数据
- readNextPartition方法即调用SpillReader.readNextPartition()方法

```
 /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   *
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {
    val readers = spills.map(new SpillReader(_))
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (aggregator.isDefined) {
        // Perform partial aggregation across partitions
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }


```
  

- SpillReader.readNextPartition()

```

def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

```





end



end