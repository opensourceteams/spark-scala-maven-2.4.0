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
- maybeSpillCollection方法会判断是否满足磁盘溢出到临时文件，满足条件，会把当前内存中的数据写到磁盘中,写到磁盘中的数据是按partition升序排序，再按key升序排序，就是(key,value)的临时文件，不带partition,但是会记录每个分区的数量elementsPerPartition(partitionId) ，记录每一次Flush的数据大小batchSizes入到ArrayBuffer中保存


```
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


## 源码分析(内存数据Spill和合并)











end



end