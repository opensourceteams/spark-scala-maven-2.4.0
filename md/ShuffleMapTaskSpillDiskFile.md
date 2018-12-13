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

## 内存和SpillFile合并
- 把数据insertAll()到ExternalSorter中，完成后，此时如果数据大的话，会进行溢出到临时文件的操作，数据写到临时文件后
- 把当前内存中的数据和临时文件中的数据进行合并数据文件,合并后的文件只包含(key,value)，并且是按partition升序排序，然后按key升序排序，输出文件名称：ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID) + UUID 即:"shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data" + UUID,reduceId为默认值0
- 还会有一份索引文件： "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index" + "." +UUID,索引文件依次存储每个partition的位置偏移量
- 数据文件的写入分两种情况，一种是直接内存写入，没有溢出临时文件到磁盘中，这种是直接在内存中操作的（数据量相对小些），另外单独分析
- 一种是有磁盘溢出文件的，这种情况是本文重点分析的情况
- ExternalSorter.partitionedIterator()方法可以处理所有磁盘中的临时文件和内存中的文件，返回一个可迭代的对象，里边放的元素为reduce用到的(partition,Iterator(key,value)),迭代器中的数据是按key升序排序的
- 具体是通过ExternalSorter.mergeWithAggregation(),遍历每一个临时文件中当前partition的数据和内存中当前partition的数据，注意，临时文件数据读取时是按partition为0开始依次遍历的














end



end