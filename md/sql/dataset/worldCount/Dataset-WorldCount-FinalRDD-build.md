# Spark2.4.0源码分析之WorldCount FinalRDD构建(一)

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0

## 主要内容描述
- Spark dataSet执行计算转成FinalRDD
- FinalRdd从第一个RDD到最到一个RDD的转化过程
- RDD之间的依赖引用关系
- ShuffleRowRDD默认分区器为HashPartitioning,实际new Partitioner,分区个数为200

## FinalRDD 层级
```
FileScanRDD [0]
MapPartitionsRDD [1]
MapPartitionsRDD [2]
MapPartitionsRDD [3]
MapPartitionsRDD [4]
MapPartitionsRDD [5]
MapPartitionsRDD [6]
ShuffledRowRDD [7]
MapPartitionsRDD [8]
MapPartitionsRDD [9]

```

## FinalRDD DAG Visualization
![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/dagVisualization/stage-0.png)

![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/example/spark-sql-dataset/worldCount/dagVisualization/stage-1.png)

## 输入数据

```
a b a a
c a a

```

## 客户端程序

###  BaseSparkSession
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

###  worldCount
```
package com.opensource.bigdata.spark.standalone.wordcount.spark.session


import com.opensource.bigdata.spark.standalone.base.BaseSparkSession



object WorldCount extends BaseSparkSession{


  def main(args: Array[String]): Unit = {
    appName = "WorldCount"


    val spark = sparkSession(false,false,false,7)
    import spark.implicits._

    val distFile = spark.read.textFile("data/text/worldCount.txt")

    val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(x => x ).count()



    println(s"${dataset.collect().mkString("\n\n")}")




    spark.stop()


  }
}

```

## 时序图
![](leanote://file/getImage?fileId=5c356ffcf8392e6468000002)

## 源码分析

### 客户端调用collect()函数

- 程序的入口
- 调用Dataset.collect()触发处理程序

```
package com.opensource.bigdata.spark.standalone.wordcount.spark.session


import com.opensource.bigdata.spark.standalone.base.BaseSparkSession



object WorldCount extends BaseSparkSession{


  def main(args: Array[String]): Unit = {
    appName = "WorldCount"


    val spark = sparkSession(false,false,false,7)
    import spark.implicits._

    val distFile = spark.read.textFile("data/text/worldCount.txt")

    val dataset = distFile.flatMap( line => line.split(" ")).groupByKey(x => x ).count()

    println(s"${dataset.collect().mkString("\n\n")}")
    spark.stop()
  }
}



```

### Dataset.collect()
- 调用函数withAction()得到QueryExecution对象WholeStageCodegenExec
- 在函数withAction()调用collectFromPlan,即WholeStageCodegenExec.collectFromPlan

```
/**
   * Returns an array that contains all rows in this Dataset.
   *
   * Running collect requires moving all the data into the application's driver process, and
   * doing so on a very large dataset can crash the driver process with OutOfMemoryError.
   *
   * For Java API, use [[collectAsList]].
   *
   * @group action
   * @since 1.6.0
   */
  def collect(): Array[T] = withAction("collect", queryExecution)(collectFromPlan)

```


### Dataset.withAction
- action(qe.executedPlan)调用collectFromPlan,即WholeStageCodegenExec.collectFromPlan

```
 /**
   * Wrap a Dataset action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions.
   */
  private def withAction[U](name: String, qe: QueryExecution)(action: SparkPlan => U) = {
    try {
      qe.executedPlan.foreach { plan =>
        plan.resetMetrics()
      }
      val start = System.nanoTime()
      val result = SQLExecution.withNewExecutionId(sparkSession, qe) {
        action(qe.executedPlan)
      }
      val end = System.nanoTime()
      sparkSession.listenerManager.onSuccess(name, qe, end - start)
      result
    } catch {
      case e: Exception =>
        sparkSession.listenerManager.onFailure(name, qe, e)
        throw e
    }
  }

```

### Dataset.collectFromPlan
- 调用executeCollect()函数，得到作业处理结果，即worldCount统计结果，
- row 得到一条记录，此时为UnsafeRow,里边存着Tuple2(key,value)
- row.getUTF8String(0) 得到当前的单词
- row.getInt(1)  得到当前单词的个数
- plan.executeCollect()是计算结果的函数,即SparkPaln.executeCollect


```
  /**
   * Collect all elements from a spark plan.
   */
  private def collectFromPlan(plan: SparkPlan): Array[T] = {
    // This projection writes output to a `InternalRow`, which means applying this projection is not
    // thread-safe. Here we create the projection inside this method to make `Dataset` thread-safe.
    val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
    plan.executeCollect().map { row =>
      // The row returned by SafeProjection is `SpecificInternalRow`, which ignore the data type
      // parameter of its `get` method, so it's safe to use null here.
      objProj(row).get(0, null).asInstanceOf[T]
    }
  }

```

### SparkPaln.executeCollect
- getByteArrayRdd() 该函数，是通过执行计划得到FinalRdd的函数，也就是将执行计划转成FinalRDD的函数，本节主要分析这个函数中的内容，即FinalRDD是如何转换而来的
- byteArrayRdd.collect() 调用RDD.collect()函数，触发作业处理，该函数会去计算RDD中的WorldCount个数，即我们需要的结果
- 拿到结果后再遍历一次，对数据进行decode,解码，因为数据在计算过程中是需要进行传输处理，为了提高性能，数据在传输时是进行编码的(可以理解为压缩)

```
  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[InternalRow] = {
    val byteArrayRdd = getByteArrayRdd()

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { countAndBytes =>
      decodeUnsafeRows(countAndBytes._2).foreach(results.+=)
    }
    results.toArray
  }
```


### SparkPlan.getByteArrayRdd
- 调用execute()函数得到rdd,即调用WholeStageCodegenExec.doExecute()函数
- execute().mapPartitionsInternal此时得到的RDD为:MapPartitionsRDD [9]
- 注意，关注该RDD的上级RDD是如何转化而来的

```
  /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   */
  private def getByteArrayRdd(n: Int = -1): RDD[(Long, Array[Byte])] = {
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))
      // `iter.hasNext` may produce one row and buffer it, we should only call it when the limit is
      // not hit.
      while ((n < 0 || count < n) && iter.hasNext) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        count += 1
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator((count, bos.toByteArray))
    }
  }

```


### WholeStageCodegenExec.doExecute
- 该函数主要是调用child.asInstanceOf[CodegenSupport].inputRDDs() 来得到上级RDD
- 然后进行mapPartitionsWithIndex RDD转换得到新RDD:MapPartitionsRDD [8]
- 注意：WholeStageCodegenExec.doExecute()函数会被递归调用的，当执行计划ExchangeCoordinator为None时会计算ShuffleDependency,ShuffleDependency会计算上级RDD，所以此处会递归调用
- 此时的child为HashAggregateExec,调用HashAggregateExec.inputRDDs()函数

```
 override def doExecute(): RDD[InternalRow] = {
    val (ctx, cleanedSource) = doCodeGen()
    // try to compile and fallback if it failed
    val (_, maxCodeSize) = try {
      CodeGenerator.compile(cleanedSource)
    } catch {
      case NonFatal(_) if !Utils.isTesting && sqlContext.conf.codegenFallback =>
        // We should already saw the error message
        logWarning(s"Whole-stage codegen disabled for plan (id=$codegenStageId):\n $treeString")
        return child.execute()
    }

    // Check if compiled code has a too large function
    if (maxCodeSize > sqlContext.conf.hugeMethodLimit) {
      logInfo(s"Found too long generated codes and JIT optimization might not work: " +
        s"the bytecode size ($maxCodeSize) is above the limit " +
        s"${sqlContext.conf.hugeMethodLimit}, and the whole-stage codegen was disabled " +
        s"for this plan (id=$codegenStageId). To avoid this, you can raise the limit " +
        s"`${SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key}`:\n$treeString")
      child match {
        // The fallback solution of batch file source scan still uses WholeStageCodegenExec
        case f: FileSourceScanExec if f.supportsBatch => // do nothing
        case _ => return child.execute()
      }
    }

    val references = ctx.references.toArray

    val durationMs = longMetric("pipelineTime")

    val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()
    assert(rdds.size <= 2, "Up to two input RDDs can be supported")
    if (rdds.length == 1) {
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
    } else {
      // Right now, we support up to two input RDDs.
      rdds.head.zipPartitions(rdds(1)) { (leftIter, rightIter) =>
        Iterator((leftIter, rightIter))
        // a small hack to obtain the correct partition index
      }.mapPartitionsWithIndex { (index, zippedIter) =>
        val (leftIter, rightIter) = zippedIter.next()
        val (clazz, _) = CodeGenerator.compile(cleanedSource)
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(index, Array(leftIter, rightIter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val v = buffer.hasNext
            if (!v) durationMs += buffer.durationMs()
            v
          }
          override def next: InternalRow = buffer.next()
        }
      }
    }
  }

```

### HashAggregateExec.inputRDDs
- 此时child 为InputAdapter,即调用InputAdapter.inputRDDs()函数

```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

```


### InputAdapter.inputRDDs
- 此时的child为ShuffleExchangeExec,即调用ShuffleExchangeExec.doExecute()函数

```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.execute() :: Nil
  }

```
### ShuffleExchangeExec.doExecute()
- 此时的exchangeCoordinator为None
- 调用函数prepareShuffleDependency()得到ShuffleDependency
- 再调用preparePostShuffleRDD()函数构建ShuffledRowRDD 为 ShuffledRowRDD [7]

```
  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = coordinator match {
        case Some(exchangeCoordinator) =>
          val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
          assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
          shuffleRDD
        case _ =>
          val shuffleDependency = prepareShuffleDependency()
          preparePostShuffleRDD(shuffleDependency)
      }
    }
    cachedShuffleRDD
  }
}

```

### ShuffleExchangeExec.prepareShuffleDependency()
- child.execute(),会先计算上级RDD,此时child 为 WholeStageCodegenExec,会先调用WholeStageCodegenExec.doExecute()函数，注意，上次调用的该函数还没执行完成，现在又一次调用该函数了
- ShuffleExchangeExec.prepareShuffleDependency会得到分区器

```
  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  private[exchange] def prepareShuffleDependency()
    : ShuffleDependency[Int, InternalRow, InternalRow] = {
    ShuffleExchangeExec.prepareShuffleDependency(
      child.execute(), child.output, newPartitioning, serializer)
  }

```

### WholeStageCodegenExec.doExecute()
- 此时的child为HashAaareaateExec,HashAggregateExec.inputRDDs()函数
- 然后再进行mapPartitionsWithIndex函数调用，rdds.head.mapPartitionsWithIndex得到的RDD为MapPartitionsRDD [5]


### HashAggregateExec.inputRDDs()
- child为ProjectExec即调用ProjectExec.inputRDDs()函数
```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }
```


### ProjectExec.inputRDDs()
- child为InputAdapter,即调用InputAdapter.inputRDDs()函数

```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

```

### InputAdapter.inputRDDs()
- child 为AppendColumnsWithObjectExec,即调用AppendColumnsWithObjectExec.doExecute()函数
```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.execute() :: Nil
  }
```

### AppendColumnsWithObjectExec.doExecute()
- child 为MapPartitionsExec即调用MapPartitionsExec.doExecute()函数
- child.execute().mapPartitionsInternal得到的RDD为MapPartitionsRDD [4]

```
  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getChildObject = ObjectOperator.unwrapObjectFromRow(child.output.head.dataType)
      val outputChildObject = ObjectOperator.serializeObjectToRow(inputSerializer)
      val outputNewColumnOjb = ObjectOperator.serializeObjectToRow(newColumnsSerializer)
      val combiner = GenerateUnsafeRowJoiner.create(inputSchema, newColumnSchema)

      iter.map { row =>
        val childObj = getChildObject(row)
        val newColumns = outputNewColumnOjb(func(childObj))
        combiner.join(outputChildObject(childObj), newColumns): InternalRow
      }
    }
  }

```

### MapPartitionsExec.doExecute()
- child为DeserializeToObjectExec,即调用DeserializeToObjectExec.doExecute()函数
- child.execute().mapPartitionsInternal得到的RDD为MapPartitionsRDD [3]

```
  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = ObjectOperator.unwrapObjectFromRow(child.output.head.dataType)
      val outputObject = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)
      func(iter.map(getObject)).map(outputObject)
    }
  }
```

### DeserializeToObjectExec.doExecute()
- child 为WholeStageCodegenExec,即调用WholeStageCodegenExec.doExecute()函数，又回去了
- 此时child.execute().mapPartitionsWithIndexInternal 得到的RDD为MapPartitionsRDD [2]

```
  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val projection = GenerateSafeProjection.generate(deserializer :: Nil, child.output)
      projection.initialize(index)
      iter.map(projection)
    }
  }
```

### WholeStageCodegenExec.doExecute()
- child为FileSourceScanExec即调用FileSourceScanExec.inputRDDs()函数


###  FileSourceScanExec.inputRDDs
- 调用函数FileSourceScanExec.inputRDD

```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

```

### FileSourceScanExec.inputRDD
- lazy 函数
- 调用FileSourceScanExec.createNonBucketedReadRDD()函数创建FileScanRDD



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

### FileSourceScanExec.createNonBucketedReadRDD
- 创建FileScanRDD，此时的RDD为FileScanRDD [0],也就是这个对象直接读HDFS上文件数据
- 对HDFS上的文件进行逻辑分区，我这里设置的是spark.sql.files.maxPartitionBytes的值为7 byte,所以计算文件分区大小为7 byte,总文件大小为14个byte,所以PartitionedFile(0)=hdfs://standalone.com:9000/user/liuwen/data/text/worldCount.txt, range: 0-7
PartitionedFile(1)=hdfs://standalone.com:9000/user/liuwen/data/text/worldCount.txt, range: 7-14


```
 /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val defaultMaxSplitBytes =
      fsRelation.sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = fsRelation.sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val blockLocations = getBlockLocations(file)
        if (fsRelation.fileFormat.isSplitable(
            fsRelation.sparkSession, fsRelation.options, file.getPath)) {
          (0L until file.getLen by maxSplitBytes).map { offset =>
            val remaining = file.getLen - offset
            val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
            val hosts = getBlockHosts(blockLocations, offset, size)
            PartitionedFile(
              partition.values, file.getPath.toUri.toString, offset, size, hosts)
          }
        } else {
          val hosts = getBlockHosts(blockLocations, 0, file.getLen)
          Seq(PartitionedFile(
            partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts))
        }
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition =
          FilePartition(
            partitions.size,
            currentFiles.toArray.toSeq) // Copy to a new Array.
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // Assign files to partitions using "Next Fit Decreasing"
    splitFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()

    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }
```

### 回调 ShuffleExchangeExec.prepareShuffleDependency
- 此时 rdd为:MapPartitionsRDD[5]
- 默认的分区器为HashPartitioning,默认的分区个数为200
- new Partitioner 
- 调用函数mapPartitionsWithIndexInternal,即得到RDD 为rddWithPartitionIds = MapPartitionsRDD[6]
- new ShuffleDependency
- 调用函数ShuffleExchangeExec.preparePostShuffleRDD得到ShuffleRowRDD

```
 def prepareShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(_, n) =>
        new Partitioner {
          override def numPartitions: Int = n
          // For HashPartitioning, the partitioning key is already a valid partition ID, as we use
          // `HashPartitioning.partitionIdExpression` to produce partitioning key.
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
        // partition bounds. To get accurate samples, we need to copy the mutable keys.
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val mutablePair = new MutablePair[InternalRow, Null]()
          iter.map(row => mutablePair.update(row.copy(), null))
        }
        implicit val ordering = new LazilyGeneratedOrdering(sortingExpressions, outputAttributes)
        new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }

```

### ShuffleExchangeExec.preparePostShuffleRDD
- new ShuffledRowRDD()
- 此时的RDD为 ShuffledRowRDD [7]
- 返回WholeStageCodegenExec.doExecute()函数

```
  /**
   * Returns a [[ShuffledRowRDD]] that represents the post-shuffle dataset.
   * This [[ShuffledRowRDD]] is created based on a given [[ShuffleDependency]] and an optional
   * partition start indices array. If this optional array is defined, the returned
   * [[ShuffledRowRDD]] will fetch pre-shuffle partitions based on indices of this array.
   */
  private[exchange] def preparePostShuffleRDD(
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    // If an array of partition start indices is provided, we need to use this array
    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
    // update the number of post-shuffle partitions.
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)
    }
    new ShuffledRowRDD(shuffleDependency, specifiedPartitionStartIndices)
  }

```

### WholeStageCodegenExec.doExecute()
- 此时child 为ShuffledRowRDD [7]，调用rdds.head.mapPartitionsWithIndex
- 即此时RDD为MapPartitionsRDD [8]
- 返回SparkPlan.getByteArrayRdd

### SparkPlan.getByteArrayRdd
- 此时child 为MapPartitionsRDD [8]
- 调用mapPartitionsInternal得到RDD为RDD为MapPartitionsRDD [9]
- 返回SparkPlan.executeCollect()

```
  /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   */
  private def getByteArrayRdd(n: Int = -1): RDD[(Long, Array[Byte])] = {
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))
      // `iter.hasNext` may produce one row and buffer it, we should only call it when the limit is
      // not hit.
      while ((n < 0 || count < n) && iter.hasNext) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        count += 1
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator((count, bos.toByteArray))
    }
  }
```

### SparkPlan.executeCollect()
-     val byteArrayRdd = getByteArrayRdd()得到MapPartitionsRDD [9],即通过Spark执行计划转化为Final RDD
- 调用RDD.collect()触发作业处理，就可以通过Spark集群计算任务，最后收集结果返回，这个过程这里不分析，这部分内容重点分析Final RDD 是如何转化过来的    

```
  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[InternalRow] = {
    val byteArrayRdd = getByteArrayRdd()

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { countAndBytes =>
      decodeUnsafeRows(countAndBytes._2).foreach(results.+=)
    }
    results.toArray
  }
```

end 