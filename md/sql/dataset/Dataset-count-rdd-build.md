# Spark2.4.0源码分析之Dataset.count RDD构建

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0


## 时序图
- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/sql/dataset/count.rdd.build.jpg

![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/sql/dataset/count.rdd.build.jpg)





## 主要内容介绍
- 统计HDFS上文件的行数源码分析
- Dataset.count触发作业提交之前的源码分析
- 即作业提交时，FinalRDD是如何通过SparkPlan构建的

### 执行计划之间的关系

```aidl

WholeStageCodegenExec.child = HashAggregateExec
HashAggregateExec.child = InputAdapter
InputAdapter.child = ShuffleExchangeExec
ShuffleExchangeExec.child=WholeStageCodegenExec
WholeStageCodegenExec.child=HashAggregateExec
HashAggregateExec.child=FileSourceScanExec

```

### RDD构建

```aidl
FileScanRdd[0]
MapPartitionsRDD[1]
MapPartitionsRDD[2]
MapPartitionsRDD[3]
MapPartitionsRDD[4]
MapPartitionsRDD[5]

```

## 输入数据
```
第一行数据
第二行数据
第三行数据
第四行数据
```

## 输入程序

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


  def sparkSession(isLocal:Boolean = false, isHiveSupport:Boolean = false, remoteDebug:Boolean=false): SparkSession = {

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

### main
- 调用dataSet.count进行统计行数
 
```
object Run_dataset_count extends BaseSparkSession{


  appName = "Dataset textFile count"

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true,false,false)
    //读取HDFS上文件
    val df = spark.read.textFile("data/text/line.txt")
    val count = df.count
    println(s"结果:${count}")



    spark.stop()
  }

}

```

## 源码分析

### Dataset.Count
- 调用 groupBy().count().queryExecution构建执行计划
- 调用Dataset.withAction()函数，该函数中相当于groupBy().count().queryExecution构建的QueryExecution对象(queryExecution)
- queryExecution.executedPlan来调用executeCollect()函数
- 最后构建的WholeStageCodegenExec对象没有重写executeCollect()函数，调用父类SparkPaln.executeCollect()函数

```
  /**
   * Returns the number of rows in the Dataset.
   * @group action
   * @since 1.6.0
   */
  def count(): Long = withAction("count", groupBy().count().queryExecution) { plan =>
    plan.executeCollect().head.getLong(0)
  }

```
- 执行计划之间的关系
```
Dataset.withAction参数:
groupBy().count().queryExecution.executedPlan=WholeStageCodegenExec
WholeStageCodegenExec .child = HashAggregateExec
HashAggregateExec.child = InputAdapter
InputAdapter.child = ShuffleExchangeExec
ShuffleExchangeExec.child=WholeStageCodegenExec
WholeStageCodegenExec.child=HashAggregateExec
HashAggregateExec.child=FileSourceScanExec

```

### Dataset.withAction
- 调用Dataset.withAction()函数，该函数中相当于groupBy().count().queryExecution构建的QueryExecution对象(queryExecution)
- queryExecution.executedPlan来调用executeCollect()函数

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


### SparkPaln.executeCollect()
- 这里是关键代码
- getByteArrayRdd()会通过SparkPlan来构建FinalRdd,FinalRdd是包含了所有RDD之间的依赖和转换的
- byteArrayRdd.collect()相当于把FinalRdd.collect()触发作业提交
- 作业提交放到下一部分说明，不然篇幅太长了，导致说不清楚


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



### SparkPlan.getByteArrayRdd()
- 该函数主要是调用SparkPlan.execute()函数
- SparkPlan.execute()函数会调用SparkPlan.doExecute()这是一个未实现的函数，在具体实现SparkPlan的子类中实现，所以这里会进行实现类的doExecute()函数调用
- 此时调用的是WholeStageCodegenExec.doExecute()
- SparkPlan.execute()函数得到RDD,调用mapPartitionsInternal会new MapPartitionsRDD,进行一次RDD转换

```
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

### WholeStageCodegenExec.doExecute()
- 该方法重要的地方,child.asInstanceOf[CodegenSupport].inputRDDs() ,child是WholeStageCodegenExec样例类的了个属性，类型为SparkPlan,相当于构建执行计划之前的关系
- 调用SparkPlan.inputRDDs函数来得到RDD,当然这里调用的是具体的执行计划中的实现类
- 当前调用的是HashAggregateExec.inputRDDs()函数
- 得到RDD后会对RDD进行一次mapPartitionsWithIndex操作，会new MapPartitionsRdd()

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


### HashAggregateExec.inputRDDs()
- 调用InputAdapter.inputRDDs()函数

```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

```

### InputAdapter.inputRDDs()
- 调用SparkPlan.execute
- 调用SparkPlan.doExecute()
- 调用ShuffleExchangeExec.doExecute()函数

```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.execute() :: Nil
  }
```


### ShuffleExchangeExec.doExecute()
- 该函数调用prepareShuffleDependency()函数得到RDD的依赖，并且是ShuffleDependency
- 再调用函数preparePostShuffleRDD(并且是ShuffleDependency),该函数是new ShuffledRowRDD(并且是ShuffleDependency)



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

### ShuffleExchangeExec.prepareShuffleDependency
- 该函数是为了得到依赖，也就是RDD的依赖，用于后续构建RDD使用
- 构建依赖之前会先调用该执行计划的上一个执行计划，也就是ShuffleExchangeExec中的属性child(SpakPlan)
- 等于是调用WholeStageCodegenExec.doExecute()函数

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
- 该函数在上面源码已贴出来了，就不再贴，太长了
- 此时又重新调用inputRDDs()函数，
- 最后回调FileSourceScanExec.inputRDDs()函数

### FileSourceScanExec.inputRDDs()
- 调用 FileSourceScanExec.inputRDD
 
```
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }
```

### FileSourceScanExec.inputRDD
- 调用函数createNonBucketedReadRDD()构建FileScanRDD

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



### FileSourceScanExec.createNonBucketedReadRDD()

- 该函数构建FileScanRDD(),该RDD负责读取HDFS上文件的数据
- 然后一步一步返回得到FinalRDD
- 回到SparkPlan.executeCollect()

```
FileScanRdd[0]
MapPartitionsRDD[1]
MapPartitionsRDD[2]
MapPartitionsRDD[3]
MapPartitionsRDD[4]
MapPartitionsRDD[5]

```

```


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


### SparkPlan.executeCollect()
- getByteArrayRdd()函数返回FinalRdd
- FinalRdd.collect调用sc.runJob()函数触发作业提交
- 由于篇幅问题，触发作业提交部分放到下一个知识点说明



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