# Spark2.4.0 QueryExecution(LogicalPlan) 源码分析

## 各执行计划关系
 -  LogicalPlan   =>  analyzed     =>    optimizedPlan     =>    sparkPlan   =>    executedPlan 

## 源码分析

###   LogicalPlan 源码分析

#### 图解

#### 程序入口
- 调用DataFrameReader.textFile()函数

```
val df = spark.read.textFile("data/text/line.txt")
```
#### DataFrameReader.textFile
- 调用同名函数DataFrameReader.textFile

```
  /**
   * Loads text files and returns a [[Dataset]] of String. See the documentation on the
   * other overloaded `textFile()` method for more details.
   * @since 2.0.0
   */
  def textFile(path: String): Dataset[String] = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    textFile(Seq(path): _*)
  }
```

- 调用函数DataFrameReader.text()
```
  /**
   * Loads text files and returns a [[Dataset]] of String. The underlying schema of the Dataset
   * contains a single string column named "value".
   *
   * If the directory structure of the text files contains partitioning information, those are
   * ignored in the resulting Dataset. To include partitioning information as columns, use `text`.
   *
   * By default, each line in the text files is a new row in the resulting DataFrame. For example:
   * {{{
   *   // Scala:
   *   spark.read.textFile("/path/to/spark/README.md")
   *
   *   // Java:
   *   spark.read().textFile("/path/to/spark/README.md")
   * }}}
   *
   * You can set the following textFile-specific option(s) for reading text files:
   * <ul>
   * <li>`wholetext` (default `false`): If true, read a file as a single row and not split by "\n".
   * </li>
   * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
   * that should be used for parsing.</li>
   * </ul>
   *
   * @param paths input path
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def textFile(paths: String*): Dataset[String] = {
    assertNoSpecifiedSchema("textFile")
    text(paths : _*).select("value").as[String](sparkSession.implicits.newStringEncoder)
  }
```

- 调用函数DataFrameReader.load()
 
```
 /**
   * Loads text files and returns a `DataFrame` whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any.
   *
   * By default, each line in the text files is a new row in the resulting DataFrame. For example:
   * {{{
   *   // Scala:
   *   spark.read.text("/path/to/spark/README.md")
   *
   *   // Java:
   *   spark.read().text("/path/to/spark/README.md")
   * }}}
   *
   * You can set the following text-specific option(s) for reading text files:
   * <ul>
   * <li>`wholetext` (default `false`): If true, read a file as a single row and not split by "\n".
   * </li>
   * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
   * that should be used for parsing.</li>
   * </ul>
   *
   * @param paths input paths
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def text(paths: String*): DataFrame = format("text").load(paths : _*)

```

- 调用函数 DataFrameReader.loadV1Source()

```
 /**
   * Loads input in as a `DataFrame`, for data sources that support multiple paths.
   * Only works if the source is a HadoopFsRelationProvider.
   *
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def load(paths: String*): DataFrame = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not " +
        "read files of Hive data source directly.")
    }

    val cls = DataSource.lookupDataSource(source, sparkSession.sessionState.conf)
    if (classOf[DataSourceV2].isAssignableFrom(cls)) {
      val ds = cls.newInstance().asInstanceOf[DataSourceV2]
      if (ds.isInstanceOf[ReadSupport]) {
        val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
          ds = ds, conf = sparkSession.sessionState.conf)
        val pathsOption = {
          val objectMapper = new ObjectMapper()
          DataSourceOptions.PATHS_KEY -> objectMapper.writeValueAsString(paths.toArray)
        }
        Dataset.ofRows(sparkSession, DataSourceV2Relation.create(
          ds, sessionOptions ++ extraOptions.toMap + pathsOption,
          userSpecifiedSchema = userSpecifiedSchema))
      } else {
        loadV1Source(paths: _*)
      }
    } else {
      loadV1Source(paths: _*)
    }
  }
```


- 调用函数sparkSession.baseRelationToDataFrame()
- DataSource.apply构建DataSource对象
- DataSouce.resolveRelation()函数返回SparkPlan的子类:HadoopFsRelation
- sparkSession.baseRelationToDataFrame() 构建Dataset,构建Dataset中的QueryExecution(包括LogicalPlan)

```
  private def loadV1Source(paths: String*) = {
    // Code path for data source v1.
    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap).resolveRelation())
  }

```
#### DataSource.resolveRelation()函数
- 返回SparkPlan的子类:HadoopFsRelation

```
/**
   * Create a resolved [[BaseRelation]] that can be used to read data from or write data into this
   * [[DataSource]]
   *
   * @param checkFilesExist Whether to confirm that the files exist when generating the
   *                        non-streaming file based datasource. StructuredStreaming jobs already
   *                        list file existence, and when generating incremental jobs, the batch
   *                        is considered as a non-streaming file based data source. Since we know
   *                        that files already exist, we don't need to check them again.
   */
  def resolveRelation(checkFilesExist: Boolean = true): BaseRelation = {
    val relation = (providingClass.newInstance(), userSpecifiedSchema) match {
      // TODO: Throw when too much is given.
      case (dataSource: SchemaRelationProvider, Some(schema)) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions, schema)
      case (dataSource: RelationProvider, None) =>
        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
      case (_: SchemaRelationProvider, None) =>
        throw new AnalysisException(s"A schema needs to be specified when using $className.")
      case (dataSource: RelationProvider, Some(schema)) =>
        val baseRelation =
          dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
        if (baseRelation.schema != schema) {
          throw new AnalysisException(s"$className does not allow user-specified schemas.")
        }
        baseRelation

      // We are reading from the results of a streaming query. Load files from the metadata log
      // instead of listing them using HDFS APIs.
      case (format: FileFormat, _)
          if FileStreamSink.hasMetadata(
            caseInsensitiveOptions.get("path").toSeq ++ paths,
            sparkSession.sessionState.newHadoopConf()) =>
        val basePath = new Path((caseInsensitiveOptions.get("path").toSeq ++ paths).head)
        val fileCatalog = new MetadataLogFileIndex(sparkSession, basePath, userSpecifiedSchema)
        val dataSchema = userSpecifiedSchema.orElse {
          format.inferSchema(
            sparkSession,
            caseInsensitiveOptions,
            fileCatalog.allFiles())
        }.getOrElse {
          throw new AnalysisException(
            s"Unable to infer schema for $format at ${fileCatalog.allFiles().mkString(",")}. " +
                "It must be specified manually")
        }

        HadoopFsRelation(
          fileCatalog,
          partitionSchema = fileCatalog.partitionSchema,
          dataSchema = dataSchema,
          bucketSpec = None,
          format,
          caseInsensitiveOptions)(sparkSession)

      // This is a non-streaming file based datasource.
      case (format: FileFormat, _) =>
        val globbedPaths =
          checkAndGlobPathIfNecessary(checkEmptyGlobPath = true, checkFilesExist = checkFilesExist)
        val useCatalogFileIndex = sparkSession.sqlContext.conf.manageFilesourcePartitions &&
          catalogTable.isDefined && catalogTable.get.tracksPartitionsInCatalog &&
          catalogTable.get.partitionColumnNames.nonEmpty
        val (fileCatalog, dataSchema, partitionSchema) = if (useCatalogFileIndex) {
          val defaultTableSize = sparkSession.sessionState.conf.defaultSizeInBytes
          val index = new CatalogFileIndex(
            sparkSession,
            catalogTable.get,
            catalogTable.get.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
          (index, catalogTable.get.dataSchema, catalogTable.get.partitionSchema)
        } else {
          val index = createInMemoryFileIndex(globbedPaths)
          val (resultDataSchema, resultPartitionSchema) =
            getOrInferFileFormatSchema(format, Some(index))
          (index, resultDataSchema, resultPartitionSchema)
        }

        HadoopFsRelation(
          fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = dataSchema.asNullable,
          bucketSpec = bucketSpec,
          format,
          caseInsensitiveOptions)(sparkSession)

      case _ =>
        throw new AnalysisException(
          s"$className is not a valid Spark SQL Data Source.")
    }

    relation match {
      case hs: HadoopFsRelation =>
        SchemaUtils.checkColumnNameDuplication(
          hs.dataSchema.map(_.name),
          "in the data schema",
          equality)
        SchemaUtils.checkColumnNameDuplication(
          hs.partitionSchema.map(_.name),
          "in the partition schema",
          equality)
        DataSourceUtils.verifyReadSchema(hs.fileFormat, hs.dataSchema)
      case _ =>
        SchemaUtils.checkColumnNameDuplication(
          relation.schema.map(_.name),
          "in the data schema",
          equality)
    }

    relation
  }
```



#### sparkSession.baseRelationToDataFrame()

```
  /**
   * Convert a `BaseRelation` created for external data sources into a `DataFrame`.
   *
   * @since 2.0.0
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    Dataset.ofRows(self, LogicalRelation(baseRelation))
  }

```


#### Dataset
- Dataset.ofRows函数，构建Dataset,包括QueryExecution

```
  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
```


#### SessionState.executePlan()函数

```
  def executePlan(plan: LogicalPlan): QueryExecution = createQueryExecution(plan)
```


- SessionState.createQueryExecution
```
private[sql] class SessionState(
    sharedState: SharedState,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods,
    val functionRegistry: FunctionRegistry,
    val udfRegistration: UDFRegistration,
    catalogBuilder: () => SessionCatalog,
    val sqlParser: ParserInterface,
    analyzerBuilder: () => Analyzer,
    optimizerBuilder: () => Optimizer,
    val planner: SparkPlanner,
    val streamingQueryManager: StreamingQueryManager,
    val listenerManager: ExecutionListenerManager,
    resourceLoaderBuilder: () => SessionResourceLoader,
    createQueryExecution: LogicalPlan => QueryExecution,
    createClone: (SparkSession, SessionState) => SessionState) {

```

- SessionState构建
- 调用SparkSession.instantiateSessionState

```
  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   * If `parentSessionState` is not null, the `SessionState` will be a copy of the parent.
   *
   * This is internal to Spark and there is no guarantee on interface stability.
   *
   * @since 2.2.0
   */
  @InterfaceStability.Unstable
  @transient
  lazy val sessionState: SessionState = {
    parentSessionState
      .map(_.clone(this))
      .getOrElse {
        val state = SparkSession.instantiateSessionState(
          SparkSession.sessionStateClassName(sparkContext.conf),
          self)
        initialSessionOptions.foreach { case (k, v) => state.conf.setConfString(k, v) }
        state
      }
  }
```


#### SparkSession.instantiateSessionState
- 调用BaseSessionStateBuilder.build()函数，构建 SessionState对象

```
  private def instantiateSessionState(
      className: String,
      sparkSession: SparkSession): SessionState = {
    try {
      // invoke `new [Hive]SessionStateBuilder(SparkSession, Option[SessionState])`
      val clazz = Utils.classForName(className)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(sparkSession, None).asInstanceOf[BaseSessionStateBuilder].build()
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }
```
#### BaseSessionStateBuilder.build()
- 调用 BaseSessionStateBuilder.createQueryExecution()函数，构建QueryExecution对象，这个是最重要的，把HadoopFSRelation作为QueryExecution对象的LogicalPlan

```
  /**
   * Build the [[SessionState]].
   */
  def build(): SessionState = {
    new SessionState(
      session.sharedState,
      conf,
      experimentalMethods,
      functionRegistry,
      udfRegistration,
      () => catalog,
      sqlParser,
      () => analyzer,
      () => optimizer,
      planner,
      streamingQueryManager,
      listenerManager,
      () => resourceLoader,
      createQueryExecution,
      createClone)
  }
```


- BaseSessionStateBuilder.createQueryExecution
```
 /**
   * Create a query execution object.
   */
  protected def createQueryExecution: LogicalPlan => QueryExecution = { plan =>
    new QueryExecution(session, plan)
  }
```





end