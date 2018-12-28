# Spark2.4.0 QueryExecution(SparkPlan) 源码分析

## 各执行计划关系
 -  LogicalPlan   =>  analyzed     =>    optimizedPlan     =>    sparkPlan   =>    executedPlan 

## 源码分析
# Spark2.4.0 QueryExecution(OptimizedPlan) 源码分析

#### 图解

- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/sql/dataset/sparkPlan.jpg

![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/sql/dataset/sparkPlan.jpg)


#### 程序入口
- 调用Dataset.count()函数

```
val df = spark.read.textFile("data/text/line.txt")
// 得到HDFS上的文件有多少行数据
val count = df.count()
//打印输出，多少行数据
println(s"结果:${count}")
```


#### Dataset.count
- 调用Dataset.withAction()函数

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


#### 用Dataset.withAction()
- 调用QueryExecution.executedPlan()函数，这个函数会调用executedPlan
- executedPlan会调用sparkPlan
- sparkPlan会调用OptimizedPlan

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

#### QueryExecution.executedPlan
- 调用 QueryExecution.prepareForExecution() 函数,调函数的参数为sparkPlan
- 所以会调用QueryExecution.sparkPlan()函数

```
  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)
```

- QueryExecution.sparkPlan
- 会先调用QueryExecution.optimizedPlan对LogincalPlan进行OptimizedPlan分析
- 再调用QueryPlanner.plan()函数

```
  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }
```

#### QueryPlanner.plan()
 - 调用函数 QueryPlanner.strategies(),这是一个抽像的函数，会调用实现类SparkPlanner.strategies()

```
def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    // Collect physical plan candidates.
    val candidates = strategies.iterator.flatMap(_(plan))

    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = this.plan(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) => childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

```


#### SparkPlanner.strategies()
- 调用FileSourceStrategy.apply

```
override def strategies: Seq[Strategy] =
    experimentalMethods.extraStrategies ++
      extraPlanningStrategies ++ (
      PythonEvals ::
      DataSourceV2Strategy ::
      FileSourceStrategy ::
      DataSourceStrategy(conf) ::
      SpecialLimits ::
      Aggregation ::
      Window ::
      JoinSelection ::
      InMemoryScans ::
      BasicOperators :: Nil)

```

#### FileSourceStrategy.apply
- 返回 FileSourceScanExec
- FileSourceScanExec 继承 DataSourceScanExec
- DataSourceScanExec 继承 LeafExecNode
- LeafExecNode 继承 SparkPlan
- 即QueryExecution.sparkPlan= FileSourceScanExec.apply()

```
 def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
      l @ LogicalRelation(fsRelation: HadoopFsRelation, _, table, _)) =>
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we do not need to
      // worry about case sensitivity anymore.
      val normalizedFilters = filters.map { e =>
        e transform {
          case a: AttributeReference =>
            a.withName(l.output.find(_.semanticEquals(a)).get.name)
        }
      }

      val partitionColumns =
        l.resolve(
          fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters
          .filterNot(SubqueryExpression.hasSubquery(_))
          .filter(_.references.subsetOf(partitionSet)))

      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

      val bucketSpec: Option[BucketSpec] = fsRelation.bucketSpec
      val bucketSet = if (shouldPruneBuckets(bucketSpec)) {
        genBucketSet(normalizedFilters, bucketSpec.get)
      } else {
        None
      }

      val dataColumns =
        l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val readDataColumns =
        dataColumns
          .filter(requiredAttributes.contains)
          .filterNot(partitionColumns.contains)
      val outputSchema = readDataColumns.toStructType
      logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

      val outputAttributes = readDataColumns ++ partitionColumns

      val scan =
        FileSourceScanExec(
          fsRelation,
          outputAttributes,
          outputSchema,
          partitionKeyFilters.toSeq,
          bucketSet,
          dataFilters,
          table.map(_.identifier))

      val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
      val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
      val withProjections = if (projects == withFilter.output) {
        withFilter
      } else {
        execution.ProjectExec(projects, withFilter)
      }

      withProjections :: Nil

    case _ => Nil
  }
```


end