# Spark2.4.0 QueryExecution(OptimizedPlan) 源码分析

## 各执行计划关系
 -  LogicalPlan   =>  analyzed     =>    optimizedPlan     =>    sparkPlan   =>    executedPlan 
 - 关系
 
 ```aidl
dataset.queryExecution.logical        ->  child  -> LogicalRelation ->  relation(HadoopFsRelation)

dataset.queryExecution.optimizedPlan:LogicalRelation ->  relation(HadoopFsRelation) 
dataset.queryExecution.sparkPlan:FileSourceScanExec
dataset.queryExecution.executedPlan:WholeStageCodegenExec

```

## 源码分析

###   OptimizedPlan 源码分析

#### 图解
- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/sql/dataset/optimizedPlan.jpg

![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/sql/dataset/optimizedPlan.jpg)


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


- QueryExecution.sparkPlan()函数
- 调用QueryExecution.optimizedPlan()函数

```
  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }
```

- 用QueryExecution.optimizedPlan()函数
- 调用函数QueryExecution.withCachedData()

```
 lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)

```

- QueryExecution.withCachedData()
- 调用函数 QueryExecution.analyzed()

```
  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }

```

- QueryExecution.analyzed()
- 开始对LogincalPlan进行分析
- 调用catalyst.analysis.Analyzer.executeAndCheck()函数

```
  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.executeAndCheck(logical)
  }

```

#### Analyzer.executeAndCheck()函数
- 调用Analyzer.execute()函数

```
  def executeAndCheck(plan: LogicalPlan): LogicalPlan = AnalysisHelper.markInAnalyzer {
    val analyzed = execute(plan)
    try {
      checkAnalysis(analyzed)
      analyzed
    } catch {
      case e: AnalysisException =>
        val ae = new AnalysisException(e.message, e.line, e.startPosition, Option(analyzed))
        ae.setStackTrace(e.getStackTrace)
        throw ae
    }
  }
```

- Analyzer.execute()
- 调用函数 Analyzer.executeSameContext()

```
  override def execute(plan: LogicalPlan): LogicalPlan = {
    AnalysisContext.reset()
    try {
      executeSameContext(plan)
    } finally {
      AnalysisContext.reset()
    }
  }

```


- Analyzer.executeSameContext()
- 调用RuleExecutor.execute()函数，用优化规则进行匹配，得出优化后的计划

```
private def executeSameContext(plan: LogicalPlan): LogicalPlan = super.execute(plan)
```

#### RuleExecutor.execute()
- 对logicalPlan用优化规则进行匹配，得出优化后的计划,并把返回值赋值给optimizedPlan

```
 /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    val queryExecutionMetrics = RuleExecutor.queryExecutionMeter

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime

            if (!result.fastEquals(plan)) {
              queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName)
              queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime)
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)

            // Run the structural integrity checker against the plan after each rule.
            if (!isPlanIntegral(result)) {
              val message = s"After applying rule ${rule.ruleName} in batch ${batch.name}, " +
                "the structural integrity of the plan is broken."
              throw new TreeNodeException(result, message, null)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}"
            if (Utils.isTesting) {
              throw new TreeNodeException(curPlan, message, null)
            } else {
              logWarning(message)
            }
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      if (!batchStartPlan.fastEquals(curPlan)) {
        logDebug(
          s"""
            |=== Result of Batch ${batch.name} ===
            |${sideBySide(batchStartPlan.treeString, curPlan.treeString).mkString("\n")}
          """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    }

    curPlan
  }
```



end