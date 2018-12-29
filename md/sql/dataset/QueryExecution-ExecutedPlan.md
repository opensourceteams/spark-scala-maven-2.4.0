# Spark2.4.0 QueryExecution(ExecutedPlan) 源码分析

## 更多资源
- github: https://github.com/opensourceteams/spark-scala-maven-2.4.0


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
# Spark2.4.0 QueryExecution(OptimizedPlan) 源码分析

#### 图解
- https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/sql/dataset/executedPlan.jpg

![](https://github.com/opensourceteams/spark-scala-maven-2.4.0/blob/master/md/image/spark/sql/dataset/executedPlan.jpg)


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

- QueryExecution.prepareForExecution()
- 调用QueryExecution.preparations

```
  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }
```

- QueryExecution.preparations
- 调用CollapseCodegenStages.apply

```
  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf))

```

#### CollapseCodegenStages.apply
- 调用 CollapseCodegenStages.insertWholeStageCodegen函数

```
  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.wholeStageEnabled) {
      WholeStageCodegenId.resetPerQuery()
      insertWholeStageCodegen(plan)
    } else {
      plan
    }
  }
```

- CollapseCodegenStages. insertWholeStageCodegen
- 调用WholeStageCodegenExe.apply()
- WholeStageCodegenExe 继承UnaryExecNode
- UnaryExecNode 继承 SparkPlan
- 返回给QueryExecution.executedPlan
 
```
  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = plan match {
    // For operators that will output domain object, do not insert WholeStageCodegen for it as
    // domain object can not be written into unsafe row.
    case plan if plan.output.length == 1 && plan.output.head.dataType.isInstanceOf[ObjectType] =>
      plan.withNewChildren(plan.children.map(insertWholeStageCodegen))
    case plan: CodegenSupport if supportCodegen(plan) =>
      WholeStageCodegenExec(insertInputAdapter(plan))(WholeStageCodegenId.getNextStageId())
    case other =>
      other.withNewChildren(other.children.map(insertWholeStageCodegen))
  }

```





end