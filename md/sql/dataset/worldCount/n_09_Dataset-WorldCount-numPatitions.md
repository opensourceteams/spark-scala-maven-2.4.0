# Spark2.4.0源码分析之WorldCount 默认shuffling并行度为200(九)

## 配置
- The default number of partitions to use when shuffling data for joins or aggregations.
```
spark.sql.shuffle.partitions=200
```

### QueryExecution.executedPlan
- 调用 QueryExecution.prepareForExecution

```
 // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)
```

### QueryExecution.prepareForExecution
- 调用函数 QueryExecution.preparations

```
  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }
```
### QueryExecution.preparations
- 调用EnsureRequirements.apply

```
protected def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf))
```



### EnsureRequirements.apply
- 调用 EnsureRequirements.ensureDistributionAndOrdering
```
  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    // TODO: remove this after we create a physical operator for `RepartitionByExpression`.
    case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, _) =>
      child.outputPartitioning match {
        case lower: HashPartitioning if upper.semanticEquals(lower) => child
        case _ => operator
      }
    case operator: SparkPlan =>
      ensureDistributionAndOrdering(reorderJoinPredicates(operator))
  }

```

###  EnsureRequirements.ensureDistributionAndOrdering
- SQLConf,默认sql.shuffle的并行度为200,可以通过spark.sql.shuffle.partitions来进行配置
```
def numShufflePartitions: Int = getConf(SHUFFLE_PARTITIONS)
```

```
  val SHUFFLE_PARTITIONS = buildConf("spark.sql.shuffle.partitions")
    .doc("The default number of partitions to use when shuffling data for joins or aggregations.")
    .intConf
    .createWithDefault(200)
```
- defaultPartitioning
```
private def defaultNumPreShufflePartitions: Int = conf.numShufflePartitions
```
-   val numPartitions = distribution.requiredNumPartitions
          .getOrElse(defaultNumPreShufflePartitions) 得到numPartitions
- 调用ShuffleExchangeExec(defaultPartitioning, c)


```
private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements.
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      case (child, distribution) =>
        val numPartitions = distribution.requiredNumPartitions
          .getOrElse(defaultNumPreShufflePartitions)
        ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child)
    }

    // Get the indexes of children which have specified distribution requirements and need to have
    // same number of partitions.
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (UnspecifiedDistribution, _) => false
      case (_: BroadcastDistribution, _) => false
      case _ => true
    }.map(_._2)

    val childrenNumPartitions =
      childrenIndexes.map(children(_).outputPartitioning.numPartitions).toSet

    if (childrenNumPartitions.size > 1) {
      // Get the number of partitions which is explicitly required by the distributions.
      val requiredNumPartitions = {
        val numPartitionsSet = childrenIndexes.flatMap {
          index => requiredChildDistributions(index).requiredNumPartitions
        }.toSet
        assert(numPartitionsSet.size <= 1,
          s"$operator have incompatible requirements of the number of partitions for its children")
        numPartitionsSet.headOption
      }

      val targetNumPartitions = requiredNumPartitions.getOrElse(childrenNumPartitions.max)

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, distribution), index) if childrenIndexes.contains(index) =>
          if (child.outputPartitioning.numPartitions == targetNumPartitions) {
            child
          } else {
            val defaultPartitioning = distribution.createPartitioning(targetNumPartitions)
            child match {
              // If child is an exchange, we replace it with a new one having defaultPartitioning.
              case ShuffleExchangeExec(_, c, _) => ShuffleExchangeExec(defaultPartitioning, c)
              case _ => ShuffleExchangeExec(defaultPartitioning, child)
            }
          }

        case ((child, _), _) => child
      }
    }

    // Now, we need to add ExchangeCoordinator if necessary.
    // Actually, it is not a good idea to add ExchangeCoordinators while we are adding Exchanges.
    // However, with the way that we plan the query, we do not have a place where we have a
    // global picture of all shuffle dependencies of a post-shuffle stage. So, we add coordinator
    // at here for now.
    // Once we finish https://issues.apache.org/jira/browse/SPARK-10665,
    // we can first add Exchanges and then add coordinator once we have a DAG of query fragments.
    children = withExchangeCoordinator(children, requiredChildDistributions)

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        SortExec(requiredOrdering, global = false, child = child)
      }
    }

    operator.withNewChildren(children)
  }

```


end