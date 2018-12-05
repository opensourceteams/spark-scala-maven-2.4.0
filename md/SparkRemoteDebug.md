# Spark 远程调试

## 更多资源分享
- github: https://github.com/opensourceteams/spark-scala-maven
- csdn(汇总视频在线看): https://blog.csdn.net/thinktothings/article/details/84726769


## IDEA 远程调试设置 Remote Applicate
[![idea 远程 debug](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/spark/idea_remote_debug.png "idea 远程 debug")](https://github.com/opensourceteams/spark-scala-maven/blob/master/md/images/spark/idea_remote_debug.png "idea 远程 debug")

## master远程调试
- 重启 start-master.sh 就会生效

### spark-env.sh文件配置

```shell
export SPARK_MASTER_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10000"
```

## worker远程调试
- 重启 start-slave.sh 就会生效

### spark-env.sh文件配置

```shell
export SPARK_WORKER_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10001"
```

## executor 远程调试


### 方式一程序中写死

```scala
spark.executor.extraJavaOptions=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002
```

```scala
 def sparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled","true")
   // conf.set("spark.ui.port","10002")
    conf.set("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
    conf.set("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/eventLog")
    //executor debug,是在提交作的地方读取
    conf.set("spark.executor.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002")
    conf.setJars(Array("/opt/n_001_workspaces/bigdata/spark-scala-maven/target/spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)

    //设置日志级别
    //sc.setLogLevel("ERROR")
    sc
  }
```

##  spark-submit

```shell
spark-submit \
  --class com.opensource.bigdata.spark.standalone.RunTextFileMkString2 \
  --master spark://standalone:7077 \
  --executor-memory 1G \
  --total-executor-cores 100 \
  --driver-java-options "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002" \
  /root/temp/spark-scala-maven-1.0-SNAPSHOT.jar \
```


