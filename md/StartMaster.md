# Spark Master启动源码分析
## 更多资源分享
- github: https://github.com/opensourceteams/spark-scala-maven

## Youtube 视频
- Spark master启动源码分析: https://youtu.be/74q1nddoaiY​

## 启动 master

### 启动脚本 start-master.sh 

#### 加载配置文件

```shell
. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"
```

#### 默认配置

```shell
SPARK_MASTER_PORT=7077
SPARK_MASTER_IP=`hostname`
SPARK_MASTER_WEBUI_PORT=8080
CLASS="org.apache.spark.deploy.master.Master"
```

#### 调用spark-daemon.sh

```shell
CLASS="org.apache.spark.deploy.master.Master"
ORIGINAL_ARGS="$@"

"${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS 1 \
  --ip $SPARK_MASTER_IP --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT \
  $ORIGINAL_ARGS
  
```

### spark-daemon.sh

#### 解析spark-daemon.sh
- 调用命令

	```shell
nohup nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class $command "$@" >> "$log" 2>&1 < /dev/null &
newpid="$!"
;;
```

- 即
	
	```shell
nohup nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.master.Master "$@" >> "$log" 2>&1 < /dev/null &
newpid="$!"
;;
```



```shell

run_command() {
  mode="$1"
  shift

  mkdir -p "$SPARK_PID_DIR"

  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi

  if [ "$SPARK_MASTER" != "" ]; then
    echo rsync from "$SPARK_MASTER"
    rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' "$SPARK_MASTER/" "${SPARK_HOME}"
  fi

  spark_rotate_log "$log"
  echo "starting $command, logging to $log"

  case "$mode" in
    (class)
      nohup nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class $command "$@" >> "$log" 2>&1 < /dev/null &
      newpid="$!"
      ;;

    (submit)
      nohup nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-submit --class $command "$@" >> "$log" 2>&1 < /dev/null &
      newpid="$!"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

  echo "$newpid" > "$pid"
  sleep 2
  # Check if the process has died; in that case we'll tail the log so the user can see
  if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
    echo "failed to launch $command:"
    tail -2 "$log" | sed 's/^/  /'
    echo "full log in $log"
  fi
}
```

```shell
option=$1

case $option in

  (submit)
    run_command submit "$@"
    ;;

  (start)
    run_command class "$@"
    ;;
```

### spark-class
#### 加载配置文件

```shell
. "${SPARK_HOME}"/bin/load-spark-env.sh
```

#### 调用Main入口类

```shell
# The launcher library will print arguments separated by a NULL character, to allow arguments with
# characters that would be otherwise interpreted by the shell. Read that in a while loop, populating
# an array that will be used to exec the final command.
CMD=()
while IFS= read -d '' -r ARG; do
  CMD+=("$ARG")
done < <("$RUNNER" -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.Main "$@")
exec "${CMD[@]}"

```

## Main类入口
### 启动Master类命令调用

```scala
/**
   * Usage: Main [class] [class args]
   * <p>
   * This CLI works in two different modes:
   * <ul>
   *   <li>"spark-submit": if <i>class</i> is "org.apache.spark.deploy.SparkSubmit", the
   *   {@link SparkLauncher} class is used to launch a Spark application.</li>
   *   <li>"spark-class": if another class is provided, an internal Spark class is run.</li>
   * </ul>
   *
   * This class works in tandem with the "bin/spark-class" script on Unix-like systems, and
   * "bin/spark-class2.cmd" batch script on Windows to execute the final command.
   * <p>
   * On Unix-like systems, the output is a list of command arguments, separated by the NULL
   * character. On Windows, the output is a command line suitable for direct execution from the
   * script.
   */
  public static void main(String[] argsArray) throws Exception {
    checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

    List<String> args = new ArrayList<String>(Arrays.asList(argsArray));
    String className = args.remove(0);

    boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
    AbstractCommandBuilder builder;
    if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
      try {
        builder = new SparkSubmitCommandBuilder(args);
      } catch (IllegalArgumentException e) {
        printLaunchCommand = false;
        System.err.println("Error: " + e.getMessage());
        System.err.println();

        MainClassOptionParser parser = new MainClassOptionParser();
        try {
          parser.parse(args);
        } catch (Exception ignored) {
          // Ignore parsing exceptions.
        }

        List<String> help = new ArrayList<String>();
        if (parser.className != null) {
          help.add(parser.CLASS);
          help.add(parser.className);
        }
        help.add(parser.USAGE_ERROR);
        builder = new SparkSubmitCommandBuilder(help);
      }
    } else {
      builder = new SparkClassCommandBuilder(className, args);
    }

    Map<String, String> env = new HashMap<String, String>();
    List<String> cmd = builder.buildCommand(env);
    if (printLaunchCommand) {
      System.err.println("Spark Command: " + join(" ", cmd));
      System.err.println("========================================");
    }

    if (isWindows()) {
      System.out.println(prepareWindowsCommand(cmd, env));
    } else {
      // In bash, use NULL as the arg separator since it cannot be used in an argument.
      List<String> bashCmd = prepareBashCommand(cmd, env);
      for (String c : bashCmd) {
        System.out.print(c);
        System.out.print('\0');
      }
    }
  }
```

## Master类
### main方法
- 启动 'sparkMaster' 服务
- 给自己发送消息: BoundPortsRequest

```scala
  def main(argStrings: Array[String]) {
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }
```

```scala
  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    val portsResponse = masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
```

### onStart方法
- Started MasterWebUI
- 实例化默认的存储引擎，实例化Leader选举
- 默认每分钟检查worker的心跳，未保持连接的worker清除

```scala
override def onStart(): Unit = {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    webUi = new MasterWebUI(this, webUiPort)
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }
```

- 接受Worker注册

```scala
 override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterWorker(
        id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl) => {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      if (state == RecoveryState.STANDBY) {
        context.reply(MasterInStandby)
      } else if (idToWorker.contains(id)) {
        context.reply(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerWebUiUrl)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          context.reply(RegisteredWorker(self, masterWebUiUrl))
          schedule()
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          context.reply(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }
    }

```




