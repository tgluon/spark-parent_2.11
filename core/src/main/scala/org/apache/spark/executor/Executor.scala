/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.management.ManagementFactory
import java.net.URL
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{AccumulableInfo, DirectTaskResult, IndirectTaskResult, Task}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/**
  * Spark executor, backed by a threadpool to run tasks.
  *
  * This can be used with Mesos, YARN, and the standalone scheduler.
  * An internal RPC interface is used for communication with the driver,
  * except in the case of Mesos fine-grained mode.
  */
private[spark] class Executor(
                               executorId: String,
                               executorHostname: String,
                               env: SparkEnv,
                               userClassPath: Seq[URL] = Nil,
                               isLocal: Boolean = false)
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert(0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(SparkUncaughtExceptionHandler)
  }

  // Start worker thread pool
  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("Executor task launch worker")
  private val executorSource = new ExecutorSource(threadPool, executorId)
  // Pool used for threads that supervise task killing / cancellation
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // For tasks which are in the process of being killed, this map holds the most recently created
  // TaskReaper. All accesses to this map should be synchronized on the map itself (this isn't
  // a ConcurrentHashMap because we use the synchronization for purposes other than simply guarding
  // the integrity of the map's internal state). The purpose of this map is to prevent the creation
  // of a separate TaskReaper for every killTask() of a given task. Instead, this map allows us to
  // track whether an existing TaskReaper fulfills the role of a TaskReaper that we would otherwise
  // create. The map key is a task id.
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()

  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    env.blockManager.initialize(conf.getAppId)
  }

  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // Whether to monitor killed / interrupted tasks
  private val taskReaperEnabled = conf.getBoolean("spark.task.reaper.enabled", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back.
  private val maxDirectResultSize = Math.min(
    conf.getSizeAsBytes("spark.task.maxDirectResultSize", 1L << 20),
    RpcUtils.maxMessageSizeBytes(conf))

  // Limit of bytes for total size of results (default is 1GB)
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Executor for the heartbeat task.
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
    * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
    * times, it should kill itself. The default value is 60. It means we will retry to send
    * heartbeats about 10 minutes because the heartbeat interval is 10s.
    */
  private val HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60)

  /**
    * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
    * successful heartbeat will reset it to 0.
    */
  private var heartbeatFailures = 0

  startDriverHeartbeater()
  //对于每个task都会创建一个TaksRunner，TaskRunner继承的是java 的多线程中的Runnable接口
  def launchTask(
                  context: ExecutorBackend,
                  taskId: Long,
                  attemptNumber: Int,
                  taskName: String,
                  serializedTask: ByteBuffer): Unit = {
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    //将TaskRunner放入内存缓存
    runningTasks.put(taskId, tr)
    //Executor内部有一个java线程池，然后，这里既是将task
    // 封装在一个线程中(taskRunner), 直接将线程丢进线程池，
    // 进行执行，所以这里我们要知道，线程池是自动实现了排队机
    // 制的，也就是 说，如果线程池的线程暂时没有空闲的，那么丢
    // 进来的线程都是要排队的
    threadPool.execute(tr)
  }

  def killTask(taskId: Long, interruptThread: Boolean): Unit = {
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      if (taskReaperEnabled) {
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            val taskReaper = new TaskReaper(taskRunner, interruptThread = interruptThread)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // Execute the TaskReaper from outside of the synchronized block.
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        taskRunner.kill(interruptThread = interruptThread)
      }
    }
  }

  /**
    * Function to kill the running tasks in an executor.
    * This can be called by executor back-ends to kill the
    * tasks instead of taking the JVM down.
    *
    * @param interruptThread whether to interrupt the task thread
    */
  def killAllTasks(interruptThread: Boolean): Unit = {
    runningTasks.keys().asScala.foreach(t => killTask(t, interruptThread = interruptThread))
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  class TaskRunner(
                    execBackend: ExecutorBackend,
                    val taskId: Long,
                    val attemptNumber: Int,
                    taskName: String,
                    serializedTask: ByteBuffer)
    extends Runnable {

    val threadName = s"Executor task launch worker for task $taskId"

    /** Whether this task has been killed. */
    @volatile private var killed = false

    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** Whether this task has been finished. */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    def isFinished: Boolean = synchronized {
      finished
    }

    /** How much the JVM process has spent in GC when the task starts to run. */
    @volatile var startGCTime: Long = _

    /**
      * The task to run. This will be set in run() by deserializing the task binary coming
      * from the driver. Once it is set, it will never be changed.
      */
    @volatile var task: Task[Any] = _

    def kill(interruptThread: Boolean): Unit = {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId)")
      killed = true
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread)
          }
        }
      }
    }

    /**
      * Set the finished flag to true and clear the current thread's interrupt status
      */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      Thread.interrupted()
      // Notify any waiting TaskReapers. Generally there will only be one reaper per task but there
      // is a rare corner-case where one task can have two reapers in case cancel(interrupt=False)
      // is followed by cancel(interrupt=True). Thus we use notifyAll() to avoid a lost wakeup:
      notifyAll()
    }

    override def run(): Unit = {

      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)
      val threadMXBean = ManagementFactory.getThreadMXBean
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      // 记录开始反序列化的时间
      val deserializeStartTime = System.currentTimeMillis()
      // 记录开始反序列化的时的Cpu时间
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      var taskStartCpu: Long = 0
      // 开始GC的时间
      startGCTime = computeTotalGcTime()

      try {
        //对序列化的task数据，进行反序列化
        //反序列化任务信息
        val (taskFiles, taskJars, taskProps, taskBytes) =
          Task.deserializeWithDependencies(serializedTask)

        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        // 根据taskProps设置executor属性
        Executor.taskDeserializationProps.set(taskProps)
        //然后通过网络通信，将需要的文件、资源、jar拷贝过来

        // 根据taskFiles和taskJars，
        // 下载任务所需的File 和 加载所需的Jar包
        updateDependencies(taskFiles, taskJars)
        //最后，通过正式的反序列化操作，将整个task的数据集反序列化回来
        //为什么要用到java的classLoader，
        //因为java的ClassLoder，可以用反射的方式来动态加载一个类来创建对象

        // 根据taskBytes生成task
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        //设置task属性
        task.localProperties = taskProps
        //设置task内存管理
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        // 若在反序列话之前Task就被kill了，
        // 抛出异常
        if (killed) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }

        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        //更新mapOutputTracker Epoch 为task epoch
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.

        //计算出task开始时间

        // 记录任务开始时间
        taskStart = System.currentTimeMillis()
        // 记录任务开始时的cpu时间
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        val value = try {
          // 运行Task
          val res = task.run(
            //最关键的东西，执行task，用的是Task的run()方法,这里的value，
            // 对于ShuffleMapTask来说，其实就是MapStatus封装了ShuffleMapTask
            //计算的数据，输出位置

            //后面如果还是一个ShuffleMapTask呢？
            // 那么就会去练习MapOutputTracker，来获取上一个ShuffleMapTask
            //的输出位置，然后通过网络拉去数据,ResultTaks也是一样的
            taskAttemptId = taskId,
            attemptNumber = attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }
        }
        //计算出task结束时间
        val taskFinish = System.currentTimeMillis()
        // 记录task结束时的cpu时间
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // If the task has been killed, let's fail it.
        // 若task在运行中被kill了
        // 则抛出异常
        if (task.killed) {
          throw new TaskKilledException
        }


        //这里，其实就是对MapStatus进行了各种序列化，和封装，以为后面要发送个Driver，通过网络
        val resultSer = env.serializer.newInstance()
        // 结果记录序列化开始的系统时间
        val beforeSerialization = System.currentTimeMillis()
        // 序列化结果
        val valueBytes = resultSer.serialize(value)
        // 结果记录序列化完成的系统时间
        val afterSerialization = System.currentTimeMillis()

        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        //这里，是计算出了task相关的一些metrics，就是统计信息
        //那么包括运行了多长时间，反序列化耗费了多长时间，java
        //虚拟机gc耗费了多长时间，结果的序列化耗费了，这些东西，
        //其实会在我们的SparkUI上显示，那么大家真正在企业中运行
        //我们的长时间运行的spark程序时，肯定会到4040端口，去观察SparkUI


        // 反序列化发生在两个地方：
        // 1. 在该函数下反序列化Task信息以及Task实例。
        // 2. 在任务启动后，Task.run 反序列化 RDD 和 函数
        // 计算task的反序列化费时
        task.metrics.setExecutorDeserializeTime(
          (taskStart - deserializeStartTime) + task.executorDeserializeTime)
        // 计算task的反序列化cpu费时
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        // 计算task运行费时
        task.metrics.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
        // 计算task运行cpu费时
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        // 计算GC时间
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        //计算结果序列化时间
        task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()
        // TODO: do not serialize value twice
        // 这里代码存在缺陷：
        // value相当于被序列化了两次
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        val serializedDirectResult = ser.serialize(directResult)
        // 得到结果的大小
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        // 对于计算结果，会根据结果的大小有不同的策略：
        // 1.生成结果在(正无穷,1GB)：
        // 超过1GB的部分结果直接丢弃，
        // 可以通过spark.driver.maxResultSize实现
        // 默认为1G
        // 2.生成结果大小在$[1GB,128MB - 200KB]
        // 会把该结果以taskId为编号存入BlockManager中，
        // 然后把该编号通过Netty发送给Driver，
        // 该阈值是Netty框架传输的最大值
        // spark.akka.frameSize（默认为128MB）和Netty的预留空间reservedSizeBytes（200KB）的差值
        // 3.生成结果大小在（128MB - 200KB,0）：
        // 直接通过Netty发送到Driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }
        //这个非常核心，其实就是调用了Executor所在的CorseGrainedExecutorBackend
        //的statesUpdate() 方法
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case ffe: FetchFailedException =>
          val reason = ffe.toTaskFailedReason
          setTaskFinishedAndClearInterruptStatus()
          // 更新execBackend 状态
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case _: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId)")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case _: InterruptedException if task.killed =>
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId)")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskFailedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // Collect latest accumulator values to report back to the driver
          val accums: Seq[AccumulatorV2[_, _]] =
            if (task != null) {
              task.metrics.setExecutorRunTime(System.currentTimeMillis() - taskStart)
              task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
              task.collectAccumulatorUpdates(taskFailed = true)
            } else {
              Seq.empty
            }

          val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

          val serializedTaskEndReason = {
            try {
              ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
            } catch {
              case _: NotSerializableException =>
                // t is not serializable so just send the stacktrace
                ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
            }
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            SparkUncaughtExceptionHandler.uncaughtException(t)
          }

      } finally {
        // 任务结束后移除
        runningTasks.remove(taskId)
      }
    }
  }

  /**
    * Supervises the killing / cancellation of a task by sending the interrupted flag, optionally
    * sending a Thread.interrupt(), and monitoring the task until it finishes.
    *
    * Spark's current task cancellation / task killing mechanism is "best effort" because some tasks
    * may not be interruptable or may not respond to their "killed" flags being set. If a significant
    * fraction of a cluster's task slots are occupied by tasks that have been marked as killed but
    * remain running then this can lead to a situation where new jobs and tasks are starved of
    * resources that are being used by these zombie tasks.
    *
    * The TaskReaper was introduced in SPARK-18761 as a mechanism to monitor and clean up zombie
    * tasks. For backwards-compatibility / backportability this component is disabled by default
    * and must be explicitly enabled by setting `spark.task.reaper.enabled=true`.
    *
    * A TaskReaper is created for a particular task when that task is killed / cancelled. Typically
    * a task will have only one TaskReaper, but it's possible for a task to have up to two reapers
    * in case kill is called twice with different values for the `interrupt` parameter.
    *
    * Once created, a TaskReaper will run until its supervised task has finished running. If the
    * TaskReaper has not been configured to kill the JVM after a timeout (i.e. if
    * `spark.task.reaper.killTimeout < 0`) then this implies that the TaskReaper may run indefinitely
    * if the supervised task never exits.
    */
  private class TaskReaper(
                            taskRunner: TaskRunner,
                            val interruptThread: Boolean)
    extends Runnable {

    private[this] val taskId: Long = taskRunner.taskId

    private[this] val killPollingIntervalMs: Long =
      conf.getTimeAsMs("spark.task.reaper.pollingInterval", "10s")

    private[this] val killTimeoutMs: Long = conf.getTimeAsMs("spark.task.reaper.killTimeout", "-1")

    private[this] val takeThreadDump: Boolean =
      conf.getBoolean("spark.task.reaper.threadDump", true)

    override def run(): Unit = {
      val startTimeMs = System.currentTimeMillis()

      def elapsedTimeMs = System.currentTimeMillis() - startTimeMs

      def timeoutExceeded(): Boolean = killTimeoutMs > 0 && elapsedTimeMs > killTimeoutMs

      try {
        // Only attempt to kill the task once. If interruptThread = false then a second kill
        // attempt would be a no-op and if interruptThread = true then it may not be safe or
        // effective to interrupt multiple times:
        taskRunner.kill(interruptThread = interruptThread)
        // Monitor the killed task until it exits. The synchronization logic here is complicated
        // because we don't want to synchronize on the taskRunner while possibly taking a thread
        // dump, but we also need to be careful to avoid races between checking whether the task
        // has finished and wait()ing for it to finish.
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // We need to synchronize on the TaskRunner while checking whether the task has
            // finished in order to avoid a race where the task is marked as finished right after
            // we check and before we call wait().
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
            if (takeThreadDump) {
              try {
                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        if (!taskRunner.isFinished && timeoutExceeded()) {
          if (isLocal) {
            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
              "not killing JVM because we are running in local mode.")
          } else {
            // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
            // handler and cause the executor JVM to exit.
            throw new SparkException(
              s"Killing executor JVM because killed task $taskId could not be stopped within " +
                s"$killTimeoutMs ms.")
          }
        }
      } finally {
        // Clean up entries in the taskReaperForTask map.
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // This must have been a TaskReaper where interruptThread == false where a subsequent
              // killTask() call for the same task had interruptThread == true and overwrote the
              // map entry.
            }
          }
        }
      }
    }
  }

  /**
    * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
    * created by the interpreter to the search path
    */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
    * If the REPL is in use, add another ClassLoader that will read
    * new classes defined by the REPL as the user types code
    */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
    * Download any missing dependencies if we receive a new set of files and JARs from the
    * SparkContext. Also adds any new JARs we fetched to the class loader.
    */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    //获取hadoop配置文件
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    //为什么要同步？
    //因为task实际上是以java线程的方式，在一个CoarseGrainedExcutorBackend进行内
    //并发运行的，如果执行业务逻辑的时候，要访问一些共享的资源，那么就可能会出现
    //多线程并发访问安全问题，所以，在这里spark选择进行并发访问的同步(synchronized)
    //为什么在这里要进行多线程并发访问的同步？
    //因为在里面访问了诸如currentFiles等等，这里共享资源
    synchronized {
      // Fetch missing dependencies
      //遍历拉去的文件
      //通过Utils的fetchFile方法，通过网络同喜，从远程拉去文件
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      //遍历要拉去的jar
      for ((name, timestamp) <- newJars) {
        val localName = name.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        //判断时间戳,判断时间戳，要求jar当前时间戳必须小于目标时间戳
        //通过Utils的fetchFile()方法，拉去jar文件
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
      }
    }

    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
    try {
      val response = heartbeatReceiverRef.askWithRetry[HeartbeatResponse](
        message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
    * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
    */
  private def startDriverHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}
