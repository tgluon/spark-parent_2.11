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

package org.apache.spark.scheduler

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable.{HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._

/**
  * 实现了面向stage的调度机制的高层调度，它会为每个job计算一个stage的DAG
  * (有向无环图)追踪RDD的stage的输出是否被物化(物化就是说，写入磁盘或者内
  * 存等地方)，并且寻找最少小号(最优、最小)调度机制来运行job。他会将stage
  * 作为tasksets提交到底层的TaskSchedulerImpl上，来在集群上运行它们(task)。
  * 除了处理stage的DAG,它负责决定运行每个task的最佳位置，基于当前的缓存状态，
  * 将这些最佳的位置提交底层的TaskSchedulerImpl。此外，它会处理由于shuffle文件
  * 丢失所导致的，会被TaskS
  * cheduler处理，它会多次重试每一个task，直到最后，实在不
  * 行，才会去取消整个stage
  * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
  * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
  * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
  * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
  * tasks that can run right away based on the data that's already on the cluster (e.g. map output
  * files from previous stages), though it may fail if this data becomes unavailable.
  *
  * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
  * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
  * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
  * set of map output files, and another to read those files after a barrier). In the end, every
  * stage will have only shuffle dependencies on other stages, and may compute multiple operations
  * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
  * various RDDs (MappedRDD, FilteredRDD, etc).
  *
  * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
  * locations to run each task on, based on the current cache status, and passes these to the
  * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
  * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
  * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
  * a small number of times before cancelling the whole stage.
  *
  * When looking through this code, there are several key concepts:
  *
  *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
  * For example, when the user calls an action, like count(), a job will be submitted through
  *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
  *
  *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
  * task computes the same function on partitions of the same RDD. Stages are separated at shuffle
  * boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
  * fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
  * executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
  * Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
  *
  *  - Tasks are individual units of work, each sent to one machine.
  *
  *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
  * and likewise remembers which shuffle map stages have already produced output files to avoid
  * redoing the map side of a shuffle.
  *
  *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
  * on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
  *
  *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
  * to prevent memory leaks in a long-running application.
  *
  * To recover from failures, the same stage might need to run multiple times, which are called
  * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
  * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
  * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
  * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
  * stage(s) that compute the missing tasks. As part of this process, we might also have to create
  * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
  * tasks from the old attempt of a stage could still be running, care must be taken to map any
  * events received in the correct Stage object.
  *
  * Here's a checklist to use when making or reviewing changes to this class:
  *
  *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
  * accumulation of state in long-running programs.
  *
  *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
  * include the new structure. This will help to catch memory leaks.
  */
private[spark]
class DAGScheduler(
                    private[scheduler] val sc: SparkContext,
                    private[scheduler] val taskScheduler: TaskScheduler,
                    listenerBus: LiveListenerBus,
                    mapOutputTracker: MapOutputTrackerMaster,
                    blockManagerMaster: BlockManagerMaster,
                    env: SparkEnv,
                    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)

  private[scheduler] def numTotalJobs: Int = nextJobId.get()

  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
    * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
    * that dependency. Only includes stages that are part of currently running job (when the job(s)
    * that require the shuffle stage complete, the mapping will be removed, and the only record of
    * the shuffle data will be in the MapOutputTracker).
    */
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
    * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
    * and its values are arrays indexed by partition numbers. Each array value is the set of
    * locations where that RDD partition is cached.
    *
    * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
    */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private[scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  /**
    * Called by the TaskSetManager to report task's starting.
    */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
    * Called by the TaskSetManager to report that a task has completed
    * and results are being fetched remotely.
    */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
    * Called by the TaskSetManager to report task completions or failures.
    */
  def taskEnded(
                 task: Task[_],
                 reason: TaskEndReason,
                 result: Any,
                 accumUpdates: Seq[AccumulatorV2[_, _]],
                 taskInfo: TaskInfo): Unit = {
    //发送CompletionEvent信号
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo))
  }

  /**
    * Update metrics for in-progress tasks and let the master know that the BlockManager is still
    * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
    * indicating that the block manager should re-register.
    */
  def executorHeartbeatReceived(
                                 execId: String,
                                 // (taskId, stageId, stageAttemptId, accumUpdates)
                                 accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
                                 blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
    * Called by TaskScheduler implementation when an executor fails.
    */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
    * Called by TaskScheduler implementation when a host is added.
    */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
    * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
    * cancellation of the job itself.
    */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
    * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
    * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
    * addition to any missing ancestor shuffle map stages.
    */
  private def getOrCreateShuffleMapStage(
                                          shuffleDep: ShuffleDependency[_, _, _],
                                          firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      ////若找到则直接返回
      case Some(stage) =>
        stage
      // 检查这个Stage的Parent Stage是否生成
      // 若没有，则生成它们
      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            // 生成新的Stage
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }

  /**
    * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
    * previously run stage generated the same shuffle data, this function will copy the output
    * locations that are still available from the previous shuffle to avoid unnecessarily
    * regenerating data.
    */
  def createShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)

    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // A previously run stage generated partitions for this shuffle, so for each output
      // that's still available, copy information about that output location to the new stage
      // (so we don't unnecessarily re-compute that data).
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      (0 until locs.length).foreach { i =>
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
    * Create a ResultStage associated with the provided jobId.
    */
  // Spark的Stage调用是从最后一个RDD所在的Stage，ResultStage开始划分的，这里即为G所在的Stage。
  // 但是在生成这个Stage之前会生成它的parent Stage，就这样递归的把parent Stage都先生成了。
  private def createResultStage(
                                 rdd: RDD[_],
                                 func: (TaskContext, Iterator[_]) => _,
                                 partitions: Array[Int],
                                 jobId: Int,
                                 callSite: CallSite): ResultStage = {

    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
    * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
    * the provided firstJobId.
    */
  private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getMissingAncestorShuffleDependencies(
                                                     rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val ancestors = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        getShuffleDependencies(toVisit).foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.push(shuffleDep)
            waitingForVisit.push(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
  }

  /**
    * Returns shuffle dependencies that are immediate parents of the given RDD.
    *
    * This function will not return more distant ancestors.  For example, if C has a shuffle
    * dependency on B which has a shuffle dependency on A:
    *
    * A <-- B <-- C
    *
    * calling this function with rdd C will only return the B <-- C dependency.
    *
    * This function is scheduler-visible for the purpose of unit testing.
    */
  private[scheduler] def getShuffleDependencies(
                                                 rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new Stack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.push(dependency.rdd)
        }
      }
    }
    parents
  }

  //获取stage的父stage
  //这个方法的意思，就是说，对一个stage
  // 如果它的最后一个rdd的所有依赖，都是窄依赖，那么就不会创建任何新的stage
  //然后立即将新的stage返回
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]

    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          //遍历rdd的依赖
          //所以说，针对我们之前将的那个图，来看看
          //其实对于每一种有shuffle的操作，比如groupByKey、reduceByKey
          //countBykey底层对应了两个RDD：MapPartionsRDD、ShuffleRDD
          for (dep <- rdd.dependencies) {
            dep match {
              //如果是宽依赖
              case shufDep: ShuffleDependency[_, _, _] =>
                //那么使用宽依赖的那个rdd，创建一个stage,并且会将isShuffleMap设置为true
                //但是finalStage之前所有的stage，都是shuffleMap stage
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              //如果是窄依赖，那么将依赖的rdd放入栈中
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }

    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
    * Registers the given jobId among the jobs that need the given stage and
    * all of that stage's ancestors.
    */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter {
          !_.jobIds.contains(jobId)
        }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }

    updateJobIdStageIdMapsList(List(stage))
  }

  /**
    * Removes state for job and any stages that are not needed by any other job.  Does not
    * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
    *
    * @param job The job whose state to cleanup.
    */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
                .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
    * Submit an action job to the scheduler.
    *
    * @param rdd           target RDD to run tasks on
    * @param func          a function to run on each partition of the RDD
    * @param partitions    set of partitions to run on; some jobs may not want to compute on all
    *                      partitions of the target RDD, e.g. for operations like first()
    * @param callSite      where in the user program this job was called
    * @param resultHandler callback to pass each result to
    * @param properties    scheduler properties to attach to this job, e.g. fair scheduler pool name
    * @return a JobWaiter object that can be used to block until the job finishes executing
    *         or can be used to cancel the job.
    * @throws IllegalArgumentException when partitions ids are illegal
    */

  def submitJob[T, U](
                       rdd: RDD[T],
                       func: (TaskContext, Iterator[T]) => U,
                       partitions: Seq[Int],
                       callSite: CallSite,
                       resultHandler: (Int, U) => Unit,
                       properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    // 确认没在不存在的partition上执行任务
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }
    //递增得到jobId
    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      //若Job没对任何一个partition执行任务，
      //则立即返回
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }


  /**
    * 参数介绍：
    *
    * @param rdd           ： 执行任务的目标RDD
    * @param func          ： 在RDD的分区上所执行的函数
    * @param partitions    ： 需要执行的分区集合;有些job并不会对RDD的所有分区都进行计算的，比如说first()
    * @param callSite      ：用户程序的调用点
    * @param resultHandler ：回调结果
    * @param properties    ：关于这个job的调度器特征，比如说公平调度的pool名字，这个会在后续讲到
    */
  def runJob[T, U](
                    rdd: RDD[T],
                    func: (TaskContext, Iterator[T]) => U,
                    partitions: Seq[Int],
                    callSite: CallSite,
                    resultHandler: (Int, U) => Unit,
                    properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    // Note: Do not call Await.ready(future) because that calls `scala.concurrent.blocking`,
    // which causes concurrent SQL executions to fail if a fork-join pool is used. Note that
    // due to idiosyncrasies in Scala, `awaitPermission` is not actually used anywhere so it's
    // safe to pass in null here. For more detail, see SPARK-13747.
    //判断job执行结果
    val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
    waiter.completionFuture.ready(Duration.Inf)(awaitPermission)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
    * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
    * as they arrive. Returns a partial result object from the evaluator.
    *
    * @param rdd        target RDD to run tasks on
    * @param func       a function to run on each partition of the RDD
    * @param evaluator  [[ApproximateEvaluator]] to receive the partial results
    * @param callSite   where in the user program this job was called
    * @param timeout    maximum time to wait for the job, in milliseconds
    * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
    */
  def runApproximateJob[T, U, R](
                                  rdd: RDD[T],
                                  func: (TaskContext, Iterator[T]) => U,
                                  evaluator: ApproximateEvaluator[U, R],
                                  callSite: CallSite,
                                  timeout: Long,
                                  properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult() // Will throw an exception if the job fails
  }

  /**
    * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
    * can be used to block until the job finishes executing or can be used to cancel the job.
    * This method is used for adaptive query planning, to run map stages and look at statistics
    * about their outputs before submitting downstream stages.
    *
    * @param dependency the ShuffleDependency to run a map stage for
    * @param callback   function called with the result of the job, which in this case will be a
    *                   single MapOutputStatistics object showing how much data was produced for each partition
    * @param callSite   where in the user program this job was submitted
    * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
    */
  def submitMapStage[K, V, C](
                               dependency: ShuffleDependency[K, V, C],
                               callback: MapOutputStatistics => Unit,
                               callSite: CallSite,
                               properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter(this, jobId, 1, (i: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
    * Cancel a job that is running or waiting in the queue.
    */
  def cancelJob(jobId: Int): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  /**
    * Cancel all jobs in the given job group ID.
    */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
    * Cancel all jobs that are running or waiting in the queue.
    */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**
    * Cancel all jobs associated with a running or scheduled stage.
    */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
    * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
    * the last fetch failure.
    */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
  }

  /**
    * Check for waiting stages which are now eligible for resubmission.
    * Submits stages that depend on the given parent stage. Called when the parent stage completes
    * successfully.
    */
  private def submitWaitingChildStages(parent: Stage) {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
  }

  private[scheduler] def handleTaskSetFailed(
                                              taskSet: TaskSet,
                                              reason: String,
                                              exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach {
      abortStage(_, reason, exception)
    }
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  //DAGScheduler的job调度的核心入口,自此Job的提交就完成了：
  private[scheduler] def handleJobSubmitted(jobId: Int,
                                            finalRDD: RDD[_],
                                            func: (TaskContext, Iterator[_]) => _,
                                            partitions: Array[Int],
                                            callSite: CallSite,
                                            listener: JobListener,
                                            properties: Properties) {
    ////使用触发job的最后一个RDD，创建finalStage
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      //创建一个stage对象
      //并且将stage加入DAGScheduler内部的内存缓存中
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    //第二步，用finalstage,创建一个stage，当然就是我们的finalStage了
    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))
    //得到job提交的时间
    val jobSubmissionTime = clock.getTimeMillis()
    //第三步，将job加入内存缓存中
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    //第四步，使用submitStage()方法提交finalStage
    //这个方法的调用，其实会导致第一个stage提交
    //并且导致其他所有的stage，都放入waitingStages队列里了
    submitStage(finalStage)

    /**
      * stage划分算法，实在太重要，对于spark高手，或者spark
      * 精通人员来说，必须对stage划分算法很清晰，直到你自己编写
      * 的spark application被划分为了几个job，每个job被划分
      * 成几个stage, 每个stage，包括你的那些代码，只有直到了每
      * 个stage包括了你的那些代码之后，在线上，如果你发现某个stage
      * 执行特别慢，或者某个stage一直报错
      *
      * stage划分算法总结
      *1.从finalStage倒推
      *2.通过宽依赖，来进行新的stage的划分
      *3.使用递归，优先提交父stage
      *
      */
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
                                                 dependency: ShuffleDependency[_, _, _],
                                                 callSite: CallSite,
                                                 listener: JobListener,
                                                 properties: Properties) {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  //提交stage方法
  //这个，其实就是stage划分算法的入口但是，stage划分算法，其实是由submitStage()方法与getMissingParentStage()
  // 方法共同组成的
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      //调用getMissingParentStages()方法，去获取当前这个stage的父stage
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        //这里其实会反复递归调用，直到最初的stage，它没有父stage了
        //那么，此时，就会去首先提交这个第一个stage，stage0，其余
        //的stage此时全部都在waitingStages里面
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          //递归调用submit方法，去提交父stage
          //这里的递归，就是stage划分算法的推动者和精髓
          for (parent <- missing) {
            submitStage(parent)
          }
          //并且将当前stage放入waitingStages等待执行的stage的队列中
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
  //提交stage，为stage创建一批task，task数量与partition数量相同
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    // pendingPartitions 是 HashSet[Int]
    //存储待处理的Task
    stage.pendingPartitions.clear()

    // First figure out the indexes of partition ids to compute.
    // 找出还未就算的Partition
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    //从一个ActiveJob中得到关于这个Stage的
    //调度池，job组描述等信息
    val properties = jobIdToActiveJob(jobId).properties
    // runningStages 是 HashSet[Stage]
    //将当前Stage加入到运行中Stage集合
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    //计算每个task对应partition的最佳位置
    //说白了，就是从stage的最后一个rdd开始 ，去找，哪个rdd的partition，
    //是被cache了，或者checkpoint了，那么，task的最佳位置，就是缓存的/
    //checkpoint的的parttion的位置，因为这样的话，task就在那个节点上执行，
    // 不需要计算之前的rdd了
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          //ShuffleMapStage最佳位置的计算
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id)) }.toMap
        case s: ResultStage =>
          //ResultStage最佳位置的计算
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        //向listenerBus发送SparkListenerStageSubmitted事件
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      //对于最后一个Stage的Task，
      //序列化并广播(rdd, func)。
      //若是其他的Stage的Task，
      //序列化并广播(rdd, shuffleDep)
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      //若序列化失败，停止这个stage
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        // 停止执行
        return
    }
    //为stage创建指定数量的task
    //这里很关键的一点，就是task最佳位置的计算
    val tasks: Seq[Task[_]] = try {
      //对于最后一个Stage的Task，
      //则创建ResultTask。
      //若是其他的Stage的Task，
      //则创建ShuffleMapTask。
      stage match {
        case stage: ShuffleMapStage =>
          //给每一个parttion创建一个task
          //给每个task计算最佳位置
          partitionsToCompute.map { id =>
            //计算task的最佳位置
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            //然后对于finalStage之外的stage，它的isShufflerMap都是true
            //所以会创建ShuffleMapTask
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.latestInfo.taskMetrics, properties, Option(jobId),
              Option(sc.applicationId), sc.applicationAttemptId)
          }
        //如果不是shuffleMap,那么就是finalStage,finalStage，是创建ResultTask的
        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }
    //创建TaskSet并提交
    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingPartitions ++= tasks.map(_.partitionId)
      logDebug("New pending partitions: " + stage.pendingPartitions)
      //最后，针对stage的task，创建TaskSet对象，调用TaskScheduler的submitTasks()方法，
      // 提交TaskSet，默认情况下，我们的standalone模式，是使用TaskScheduler,是一个trait
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage: ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)

      submitWaitingChildStages(stage)
    }
  }

  /**
    * Merge local values from a task into the corresponding accumulators previously registered
    * here on the driver.
    *
    * Although accumulators themselves are not thread-safe, this method is called only from one
    * thread, the one that runs the scheduling loop. This means we only handle one task
    * completion event at a time so we don't need to worry about locking the accumulators.
    * This still doesn't stop the caller from updating the accumulator outside the scheduler,
    * but that's not our problem since there's nothing we can do about that.
    */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    try {
      event.accumUpdates.foreach { updates =>
        val id = updates.id
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.accumulables += acc.toInfo(Some(updates.value), Some(acc.value))
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to update accumulators for task ${task.partitionId}", e)
    }
  }

  /**
    * Responds to a task finishing. This is called inside the event loop so it assumes that it can
    * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
    */

  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val taskId = event.taskInfo.id
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // Reconstruct task metrics. Note: this may be null if the task has failed.
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    // The stage may have already finished when we get this event -- eg. maybe it was a
    // speculative task. It is important that we send the TaskEnd event in any case, so listeners
    // are properly notified and can chose to handle it. For instance, some listeners are
    // doing their own accounting and if they don't get the task end event they think
    // tasks are still running when they really aren't.
    listenerBus.post(SparkListenerTaskEnd(
      stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }
    //根据stageId 得到stage
    val stage = stageIdToStage(task.stageId)
    //这里的event就是completion
    event.reason match {
      //这里只看成功的流程
      case Success =>
        //将这个task 从stage等待处理分区中删去
        stage.pendingPartitions -= task.partitionId
        task match {
          //若是最后一个Stage的task
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            //将stage 转为 ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              //获取这Stage的job
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                  //标记状态
                  job.finished(rt.outputId) = true
                  //计数
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  // 若Job的所有partition都完成了，
                  // 移除这个Job
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  //通知 JobWaiter 有任务成功
                  //但 taskSucceeded 会运行用户自定义的代码
                  //因此可能抛出异常
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      // 标记为失败
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }
          //若不是最后一个Stage的Task
          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            //将Task的partitionId和statu追加到OutputLoc
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              shuffleStage.addOutputLoc(smt.partitionId, status)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              //将outputLoc信息注册到mapOutputTracker
              //首先ShuffleMapTask的计算结果（其实是计算结果数据所在的位置、大小等元数据信息）都会传给Driver的mapOutputTracker。
              // 所以 DAGScheduler.newOrUsedShuffleStage需要先判断Stage是否已经被计算过
              ///若计算过，DAGScheduler.newOrUsedShuffleStage则把结果复制到新创建的stage
              //如果没计算过，DAGScheduler.newOrUsedShuffleStage就向注册mapOutputTracker Stage，
              // 为存储元数据占位
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocInMapOutputTrackerFormat(),
                changeEpoch = true)

              clearCacheLocs()

              if (!shuffleStage.isAvailable) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                //若Stage不可用（一些任务失败），则从新提交Stage
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                submitStage(shuffleStage)
              } else {
                // Mark any map-stage jobs waiting on this stage as finished
                // 若该Stage的所有分区都完成了
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  for (job <- shuffleStage.mapStageJobs) {
                    //将各个Task的标记为Finished
                    markMapStageJobAsFinished(job, stats)
                  }
                }
                //提交该Stage的正在等在的Child Stages
                submitWaitingChildStages(shuffleStage)
              }
            }
        }
      //重新提交任务
      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        //把任务加入的等待队列
        stage.pendingPartitions += task.partitionId
      //获取结果失败
      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)
        //若失败的尝试ID 不是 stage尝试ID，
        //则忽略这个失败
        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {
          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          //若失败的Stage还在运行队列，
          //标记这个Stage完成
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }
          //若不允许重试，
          //则停止这个Stage
          if (disallowStageRetryForTest) {
            abortStage(failedStage, "Fetch failure will not retry stage due to testing config",
              None)
          } else if (failedStage.failedOnFetchAndShouldAbort(task.stageAttemptId)) {
            abortStage(failedStage, s"$failedStage (${failedStage.name}) " +
              s"has failed the maximum allowable number of " +
              s"times: ${Stage.MAX_CONSECUTIVE_FETCH_FAILURES}. " +
              s"Most recent failure reason: ${failureMessage}", None)
          } else {
            if (failedStages.isEmpty) {
              // Don't schedule an event to resubmit failed stages if failed isn't empty, because
              // in that case the event will already have been scheduled.
              //若失败的Stage中，没有个task完成了，
              //则重新提交Stage。
              //若果有完成的task的话，我们不能重新提交Stage，
              //因为有些task已经被调度过了。
              //task级别的重新提交是在TaskSetManager.handleFailedTask进行的
              // TODO: Cancel running tasks in the stage
              logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
                s"$failedStage (${failedStage.name}) due to fetch failure")
              messageScheduler.schedule(new Runnable {
                override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
              }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
            }
            failedStages += failedStage
            failedStages += mapStage
          }
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            // 移除OutputLoc中的数据
            // 取消注册mapOutputTracker
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          //当有executor上发生多次获取结果失败，
          //则标记这个executor丢失
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, filesLost = true, Some(task.epoch))
          }
        }
      //拒绝处理
      case commitDenied: TaskCommitDenied =>
      // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits
      // 不做任何事,
      //让 TaskScheduler 来决定如何处理

      //异常
      case exceptionFailure: ExceptionFailure =>
        // Tasks failed with exceptions might still have accumulator updates.
        // 更新accumulator
        updateAccumulators(event)
      //task结果丢失
      case TaskResultLost =>
      // Do nothing here; the TaskScheduler handles these failures and resubmits the task.
      // 不做任何事,
      // 让 TaskScheduler 处理这些错误和重新提交任务


      // executor 丢失
      // 任务被杀死
      // 未知错误
      case _: ExecutorLostFailure | TaskKilled | UnknownReason =>
      // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
      // will abort the job.
      // 不做任何事,
      // 若这task不断的错误，
      // TaskScheduler 会停止 job
    }
  }

  /**
    * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
    * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
    *
    * We will also assume that we've lost all shuffle blocks associated with the executor if the
    * executor serves its own blocks (i.e., we're not using external shuffle), the entire slave
    * is lost (likely including the shuffle service), or a FetchFailed occurred, in which case we
    * presume all shuffle data related to this executor to be lost.
    *
    * Optionally the epoch during which the failure was caught can be passed to avoid allowing
    * stray fetch failures from possibly retriggering the detection of a node as lost.
    */
  private[scheduler] def handleExecutorLost(
                                             execId: String,
                                             filesLost: Boolean,
                                             maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (filesLost || !env.blockManager.externalShuffleServiceEnabled) {
        logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleIdToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          mapOutputTracker.registerMapOutputs(
            shuffleId,
            stage.outputLocInMapOutputTrackerFormat(),
            changeEpoch = true)
        }
        if (shuffleIdToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
        "(epoch " + currentEpoch + ")")
    }
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
  }

  /**
    * Marks a stage as finished and removes it from the list of running stages.
    */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }

    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
    * Aborts all jobs depending on a particular Stage. This is called in response to a task set
    * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
    */
  private[scheduler] def abortStage(
                                     failedStage: Stage,
                                     reason: String,
                                     exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
                                           job: ActiveJob,
                                           failureReason: String,
                                           exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
                ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]

    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              } // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }

    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
    * Gets the locality information associated with a partition of a particular RDD.
    *
    * This method is thread-safe and is called from both DAGScheduler and SparkContext.
    *
    * @param rdd       whose partitions are to be looked at
    * @param partition to lookup locality information for
    * @return list of machines that are preferred by the partition
    */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
    * Recursive implementation for getPreferredLocs.
    *
    * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
    * methods (getCacheLocs()); please be careful when modifying this method, because any new
    * DAGScheduler state accessed by it may require additional synchronization.
    * 计算每个task对应partition的最佳位置
    * 说白了，就是从stage的最后一个rdd开始 ，去找，哪个rdd的partition，
    * 是被cache了，或者checkpoint了，那么，task的最佳位置，就是缓存的/
    * checkpoint的的parttion的位置，因为这样的话，task就在那个节点上执行，
    * 不需要计算之前的rdd了
    */
  private def getPreferredLocsInternal(
                                        rdd: RDD[_],
                                        partition: Int,
                                        visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    //寻找当前 rdd的partition是否缓存了
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    //寻找当前rdd的partition是否checkpoint
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    //最后，递归调用自己，去寻找rdd的父rdd看看对应的partition是否缓存或者checkpoint
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }
    // 如果这个stage，从最后一个rdd，到最开始的rdd parttition都没有被缓存或者
    // checkpoint，那么，task的最佳位置(perferredLocs),就是Nil
    Nil
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop() {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  eventProcessLoop.start()
}

// eventProcessLoop是一个DAGSchedulerEventProcessLoop类对象，
// 即一个DAG调度事件处理的监听。eventProcessLoop中调用doOnReceive来进行监听
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
    * The main event loop of the DAG scheduler.
    */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    // 当事件为JobSubmitted时，
    // 会调用DAGScheduler.handleJobSubmitted
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val filesLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, filesLost)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
