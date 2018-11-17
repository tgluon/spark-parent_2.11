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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
  * 一个ShuffleMapTask将会一个RDD的元素，切分为多个bucket
  * 基于一个在ShuffleDependency中指定的partitionner，默认是
  * HashPartitionner
  * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
  * specified in the ShuffleDependency).
  *
  * See [[org.apache.spark.scheduler.Task]] for more information.
  *
  * @param stageId         id of the stage this task belongs to
  * @param stageAttemptId  attempt id of the stage this task belongs to
  * @param taskBinary      broadcast version of the RDD and the ShuffleDependency. Once deserialized,
  *                        the type should be (RDD[_], ShuffleDependency[_, _, _]).
  * @param partition       partition of the RDD this task is associated with
  * @param locs            preferred task execution locations for locality scheduling
  * @param metrics         a `TaskMetrics` that is created at driver side and sent to executor side.
  * @param localProperties copy of thread-local properties set by the user on the driver side.
  *
  *                        The parameters below are optional:
  * @param jobId           id of the job this task belongs to
  * @param appId           id of the app this task belongs to
  * @param appAttemptId    attempt id of the app this task belongs to
  */
private[spark] class ShuffleMapTask(
                                     stageId: Int,
                                     stageAttemptId: Int,
                                     taskBinary: Broadcast[Array[Byte]],
                                     partition: Partition,
                                     @transient private var locs: Seq[TaskLocation],
                                     metrics: TaskMetrics,
                                     localProperties: Properties,
                                     jobId: Option[Int] = None,
                                     appId: Option[String] = None,
                                     appAttemptId: Option[String] = None)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, metrics, localProperties, jobId,
    appId, appAttemptId)
    with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition {
      override def index: Int = 0
    }, null, null, new Properties)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  //非常重要的一点，就是ShuffleMapTask的runTask()方法，有MapStatus返回
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    // 记录反序列化开始时间
    val deserializeStartTime = System.currentTimeMillis()
    // 记录反序列化开始时的Cpu时间
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    //对task要处理的rdd相关的数据，做一些反序列化操作
    // 这个rdd，关键问题，你是怎么拿到的？
    //因为大家知道，多个task运行在多个executor中，都是
    //并行运行，或者并行运行的，可能数据不再一个地方，但是
    //一个stage的task，其实要处理的rdd是一样的，所以task怎么
    //拿到自己处理的那个rdd的数据，这里，会通过broadcast variable直接拿到


    // 反序列化rdd 及其 依赖
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    // 计算 反序列化费时
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    // 计算 反序列化Cpu费时
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    var writer: ShuffleWriter[Any, Any] = null
    try {
      //获取ShuffleManager
      //从SparkEnv获取ShuffleManager，在系统启动时，会根据设置进行初始化

      //获取shuffleManager
      val manager = SparkEnv.get.shuffleManager
      //调用RDD进行计算，通过HashShuffleWriter的writer方法把RDD的计算结果持久化

      // 调用writer.write 开始计算RDD，
      // 获取 writer
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // 最重要的一行代码就在这里
      // 首先调用了，rdd的itorator()方法，并且传入了，当前
      // task要处理那个partition，所以核心的逻辑，就在rdd的
      //  iterator()方法中，在这里，就实现了针对rdd的某个partition，
      // 执行我们自己定义的算子，或者函数执行完了我们自己定义的算子，
      // 或者函数，是不是相当于，rdd的partition执行了处理，那么
      // 是不是会有返回的数据？返回的数据都是通过ShuffleWriter，经过
      //HashPartitioner进行分区之后，写入自己对应的分区bucket
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      //最后，返回结果，MapStatus
      //MapStatus里面封装了ShuffleMapTask计算的数据，存储在哪里，其实
      //既是BlockManager相关的信息，BlockManager，是spark底层的内存、数据
      //磁盘数据管理的组件，讲完Shuffle之后，我们就来剖析BlockManager

      // 停止计算，并返回结果
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
