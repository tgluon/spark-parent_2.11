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

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
  * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
  * requesting them from other nodes' block stores.
  */
private[spark] class BlockStoreShuffleReader[K, C](
                                                    handle: BaseShuffleHandle[K, _, C],
                                                    startPartition: Int,
                                                    endPartition: Int,
                                                    context: TaskContext,
                                                    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
                                                    blockManager: BlockManager = SparkEnv.get.blockManager,
                                                    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  //  创建ShuffleBlockFetcherIterator，一个迭代器，它获取多个块，对于本地块，从本地读取对于远程块，通过远程方法读取
  //  如果reduce端需要聚合：如果map端已经聚合过了，则对读取到的聚合结果进行聚合； 如果map端没有聚合，则针对未合并的<k,v>进行聚合
  //  如果需要对key排序，则进行排序。基于sort的shuffle实现过程中，默认只是按照partitionId排序。在每一个partition内部并没有排序，
  //  因此添加了keyOrdering变量，提供是否需要对分区内部的key排序
  override def read(): Iterator[Product2[K, C]] = {
    // 构造ShuffleBlockFetcherIterator，一个迭代器，它获取多个块，对于本地块，从本地读取
    // 对于远程块，通过远程方法读取
    // 实例化ShuffleBlockFetcherIterator
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      // 通过消息发送获取 ShuffleMapTask 存储数据位置的元数据
      //MapOutputTracker在SparkEnv启动的时候实例化
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      // 设置每次传输的大小
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      //  设置Int的大小
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue))

    // Wrap the streams for compression and encryption based on configuration
    // 基于配置的压缩和加密来包装流
    // 基于配置文件对于流进行包装
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      serializerManager.wrapStream(blockId, inputStream)
    }
    // 获取序列化实例
    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    // 对于每一个流创建一个<key,value>迭代器
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    // 每条记录读取后更新任务度量
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    // 生成完整的迭代器
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    // 传入metricIter到可中断的迭代器
    // 为了能取消迭代
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)
    // 如果reduce端需要聚合
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      // 如果map端已经聚合过了
      if (dep.mapSideCombine) {
        //则对读取到的聚合结果进行聚合
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        // 针对map端各个partition对key进行聚合后的结果再次聚合
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)

      } else {  // 如果map端没有聚合，则针对未合并的<k,v>进行聚合
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    // 如果需要对key排序，则进行排序。基于sort的shuffle实现过程中，默认只是按照partitionId排序
    // 在每一个partition内部并没有排序，因此添加了keyOrdering变量，提供是否需要对分区内部的key排序
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        // 为了减少内存压力和避免GC开销，引入了外部排序器，当内存不足时会根据配置文件
        // spark.shuffle.spill决定是否进行spill操作
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        // 不需要排序直接返回
        aggregatedIter
    }
  }
}
