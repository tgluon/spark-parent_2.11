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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter}

/**
  * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
  * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
  * optionally sorts keys within each partition using a custom Comparator. Can output a single
  * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
  *
  * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
  *
  * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
  * to its use in sort-based shuffle (for example, its block compression is controlled by
  * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
  * non-shuffle contexts where we might want to use different configuration settings.
  *
  * @param aggregator  optional Aggregator with combine functions to use for merging data
  * @param partitioner optional Partitioner; if given, sort by partition ID and then key
  * @param ordering    optional Ordering to sort keys within each partition; should be a total ordering
  * @param serializer  serializer to use when spilling to disk
  *
  *                    Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
  *                    want the output keys to be sorted. In a map task without map-side combine for example, you
  *                    probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
  *                    want to do combining, having an Ordering is more efficient than not having it.
  *
  *                    Users interact with this class in the following way:
  *
  * 1. Instantiate an ExternalSorter.
  *
  * 2. Call insertAll() with a set of records.
  *
  * 3. Request an iterator() back to traverse sorted/aggregated records.
  *     - or -
  *                    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs
  *                    that can be used in Spark's sort shuffle.
  *
  *                    At a high level, this class works internally as follows:
  *
  *  - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
  *                    we want to combine by key, or a PartitionedPairBuffer if we don't.
  *                    Inside these buffers, we sort elements by partition ID and then possibly also by key.
  *                    To avoid calling the partitioner multiple times with each key, we store the partition ID
  *                    alongside each record.
  *
  *  - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
  *                    by partition ID and possibly second by key or by hash code of the key, if we want to do
  *    aggregation. For each file, we track how many objects were in each partition in memory, so we
  *                    don't have to write out the partition ID for every element.
  *
  *  - When the user requests an iterator or file output, the spilled files are merged, along with
  *                    any remaining in-memory data, using the same sort order defined above (unless both sorting
  *                    and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
  *                    from the ordering parameter, or read the keys with the same hash code and compare them with
  *                    each other for equality to merge values.
  *
  *  - Users are expected to call stop() at the end to delete all the intermediate files.
  */
private[spark] class ExternalSorter[K, V, C](
                                              context: TaskContext,
                                              aggregator: Option[Aggregator[K, V, C]] = None,
                                              partitioner: Option[Partitioner] = None,
                                              ordering: Option[Ordering[K]] = None,
                                              serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
    with Logging {

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val serializerManager = SparkEnv.get.serializerManager
  private val serInstance = serializer.newInstance()

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.

  /**
    * ①在map端进行combine：PartitionedAppendOnlyMap 是map类型的数据结构，map是key-value ，
    * 在本地进行聚合，在本地key值不变，Value不断进行更新；PartitionedAppendOnlyMap 底层还是一个
    * 数组，基于数组实现map的原因是更节省空间，效率更高。那么直接基于数组怎么实现map：把数组的标记 0 1 2 3 4 ….
    * 把偶数设置为map的key值，把奇数设置为map的value值。
    */
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  //②在map端没有combine：使用PartitionedPairBuffer看一下insertAll方法：
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

  // Total spilling statistics
  private var _diskBytesSpilled = 0L

  def diskBytesSpilled: Long = _diskBytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L

  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  @volatile private var isShuffleSort: Boolean = true
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  @volatile private var readingIterator: SpillableIterator = null

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  private[this] case class SpilledFile(
                                        file: File,
                                        blockId: BlockId,
                                        serializerBatchSizes: Array[Long],
                                        elementsPerPartition: Array[Long])

  private val spills = new ArrayBuffer[SpilledFile]

  /**
    * Number of files this sorter has spilled so far.
    * Exposed for testing.
    */
  private[spark] def numSpills: Int = spills.size

  /**
    * 先判断是否需要进行聚合(Aggregation),如果需要，则根据键值进行合并(Combine)
    * 然后把这些数据写入到内存缓冲区中，如果排序中的Map占用的内存已经超越了使用
    * 的阀值，则将Map中的内容溢出到磁盘中，每一次溢写产生一个不同的文件。如果不
    * 聚合，则直接把数据存到内存缓存区
    */
  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    //获取外部排序中是否需要进行聚合
    // 判断aggregator是否为空，如果不为空，表示需要在本地combine
    val shouldCombine = aggregator.isDefined
    // 如果需要本地combine
    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      // 使用AppendOnlyMap优先在内存中进行combine
      // 获取aggregator的merge函数，用于merge新的值到聚合记录
      val mergeValue = aggregator.get.mergeValue
      // 获取aggregator的createCombiner函数，用于创建聚合的初始值
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      // 创建update函数，如果有值进行mergeValue,如果没有则createCombiner
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        // 处理一个元素，就更新一次结果
        addElementsRead()
        // 取出一个(key,value)
        kv = records.next()
        // 对key计算分区，然后开始进行merge
        map.changeValue((getPartition(kv._1), kv._1), update)
        //对数据进行排序并写入到内存缓冲区中，如果排序中的Map占用的内存已经
        //超越了使用的阀值，则将Map中的内容溢写到磁盘中，每一次溢写产生一个
        //不同文件
        // 如果需要溢写内存数据到磁盘
        maybeSpillCollection(usingMap = true)
      }
    } else {
      // 不需要进行本地combine
      // Stick values into our buffer
      //不需要进行聚合(Aggregation),对数据进行排序并写入到内存缓冲区中
      while (records.hasNext) {
        // 处理一个元素，就更新一次结果
        addElementsRead()
        // 取出一个(key,value)
        val kv = records.next()
        // 往PartitionedPairBuffer添加数据
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        // 如果需要溢写内存数据到磁盘
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
    * Spill the current in-memory collection to disk if needed.
    *
    * @param usingMap whether we're using a map or buffer as our current in-memory collection
    */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    // 如果使用PartitionedAppendOnlyMap存放数据，主要方便进行聚合
    if (usingMap) {
      // 首先估计一下该map的大小
      estimatedSize = map.estimateSize()
      // 然后会根据预估的map大小决定是否需要进行spill
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      //否则使用PartitionedPairBuffer,以用于本地不需要进行聚合的情况
      // 首先估计一下该map的大小
      estimatedSize = buffer.estimateSize()
      // 然后会根据预估的map大小决定是否需要进行spill
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
    *
    * Spill our in-memory collection to a sorted file that we can merge later.
    * We add this file into `spilledFiles` to find it later.
    *
    **/

  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // 返回一个根据指定的比较器排序的迭代器
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    // 溢写内存里的数据到磁盘一个临时文件
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    // 更新溢写的临时磁盘文件
    spills += spillFile
  }

  /**
    * Force to spilling the current in-memory collection to disk to release memory,
    * It will be called by TaskMemoryManager when there is not enough memory for the task.
    */
  override protected[this] def forceSpill(): Boolean = {
    if (isShuffleSort) {
      false
    } else {
      assert(readingIterator != null)
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        map = null
        buffer = null
      }
      isSpilled
    }
  }

  /**
    * Spill contents of in-memory iterator to a temporary file on disk.
    */
  //  创建临时的blockId和文件
  //  针对临时文件创建DiskBlockObjectWriter
  //  循环读取内存里的数据
  //  内存里的数据数据写入文件
  //  将数据刷到磁盘
  //  创建SpilledFile然后返回
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator)
  : SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    // 因为这些文件在shuffle期间可能被读取，他们压缩应该被spark.shuffle.spill.compress控制而不是
    // spark.shuffle.compress，所以我们需要创建临时的shuffle block
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    // 创建针对临时文件的writer
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    // 批量写入磁盘的列表
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    // 每一个分区有多少数据
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    // 刷新数据到磁盘
    def flush(): Unit = {
      // 每一个分区对应文件刷新到磁盘，并返回对应的FileSegment
      val segment = writer.commitAndGet()
      // 获取该FileSegment对应的文件的长度，并且更新batchSizes
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      // 循环读取内存里的数据
      while (inMemoryIterator.hasNext) {
        // 获取partitionId
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        // 内存里的数据数据写入文件
        inMemoryIterator.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1
        // // 将数据刷到磁盘
        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      // 遍历完了之后，刷新到磁盘
      if (objectsWritten > 0) {
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (success) {
        writer.close()
      } else {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }
    // 创建SpilledFile然后返回
    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }

  /**
    * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
    * inside each partition. This can be used to either write out a new file or return data to
    * the user.
    *
    * Returns an iterator over all the data written to this object, grouped by partition. For each
    * partition we then have an iterator over its contents, and these are expected to be accessed
    * in order (you can't "skip ahead" to one partition without reading the previous one).
    * Guaranteed to return a key-value pair for each partition, in order of partition ID.
    */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
  : Iterator[(Int, Iterator[Product2[K, C]])] = {
    val readers = spills.map(new SpillReader(_))
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (aggregator.isDefined) {
        // Perform partial aggregation across partitions
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
    * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
    */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
  : Iterator[Product2[K, C]] = {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      override def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
    })
    heap.enqueue(bufferedIters: _*) // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = !heap.isEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  /**
    * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
    * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
    * (e.g. when we sort objects by hash code and different keys may compare as equal although
    * they're not), we still merge them by doing equality tests for all keys that compare as equal.
    */
  private def mergeWithAggregation(
                                    iterators: Seq[Iterator[Product2[K, C]]],
                                    mergeCombiners: (C, C) => C,
                                    comparator: Comparator[K],
                                    totalOrder: Boolean)
  : Iterator[Product2[K, C]] = {
    if (!totalOrder) {
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      new Iterator[Iterator[Product2[K, C]]] {
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)
        }
      }.flatMap(i => i)
    } else {
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  /**
    * An internal class for reading a spilled file partition by partition. Expects all the
    * partitions to be requested in order.
    */
  private[this] class SpillReader(spill: SpilledFile) {
    // Serializer batch offsets; size will be batchSize.length + 1
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // Track which partition and which batch stream we're in. These will be the indices of
    // the next element we will read. We'll also store the last partition read so that
    // readNextPartition() can figure out what partition that was from.
    var partitionId = 0
    var indexInPartition = 0L
    var batchId = 0
    var indexInBatch = 0
    var lastPartitionId = 0

    skipToNextPartition()

    // Intermediate file and deserializer streams that read from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    var fileStream: FileInputStream = null
    var deserializeStream = nextBatchStream() // Also sets fileStream

    var nextItem: (K, C) = null
    var finished = false

    /** Construct a stream that only reads from the next batch */
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchId < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchId)
        fileStream = new FileInputStream(spill.file)
        fileStream.getChannel.position(start)
        batchId += 1

        val end = batchOffsets(batchId)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))

        val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
        serInstance.deserializeStream(wrappedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
      * Update partitionId if we have reached the end of our current partition, possibly skipping
      * empty partitions on the way.
      */
    private def skipToNextPartition() {
      while (partitionId < numPartitions &&
        indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    /**
      * Return the next (K, C) pair from the deserialization stream and update partitionId,
      * indexInPartition, indexInBatch and such to match its location.
      *
      * If the current batch is drained, construct a stream for the next batch and read from it.
      * If no more pairs are left, return null.
      */
    private def readNextItem(): (K, C) = {
      if (finished || deserializeStream == null) {
        return null
      }
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      lastPartitionId = partitionId
      // Start reading the next batch if we're done with this one
      indexInBatch += 1
      if (indexInBatch == serializerBatchSize) {
        indexInBatch = 0
        deserializeStream = nextBatchStream()
      }
      // Update the partition location of the element we're reading
      indexInPartition += 1
      skipToNextPartition()
      // If we've finished reading the last partition, remember that we're done
      if (partitionId == numPartitions) {
        finished = true
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      (k, c)
    }

    var nextPartitionToRead = 0

    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    def cleanup() {
      batchId = batchOffsets.length // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      if (ds != null) {
        ds.close()
      }
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
    * Returns a destructive iterator for iterating over the entries of this map.
    * If this iterator is forced spill to disk to release memory when there is not enough memory,
    * it returns pairs from an on-disk map.
    */
  def destructiveIterator(memoryIterator: Iterator[((Int, K), C)]): Iterator[((Int, K), C)] = {
    if (isShuffleSort) {
      memoryIterator
    } else {
      readingIterator = new SpillableIterator(memoryIterator)
      readingIterator
    }
  }

  /**
    * Return an iterator over all the data written to this object, grouped by partition and
    * aggregated by the requested aggregator. For each partition we then have an iterator over its
    * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
    * partition without reading the previous one). Guaranteed to return a key-value pair for each
    * partition, in order of partition ID.
    *
    * For now, we just merge all the spilled files in once pass, but this can be modified to
    * support hierarchical merging.
    * Exposed for testing.
    */
  //  没有溢写，则判断是否需要对key排序，如果不需要则只是将数据按照partitionId排序，
  //  否则首先按照partitionId排序，然后partition内部再按照key排序
  //  如果发生溢写，则需要将磁盘上溢写文件和内存里的数据进行合并
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    // 是否需要本地combine
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    // 如果没有发生磁盘溢写
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      // 而且不需要排序
      if (!ordering.isDefined) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        // 数据只是按照partitionId排序，并不会对key进行排序
        groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(None)))
      } else {
        // We do need to sort by both partition ID and key
        // 否则我们需要先按照partitionId排序，然后分区内部对key进行排序
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      // Merge spilled and in-memory data
      // 如果发生了溢写操作，则需要将磁盘上溢写文件和内存里的数据进行合并
      merge(spills, destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }

  /**
    * Return an iterator over all the data written to this object, aggregated by our aggregator.
    */
  def iterator: Iterator[Product2[K, C]] = {
    isShuffleSort = false
    partitionedIterator.flatMap(pair => pair._2)
  }

  /**
    * Write all the data added into this ExternalSorter into a file in the disk store. This is
    * called by the SortShuffleWriter.
    *
    * @param blockId block ID to write to. The index file will be blockId.name + ".index".
    * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
    */
  //  溢写文件为空，则内存足够，不需要溢写结果到磁盘, 返回一个对结果排序的迭代器, 遍历数据写入data临时文件；
  //  再将数据刷到磁盘文件，返回FileSegment对象；构造一个分区文件长度的数组 溢写文件不为空，则需要将溢写的
  //  文件和内存数据合并，合并之后则需要进行归并排序(merge-sort)；数据写入data临时文件，再将数据刷到磁盘文件
  //  ，返回FileSegment对象；构造一个分区文件长度的数组返回分区文件长度的数组
  def writePartitionedFile(
                            blockId: BlockId,
                            outputFile: File): Array[Long] = {
    // Track location of each range in the output file
    // 临时的data文件跟踪每一个分区的位置
    // 创建每一个分区对应的文件长度的数组

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)
    // 创建DiskBlockObjectWriter对象
    val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
      context.taskMetrics().shuffleWriteMetrics)
    // 判断是否有进行spill的文件
    if (spills.isEmpty) {
      // Case where we only have in-memory data
      // 如果是空的表示我们只有内存数据,内存足够，不需要溢写结果到磁盘
      // 如果指定aggregator，就返回PartitionedAppendOnlyMap里的数据，否则返回
      // PartitionedPairBuffer里的数据
      val collection = if (aggregator.isDefined) map else buffer
      // 返回一个对结果排序的迭代器
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        // 获取partitionId
        val partitionId = it.nextPartition()
        // 通过writer将内存数据写入临时文件
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        // 数据刷到磁盘，并且创建FileSegment数组
        val segment = writer.commitAndGet()
        // 构造一个分区文件长度的数组
        lengths(partitionId) = segment.length
      }
    } else {
      // 否则，表示有溢写文件，则需要进行归并排序(merge-sort)
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      // 每一个分区的数据都写入到data文件的临时文件
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          // 数据刷到磁盘，并且创建FileSegment数组
          val segment = writer.commitAndGet()
          // 构造一个分区文件长度的数组
          lengths(id) = segment.length
        }
      }
    }

    writer.close()
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    lengths
  }

  def stop(): Unit = {
    spills.foreach(s => s.file.delete())
    spills.clear()
    forceSpillFiles.foreach(s => s.file.delete())
    forceSpillFiles.clear()
    if (map != null || buffer != null) {
      map = null // So that the memory can be garbage-collected
      buffer = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
    * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
    * group together the pairs for each partition into a sub-iterator.
    *
    * @param data an iterator of elements, assumed to already be sorted by partition ID
    */
  private def groupByPartition(data: Iterator[((Int, K), C)])
  : Iterator[(Int, Iterator[Product2[K, C]])] = {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
    * An iterator that reads only the elements for a given partition ID from an underlying buffered
    * stream, assuming this partition is the next one to be read. Used to make it easier to return
    * partitioned iterators from our in-memory collection.
    */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]] {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }

  private[this] class SpillableIterator(var upstream: Iterator[((Int, K), C)])
    extends Iterator[((Int, K), C)] {

    private val SPILL_LOCK = new Object()

    private var nextUpstream: Iterator[((Int, K), C)] = null

    private var cur: ((Int, K), C) = readNext()

    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) {
        false
      } else {
        val inMemoryIterator = new WritablePartitionedIterator {
          private[this] var cur = if (upstream.hasNext) upstream.next() else null

          def writeNext(writer: DiskBlockObjectWriter): Unit = {
            writer.write(cur._1._2, cur._2)
            cur = if (upstream.hasNext) upstream.next() else null
          }

          def hasNext(): Boolean = cur != null

          def nextPartition(): Int = cur._1._1
        }
        logInfo(s"Task ${context.taskAttemptId} force spilling in-memory map to disk and " +
          s" it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
        forceSpillFiles += spillFile
        val spillReader = new SpillReader(spillFile)
        nextUpstream = (0 until numPartitions).iterator.flatMap { p =>
          val iterator = spillReader.readNextPartition()
          iterator.map(cur => ((p, cur._1), cur._2))
        }
        hasSpilled = true
        true
      }
    }

    def readNext(): ((Int, K), C) = SPILL_LOCK.synchronized {
      if (nextUpstream != null) {
        upstream = nextUpstream
        nextUpstream = null
      }
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null
      }
    }

    override def hasNext(): Boolean = cur != null

    override def next(): ((Int, K), C) = {
      val r = cur
      cur = readNext()
      r
    }
  }

}
