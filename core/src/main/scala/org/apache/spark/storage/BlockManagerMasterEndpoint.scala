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

package org.apache.spark.storage

import java.util.{HashMap => JHashMap}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
  * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
  * of all slaves' block managers.
  * BlockManagerMasterEndpoint，就是负责维护各个executor的BlockManager的元数据
  * BlockManagerInfo BlockStatus
  */
private[spark]
class BlockManagerMasterEndpoint(
                                  override val rpcEnv: RpcEnv,
                                  val isLocal: Boolean,
                                  conf: SparkConf,
                                  listenerBus: LiveListenerBus)
  extends ThreadSafeRpcEndpoint with Logging {

  //该HashMap中存放了BLockManagerId与BlockManager的对应，其中BlockManagerInfo包含了Excutor内存使用情况、
  //数据块的使用情况、已被缓存的数据快和Executor终端引用，通过该引用可以向该Executor发送消息
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]

  // 该HashMap中存放了ExecutorID和 BlockManagerId对应列表
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]
  //该HashMap存放了BlockId和BlockManagerID序列所对应列表，
  //原因在于一个数据块可能存储有多个副本，保存在多个Excutor中
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  private val topologyMapper = {
    val topologyMapperClassName = conf.get(
      "spark.storage.replication.topologyMapper", classOf[DefaultTopologyMapper].getName)
    val clazz = Utils.classForName(topologyMapperClassName)
    val mapper =
      clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
    logInfo(s"Using $topologyMapperClassName for getting topology information")
    mapper
  }

  logInfo("BlockManagerMasterEndpoint up")
  //接收消息并返回结果
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // 注册BlockManager
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint) =>
      context.reply(register(blockManagerId, maxMemSize, slaveEndpoint))
    // 更新block信息
    case _updateBlockInfo@
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      context.reply(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size))
      listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))
    // 根据blockId获取对应的所有BlockManagerId列表
    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))
    // 根据指定的blockId列表，返回多个blockId对应的BlockManagerId集合
    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))
    // 获取指定的blockManagerId是Executor的BlockManager，且不包括指定blockManagerId
    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))
    // 根据executorId获取RPC远程主机和端口号
    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))
    // 获取内存状态
    case GetMemoryStatus =>
      context.reply(memoryStatus)
    // 获取存储状态
    case GetStorageStatus =>
      context.reply(storageStatus)
    // 返回所有block manager的block状态
    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))
    // 获取与过滤条件相匹配的blockId
    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))
    // 删除指定rdd对应的所有blocks
    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))
    // 删除该shuffle对应的所有block
    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))
    // 删除广播数据对应的block
    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))
    //  从worker节点(slave节点)删除对应block
    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)
    // 试图从BlockManagerMaster移除掉这个Executor
    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)
    // 停止StopBlockManagerMaster消息
    case StopBlockManagerMaster =>
      context.reply(true)
      stop()
    // 发送BlockManager心跳检测消息
    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))
    // 判断executorId对应的BlockManager是否有缓存的block
    case HasCachedBlocks(executorId) =>
      blockManagerIdByExecutor.get(executorId) match {
        case Some(bm) =>
          if (blockManagerInfo.contains(bm)) {
            val bmInfo = blockManagerInfo(bm)
            context.reply(bmInfo.cachedBlocks.nonEmpty)
          } else {
            context.reply(false)
          }
        case None => context.reply(false)
      }
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    //在blockLocations和blockManagerInfo中删除该RDD的元数据信息
    //首先，根据RDD编号获取该RDD存储的数据块信息
    // 将所有可以转化为rdd的blockId转化为rddId，然后过滤出和当前指定rddId相等的blocks
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    //然后，根据数据块的信息找出这些数据块所在的BlockMnagerId列表，遍历
    //这些列表，并删除BlockManager包含数据的元数据，同时删除blockLocations
    //对应该数据块的元数据

    // 遍历和该rdd的blocks，从该block对应的BlockManager中删除该block
    // 并且blockLocations也要移除这个block
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    //最后，发送RemoveRdd消息给Executor，通知其删除RDD

    // 然后通过BlockManagerSlaveEndpoint向slave发送RemoveRdd消息
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }
//  只是向slave发送RemoveShuffle消息，让slave去删除shuffle相关的block
  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg)
      }.toSeq
    )
  }

  /**
    * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
    * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
    * from the executors, but not from the driver.
    */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }
//  removeBlockManager 删除BlockManager
  private def removeBlockManager(blockManagerId: BlockManagerId) {
    // 根据blockManaerId获取BlockInfo
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    //从blockMnagerIdByExecutor map中移除executorId对应的BlockManagerInfo

    // 从<ExecutorId,BlockManagerId>中移除diaper该block manager对应的executorId
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    //从blockMnagerInfo map中也移除blockManagerInfo
    // 从<BlockManagerId，BlockMangerInfo>中移除掉这个BlockManager
    blockManagerInfo.remove(blockManagerId)
    val iterator = info.blocks.keySet.iterator
    //遍历BlockManagerInfo内部所有的block块的blockId

    // 遍历该BlockManager所对应的所有block
    while (iterator.hasNext) {
      //清空BlockManagerInfo内部的block的BlockStatus信息
      // 获取每一个blockId
      val blockId = iterator.next
      // 从<BlockId,Set<BlockManagerId>>映射中得到该block所对应的所有BlockManager
      val locations = blockLocations.get(blockId)
      // 所有BlockManager中移除当前要移除的blockManagerId
      locations -= blockManagerId
      // 移除完了之后，Set<BlockManagerId>大小，如果没有数据了，则表示没有对应的
      // BlockManger与之对应，我们应该从<BlockId,Set<BlockManagerId>>移除这个blockId
      if (locations.size == 0) {
        blockLocations.remove(blockId)
      }
    }
    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")
  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    //获取executor对应的BlockManagerInfo，对其调用removeBlockManager()方法
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
    * Return true if the driver knows about the given block manager. Otherwise, return false,
    * indicating that the block manager should re-register.
    */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
//  removeBlockFromWorkers 从worker节点(slave节点)删除对应block
  private def removeBlockFromWorkers(blockId: BlockId) {
    // 获取该block所在的那些BlockManagerId的列表
    val locations = blockLocations.get(blockId)

    if (locations != null) {
      // 遍历blockManagerId列表，然后获取每一个blockManagerId对应的BlockManager
      // 如果这个BlockManager已经定义，则向slave节点发送RemoveBlock消息
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId))
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, info.blocks.asScala)
    }.toArray
  }

  /**
    * Return the block's status for all block managers, if any. NOTE: This is a
    * potentially expensive operation and should only be used for testing.
    *
    * If askSlaves is true, the master queries each block manager for the most updated block
    * statuses. This is useful when the master is not informed of the given block by all block
    * managers.
    */
//  blockStatus 返回所有block manager的block状态
  private def blockStatus(
                           blockId: BlockId,
                           askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
  // 创建GetBlockStatus对象
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
  // 遍历注册过的BlockManagerInfo,如果需要向slave查询，则向BlockManagerSlaveEndpoint发送BlockStatus消息
  // 否则将返回结果封装Future中，最后将结果转化成Map[BlockManagerId, Future[Option[BlockStatus]]]
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future {
            info.getStatus(blockId)
          }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
    * Return the ids of blocks present in all the block managers that match the given filter.
    * NOTE: This is a potentially expensive operation and should only be used for testing.
    *
    * If askSlaves is true, the master queries each block manager for the most updated block
    * statuses. This is useful when the master is not informed of the given block by all block
    * managers.
    */
  private def getMatchingBlockIds(
                                   filter: BlockId => Boolean,
                                   askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future {
              info.blocks.asScala.keys.filter(filter).toSeq
            }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  /**
    * Returns the BlockManagerId with topology information populated, if available.
    * 注册BlockManager
    */
//  register 注册
  private def register(
                        idWithoutTopologyInfo: BlockManagerId,
                        maxMemSize: Long,
                        slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    // the dummy id is not expected to contain the topology information.
    // we get that info here and respond back with a more fleshed out block manager id
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host))

    val time = System.currentTimeMillis()
    //首先判断，如果本地HashMap中没有指定的BlockManagerId，说明从来没有注册过
    //那么才会往下执行，去注册这个BlockManager
  // 如果还没有被注册
    if (!blockManagerInfo.contains(id)) {
      //根据blockManager对应的executor找到对应的BlockManagerInfo
      //这里其实是做一个安全判断，因为，如果blockManagerInfo map里，
      //没有BlockManagerId，那么同步的blockManagerIdByExecutor map里，
      //也必须没有，所以这里要判断一下，如果blockManagerIdByExecutor
      //map里有BlockManagerId，那么做一下清理

      // 获取该executor对应的BlockManagerId
      blockManagerIdByExecutor.get(id.executorId) match {
        // 但是该block对应的executor已经有对应的BlockManager，则表示是旧的BlockManager，则把该Executor删除掉
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
            + s" will replace old one $oldId with new one $id")
          //从内存中，移除掉executorId相关的blockManagerInfo

          // 从内存中移除该Executor以及Executor对应的BlockManager
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(

        id.hostPort, Utils.bytesToString(maxMemSize), id))
      //往blockManagerIdByExecutor map 中保存一份executor到blockManagerId的映射

      // <ExecuotorId，BlockManagerId> 映射加入这个BlockManagerId
      blockManagerIdByExecutor(id.executorId) = id
      //为blockManagerId创建一份BlockManagerId到BlockManagerInfo的映射

      // 创建BlockManagerInfo，加入到<BlockManagerId, BlockManagerInfo>
      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, slaveEndpoint)
    }
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxMemSize))
    id
  }

  /**
    *
    * 更新blockInfo
    * 也就是说，每个BlockManager上，如果block发生了变化，那么都要发送
    * updateBlockInfo请求来BlockManagerMaster这里，进行BlockInfo的更新
    *
    * @param blockManagerId
    * @param blockId
    * @param storageLevel
    * @param memSize
    * @param diskSize
    * @return
    */
//  updateBlockInfo 更新数据块信息
  private def updateBlockInfo(
                               blockManagerId: BlockManagerId,
                               blockId: BlockId,
                               storageLevel: StorageLevel,
                               memSize: Long,
                               diskSize: Long): Boolean = {
  // 如果该blockManagerId还没有注册，则返回
    if (!blockManagerInfo.contains(blockManagerId)) {
      // 如果blockManagerId是driver上的BlockManager而且又不在本地，意思就是这个BlockManager是其他节点的
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }
  // 如果没有block，也不用更新block，所以返回
    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }
    //调用BlockManager的BlockManagerInfo的updateBlockInfo()方法，更新block信息。

  // 调用BlockManagerInfo的updateBlockInfo方法，更新block
    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)
    //每个block可能会在多个BlockManager上面
    //因为如果将StorageLevel设置成带着_2的这种，那么就需要将block
    //replicate一份，放到其他BlockManager上，blockLocations map，
    //其实报错了每个blockId对应的BlockManagerId的set集合，所以这里，
    //会更新blockLocations中的信息，因为是用set存储BlockManagerId，因此
    //自动就去重了

  // 如果blockLocations包含blockId,则获取block对应的所有BlockManager集合,否则创建空的集合
  // 然后更新blockLocations集合
    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }
  // 存储级别有效，则向block对应的BlockManger集合里添加该blockManagerId
  // 如果无效，则移除之
    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
  // 如果block对应的BlockManger集合为空，则没有BlockManager与之对应，则从blockLocations删除这个blockId
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsMultipleBlockIds(
                                            blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot {
        _.isDriver
      }.filterNot {
        _ == blockManagerId
      }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
    * Returns an [[RpcEndpointRef]] of the [[BlockManagerSlaveEndpoint]] for sending RPC messages.
    */
  private def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.slaveEndpoint
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
}

@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  def isCached: Boolean = memSize + diskSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
}

//每一个BlockManager一个BlockManagerInfo
//相当于是BlockManager的元数据
private[spark] class BlockManagerInfo(
                                       val blockManagerId: BlockManagerId,
                                       timeMs: Long,
                                       val maxMem: Long,
                                       val slaveEndpoint: RpcEndpointRef)
  extends Logging {

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  //BlockManagerInfo管理了每个BlockManager内部的block的blockId到BlockStatus的映射
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // Cached blocks held by this BlockManager. This does not include broadcast blocks.
  private val _cachedBlocks = new mutable.HashSet[BlockId]

  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
                       blockId: BlockId,
                       storageLevel: StorageLevel,
                       memSize: Long,
                       diskSize: Long) {

    updateLastSeenMs()
    // 如果内部有这个block

    if (_blocks.containsKey(blockId)) {
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      val originalLevel: StorageLevel = blockStatus.storageLevel
      val originalMemSize: Long = blockStatus.memSize
      //判断入股storageLevel是使用内存，那么就给剩余内存数量加上当前的内存量
      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }
    //如果block创建一份blockStatus，然后根据其持久化级别，对相应的内存资源进行计算
    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on disk on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
      }
      if (!blockId.isBroadcast && blockStatus.isCached) {
        _cachedBlocks += blockId
      }
      //如果StorageLevel是非法的，而且之前保存过这个blockId，那么就将blockId从内存中删除
    } else if (_blocks.containsKey(blockId)) {
      // If isValid is not true, drop the block.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      _blocks.remove(blockId)
      _cachedBlocks -= blockId
      if (blockStatus.storageLevel.useMemory) {
        logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (blockStatus.storageLevel.useDisk) {
        logInfo("Removed %s on %s on disk (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
      }
    }
  }

  def removeBlock(blockId: BlockId) {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
    }
    _cachedBlocks -= blockId
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  // This does not include broadcast blocks.
  def cachedBlocks: collection.Set[BlockId] = _cachedBlocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear() {
    _blocks.clear()
  }
}
