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

import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{MapOutputTracker, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * An RpcEndpoint to take commands from the master to execute options. For example,
 * this is used to remove blocks from the slave's BlockManager.
 */
private[storage]
class BlockManagerSlaveEndpoint(
    override val rpcEnv: RpcEnv,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker)
  extends ThreadSafeRpcEndpoint with Logging {

  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-slave-async-thread-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)

  // Operations that involve removing blocks may be slow and should be done asynchronously
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // 接收master发送过来的RemoveBlock消息
    case RemoveBlock(blockId) =>
      doAsync[Boolean]("removing block " + blockId, context) {
        // 调用BlockManager删除block
        blockManager.removeBlock(blockId)
        true
      }

    // 接收master发送过来的RemoveRdd消息
    case RemoveRdd(rddId) =>
      doAsync[Int]("removing RDD " + rddId, context) {
        // 调用BlockManager删除rdd对应的block
        blockManager.removeRdd(rddId)
      }
    // 接收master发送过来的RemoveShuffle消息
    case RemoveShuffle(shuffleId) =>
      doAsync[Boolean]("removing shuffle " + shuffleId, context) {
        if (mapOutputTracker != null) {
          // 首先需要调用MapOutputTracker取消shuffleId的注册的
          mapOutputTracker.unregisterShuffle(shuffleId)
        }
        // 删除shuffle的元数据
        SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
      }
    // 接收master发送过来的RemoveBroadcast消息
    case RemoveBroadcast(broadcastId, _) =>
      doAsync[Int]("removing broadcast " + broadcastId, context) {
        // 调用BlockManagerd的removeBroadcast
        blockManager.removeBroadcast(broadcastId, tellMaster = true)
      }
    // 接收消息GetBlockStatus，调用blockManager的getStatus
    case GetBlockStatus(blockId, _) =>
      context.reply(blockManager.getStatus(blockId))
    // 接收GetMatchingBlockIds消息调用blockManager的getMatchingBlockIds方法
    case GetMatchingBlockIds(filter, _) =>
      context.reply(blockManager.getMatchingBlockIds(filter))

    case TriggerThreadDump =>
      context.reply(Utils.getThreadDump())
  }

  private def doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T) {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.onSuccess { case response =>
      logDebug("Done " + actionMessage + ", response is " + response)
      context.reply(response)
      logDebug("Sent response: " + response + " to " + context.senderAddress)
    }
    future.onFailure { case t: Throwable =>
      logError("Error in " + actionMessage, t)
      context.sendFailure(t)
    }
  }

  override def onStop(): Unit = {
    asyncThreadPool.shutdownNow()
  }
}
