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

package org.apache.spark.broadcast

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging

/**
  * 用于将配置信息和序列化后的RDD,Job及ShuffleDependency等信息在本地存储。如果为了容灾,也会复制到其他节点上。
  *
  * @param isDriver        标识是否是driver端
  * @param conf            配置信息
  * @param securityManager 安全管理器
  */
private[spark] class BroadcastManager(
                                       val isDriver: Boolean,
                                       conf: SparkConf,
                                       securityManager: SecurityManager)
  extends Logging {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  // 在使用Broadcast之前由SparkContext或Executor调用
  private def initialize() {
    synchronized {
      // 判断BroadcastManager是否已经初始化,以保证BroadcastManager只初始化一次
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf, securityManager)
        // 将BroadcastManager标记为已初始化状态
        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }
  // 下一个广播对象的广播ID
  private val nextBroadcastId = new AtomicLong(0)

  /**
    *
    * @param value_ 值
    * @param isLocal 是否是local模式
    * @tparam T
    * @return
    */
  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    // 使用broadcastFactory创建广播变量
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  /**
    * 移除广播变量
    *
    * @param id               id
    * @param removeFromDriver 是否从driver端移除
    * @param blocking         块
    */
  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
