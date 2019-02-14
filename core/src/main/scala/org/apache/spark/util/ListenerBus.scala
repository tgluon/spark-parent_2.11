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

package org.apache.spark.util

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
  * SparkUI 的各个监控指标都是， 由 ListenerBus 最为生产者将消息， 推送到消息缓存出默
  * 认支持 1 万， 然后推送给各个 Listener 进行处理， 然后我们的 Spark 的 webUIPage 去获取各
  * 个 Listener 的数据， 进行展示
  * An event bus which posts events to its listeners.
  * ListenerBus是个泛型特质，其泛型参数为 [L <: AnyRef, E]，其中L是代表监听器的泛型参数，
  * 可以看到ListenerBus支持任何类型的监听器，E是代表事件的泛型参数。
  */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {


  // Marked `private[spark]` for access in tests.
  /** 用于维护所有注册的监听器，其数据结构为CopyOnWriteArrayList[L] **/
  /** CopyOnWriteArrayList相当于线程安全的List **/

  private[spark] val listeners = new CopyOnWriteArrayList[L]

  /**
    * 向listeners中添加监听器的方法，由于listeners采用CopyOnWriteArrayList来实现，
    * 所以addListener方法是线程安全的；
    * Add a listener to listen events. This method is thread-safe and can be called in any thread.
    */
  final def addListener(listener: L): Unit = {
    listeners.add(listener)
  }

  /**
    * 从listeners中移除监听器的方法，由于listeners采用CopyOnWriteArrayList来实现，
    * 所以removeListener方法是线程安全的；
    * Remove a listener and it won't receive any events. This method is thread-safe and can be called
    * in any thread.
    */
  final def removeListener(listener: L): Unit = {
    listeners.remove(listener)
  }

  /**
    * 此方法的作用是将事件投递给所有的监听器。虽然CopyOnWriteArrayList本身是线程的安全的，
    * 但是由于postToAll方法内部引入了“先检查后执行”的逻辑，因而postToAll方法不是线程安全的，
    * 所以所有对postToAll方法的调用应当保证在同一个线程中；
    * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
    * `postToAll` in the same thread for all events.
    */
  def postToAll(event: E): Unit = {
    // JavaConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here we use
    // Java Iterator directly.
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        // 每当有新的事件那么关心该事件的所有listeners都会被触发
        doPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

  /**
    * 用于将事件投递给指定的监听器，此方法只提供了接口定义，具体实现需要子类提供；
    * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
    * thread for all listeners.
    */
  protected def doPostEvent(listener: L, event: E): Unit

  /**
    * 查找与指定类型相同的监听器列表
    *
    * @tparam T
    * @return Seq[T]
    */
  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    // 将T隐式转化成ClassTag
    val c = implicitly[ClassTag[T]].runtimeClass
    // 将CopyOnWriteArrayList转化成scala集合类型
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}
