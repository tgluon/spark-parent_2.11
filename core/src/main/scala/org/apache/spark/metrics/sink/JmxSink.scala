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

package org.apache.spark.metrics.sink

import java.util.Properties

import com.codahale.metrics.{JmxReporter, MetricRegistry}

import org.apache.spark.SecurityManager

/**
  * 借助Metrics提供的JmxReporter的API，将度量输出到MBean中，这样就可以打开Java VisualVM，然后打开Tomcat进程监控，
  * 给VisualVM安装MBeans插件后，选择MBeans标签页可以对JmxSink所有注册到JMX中的对象进行管理。
  */
private[spark] class JmxSink(val property: Properties, val registry: MetricRegistry,
    securityMgr: SecurityManager) extends Sink {

  val reporter: JmxReporter = JmxReporter.forRegistry(registry).build()

  override def start() {
    reporter.start()
  }

  override def stop() {
    reporter.stop()
  }

  override def report() { }

}
