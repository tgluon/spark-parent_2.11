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

package org.apache.spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

/**
  * ApplicationSource用于采集Spark应用程序相关的度量。
  * 代码清单2中ApplicationSource重载了metricRegistry和
  * sourceName，并且向自身的注册表注册了status
  * （即应用状态，包括：WAITING, RUNNING, FINISHED, FAILED,
  * KILLED, UNKNOWN）、runtime_ms（运行持续时长）、cores（授权的内核数）
  * 等度量。这三个度量的取值分别来自于ApplicationInfo的state、
  * duration和coresGranted三个属性。
  */

private[master] class ApplicationSource(val application: ApplicationInfo) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "%s.%s.%s".format("application", application.desc.name,
    System.currentTimeMillis())

  metricRegistry.register(MetricRegistry.name("status"), new Gauge[String] {
    override def getValue: String = application.state.toString
  })

  metricRegistry.register(MetricRegistry.name("runtime_ms"), new Gauge[Long] {
    override def getValue: Long = application.duration
  })

  metricRegistry.register(MetricRegistry.name("cores"), new Gauge[Int] {
    override def getValue: Int = application.coresGranted
  })

}
