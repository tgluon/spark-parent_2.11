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

import java.io.{FileInputStream, InputStream}
import java.util.{NoSuchElementException, Properties}

import scala.xml.XML

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
  * An interface to build Schedulable tree
  * buildPools: build the tree nodes(pools)
  * addTaskSetManager: build the leaf nodes(TaskSetManagers)
  */
private[spark] trait SchedulableBuilder {
  def rootPool: Pool

  def buildPools(): Unit

  def addTaskSetManager(manager: Schedulable, properties: Properties): Unit
}

private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools() {
    // nothing
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    rootPool.addSchedulable(manager)
  }
}

private[spark] class FairSchedulableBuilder(val rootPool: Pool, conf: SparkConf)
  extends SchedulableBuilder with Logging {

  val schedulerAllocFile = conf.getOption("spark.scheduler.allocation.file")
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml"
  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.pool"
  val DEFAULT_POOL_NAME = "default"
  val MINIMUM_SHARES_PROPERTY = "minShare"
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"
  val WEIGHT_PROPERTY = "weight"
  val POOL_NAME_PROPERTY = "@name"
  val POOLS_PROPERTY = "pool"
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  val DEFAULT_MINIMUM_SHARE = 0
  val DEFAULT_WEIGHT = 1
  //FairSchedulableBuilder.buildPools需要根据$SPARK_HOME/conf/fairscheduler.xml文件来构建调度树。配置文件大致如下：
  /**
    * <allocations>
    * <pool name="production">
    * <schedulingMode>FAIR</schedulingMode>
    * <weight>1</weight>
    * <minShare>2</minShare>
    * </pool>
    * <pool name="test">
    * <schedulingMode>FIFO</schedulingMode>
    * <weight>2</weight>
    * <minShare>3</minShare>
    * </pool>
    * </allocations>
    */
  override def buildPools() {
    var is: Option[InputStream] = None
    try {
      is = Option {
        schedulerAllocFile.map { f =>
          new FileInputStream(f)
        }.getOrElse {
          Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        }
      }

      is.foreach { i => buildFairSchedulerPool(i) }
    } finally {
      is.foreach(_.close())
    }

    // finally create "default" pool
    buildDefaultPool()
  }

  private def buildDefaultPool() {
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(pool)
      logInfo("Created default pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
    }
  }

  private def buildFairSchedulerPool(is: InputStream) {
    //加载xml 文件
    val xml = XML.load(is)
    //遍历
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {

      val poolName = (poolNode \ POOL_NAME_PROPERTY).text
      var schedulingMode = DEFAULT_SCHEDULING_MODE
      var minShare = DEFAULT_MINIMUM_SHARE
      var weight = DEFAULT_WEIGHT

      val xmlSchedulingMode = (poolNode \ SCHEDULING_MODE_PROPERTY).text
      if (xmlSchedulingMode != "") {
        try {
          schedulingMode = SchedulingMode.withName(xmlSchedulingMode)
        } catch {
          case e: NoSuchElementException =>
            logWarning(s"Unsupported schedulingMode: $xmlSchedulingMode, " +
              s"using the default schedulingMode: $schedulingMode")
        }
      }

      val xmlMinShare = (poolNode \ MINIMUM_SHARES_PROPERTY).text
      if (xmlMinShare != "") {
        minShare = xmlMinShare.toInt
      }

      val xmlWeight = (poolNode \ WEIGHT_PROPERTY).text
      if (xmlWeight != "") {
        weight = xmlWeight.toInt
      }
      //根据xml的配置，
      //最终生成一个新的Pool
      val pool = new Pool(poolName, schedulingMode, minShare, weight)
      //将这个Pool加入到rootPool中
      rootPool.addSchedulable(pool)
      logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, schedulingMode, minShare, weight))
    }
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    var poolName = DEFAULT_POOL_NAME
    //先生成一个默认的parentPool
    var parentPool = rootPool.getSchedulableByName(poolName)
    //若有配置信息，
    //则根据配置信息得到poolName
    if (properties != null) {
      poolName = properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      parentPool = rootPool.getSchedulableByName(poolName)
      //若rootPool中没有这个pool
      if (parentPool == null) {
        // we will create a new pool that user has configured in app
        // instead of being defined in xml file
        //我们会根据用户在app上的配置生成新的pool，
        //而不是根据xml 文件
        parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
          DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
        //将这个manager加入到这个pool
        rootPool.addSchedulable(parentPool)
        logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
          poolName, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
      }
    }
    parentPool.addSchedulable(manager)
    logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
  }
}
