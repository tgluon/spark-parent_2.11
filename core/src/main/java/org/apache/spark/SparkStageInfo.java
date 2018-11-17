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

package org.apache.spark;

import java.io.Serializable;

/**
 * Exposes information about Spark Stages.
 * 公开有关Spark Stages的信息。
 * This interface is not designed to be implemented outside of Spark.  We may add additional methods
 * which may break binary compatibility with outside implementations.
 * 此接口不是为在Spark外部实现而设计的。
 */
public interface SparkStageInfo extends Serializable {
  int stageId();
  int currentAttemptId();
  long submissionTime();
  String name();
  int numTasks();
  int numActiveTasks();
  int numCompletedTasks();
  int numFailedTasks();
}