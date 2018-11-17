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

package org.apache.spark.sql.execution.streaming

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.{FileFormat, FileFormatWriter}

object FileStreamSink {
  // The name of the subdirectory that is used to store metadata about which files are valid.
  val metadataDir = "_spark_metadata"
}

/**
 * A sink that writes out results to parquet files.  Each batch is written out to a unique
 * directory. After all of the files in a batch have been successfully written, the list of
 * file paths is appended to the log atomically. In the case of partial failures, some duplicate
 * data may be present in the target directory, but only one copy of each file will be present
 * in the log.
 */
class FileStreamSink(
    sparkSession: SparkSession,
    path: String,
    fileFormat: FileFormat,
    partitionColumnNames: Seq[String],
    options: Map[String, String]) extends Sink with Logging {

  private val basePath = new Path(path)
  private val logPath = new Path(basePath, FileStreamSink.metadataDir)
  private val fileLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, logPath.toUri.toString)
  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    /* 首先根据持久化的 fileLog 来判断这个 batchId 是否已经写出过 */
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      /* 如果 batchId 已经完整写出过，则本次跳过 addBatch */
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      /* 本次需要具体执行写出 data */
      /* 初始化 FileCommitter -- FileCommitter 能正确处理 task 推测执行、task 失败重做等情况 */
      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = path,
        isAppend = false)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ =>  // Do nothing
      }

      // Get the actual partition columns as attributes after matching them by name with
      // the given columns names.
      /* 获取需要做 partition 的 columns */
      val partitionColumns: Seq[Attribute] = partitionColumnNames.map { col =>
        val nameEquality = data.sparkSession.sessionState.conf.resolver
        data.logicalPlan.output.find(f => nameEquality(f.name, col)).getOrElse {
          throw new RuntimeException(s"Partition column $col not found in schema ${data.schema}")
        }
      }
      /* 真正写出数据 */
      FileFormatWriter.write(
        sparkSession = sparkSession,
        queryExecution = data.queryExecution,
        fileFormat = fileFormat,
        committer = committer,
        outputSpec = FileFormatWriter.OutputSpec(path, Map.empty),
        hadoopConf = hadoopConf,
        partitionColumns = partitionColumns,
        bucketSpec = None,
        refreshFunction = _ => (),
        options = options)
    }
  }

  override def toString: String = s"FileSink[$path]"
}
