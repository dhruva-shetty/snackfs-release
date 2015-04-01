/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.tuplejump.snackfs.api.model

import java.io.FileNotFoundException

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

import com.tuplejump.snackfs.SnackFS
import com.tuplejump.snackfs.api.partial.Command
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

object FileStatusCommand extends Command {
  private lazy val log = Logger.get(getClass)

  def apply(fs: SnackFS, store: FileSystemStore, filePath: Path, atMost: FiniteDuration): FileStatus = {
    val maybeFile = Try(Await.result(store.retrieveINode(filePath), atMost))
    maybeFile match {
      case Success(file: INode) =>
        val absolutePath = fs.resolvePath(new Path(fs.getWorkingDirectory, filePath))
        if (LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " getting status for %s resolved to %s", filePath, absolutePath)
        SnackFileStatus(file, absolutePath)
      case Failure(e) =>
        log.info("No such file exists: %s", filePath)
        throw new FileNotFoundException("No such file exists: " + filePath)
    }
  }
}
