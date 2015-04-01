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

object ListCommand extends Command {
  private lazy val log = Logger.get(getClass)

  def apply(fs: SnackFS, store: FileSystemStore, path: Path, atMost: FiniteDuration): Array[FileStatus] = {
    var result: Array[FileStatus] = Array()
    val absolutePath = path
    val mayBeFile = Try(Await.result(store.retrieveINode(absolutePath), atMost))

    mayBeFile match {
      case Success(file: INode) =>
        if (file.isFile) {
          if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " fetching file status for %s", path)
          val fileStatus = SnackFileStatus(file, absolutePath)
          result = Array(fileStatus)
        } else {
          if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " fetching directory status for %s", path)
          val subPaths = Await.result(store.fetchSubPaths(absolutePath, isDeepFetch = false), atMost)
          result = subPaths.map(p => FileStatusCommand(fs, store, p, atMost)).toArray
        }

      case Failure(e) =>
        val ex = new FileNotFoundException("No such file exists: " + path)
        log.error(ex, "Failed to list status of %s as it doesn't exist", path)
        throw ex
    }
    result
  }

}
