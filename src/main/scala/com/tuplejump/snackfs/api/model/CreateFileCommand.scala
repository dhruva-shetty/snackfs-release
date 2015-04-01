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

import java.io.IOException
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem.Statistics
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import com.tuplejump.snackfs.api.partial.Command
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.fs.model.FileType
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.fs.stream.FileSystemOutputStream
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger
import com.tuplejump.snackfs.security.UnixGroupsMapping
import org.apache.hadoop.fs.permission.FsAction

object CreateFileCommand extends Command {

  private lazy val log = Logger.get(getClass)

  def apply(store: FileSystemStore,
            filePath: Path,
            filePermission: FsPermission,
            overwrite: Boolean,
            bufferSize: Int,
            replication: Short,
            blockSize: Long,
            progress: Progressable,
            processId: UUID,
            statistics: Statistics,
            subBlockSize: Long,
            atMost: FiniteDuration,
            useLocking: Boolean): FSDataOutputStream = {

    var isCreatePossible = true; 
    if(useLocking) {
      isCreatePossible = Await.result(store.acquireFileLock(filePath, processId), atMost)
    }
    if (isCreatePossible) {
      try {
        val mayBeFile = Try(Await.result(store.retrieveINode(filePath), atMost))
        mayBeFile match {
          case Success(file: INode) =>
            if (file.isFile && !overwrite) {
              val ex = new IOException("File exists and cannot be overwritten")
              log.error(ex, "Failed to create file %s as it exists and cannot be overwritten", filePath)
              throw ex
            } else if (file.isDirectory) {
              val ex = new IOException("Directory with same name exists")
              log.error(ex, "Failed to create file %s as a directory with that name exists", filePath)
              throw ex
            }
          case Failure(e: Exception) =>
            val parentPath = filePath.getParent

            if (parentPath != null) {
              MakeDirectoryCommand(store, parentPath, filePermission, atMost)
            }
        }
        if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " creating file %s", filePath)
        
        val user = System.getProperty("user.name")
        val permissions = FsPermission.getDefault
        val timestamp = System.currentTimeMillis()
        val iNode = INode(user, UnixGroupsMapping.getUserGroup(user), permissions, FileType.FILE, List(), timestamp)
        
        //we only need to check if we have WRITE permission for the file/directory and it's parent/ancestor
        store.permissionChecker.checkPermission(filePath, null, FsAction.WRITE, false, checkAncestor = true, false, atMost)

        Await.ready(store.storeINode(filePath, iNode), atMost)
        val fileStream = new FileSystemOutputStream(store, filePath, blockSize, subBlockSize, bufferSize, atMost)
        val fileDataStream = new FSDataOutputStream(fileStream, statistics)
        fileDataStream
      } finally {
        store.releaseFileLock(filePath)
      }
    } else {
      val ex = new IOException("Acquire lock failure")
      log.error(ex, "Could not get lock on file %s", filePath)
      throw ex
    }
  }

}
