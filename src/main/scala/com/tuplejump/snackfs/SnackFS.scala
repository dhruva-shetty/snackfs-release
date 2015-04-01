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
package com.tuplejump.snackfs

import java.net.InetAddress
import java.net.URI

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import org.apache.cassandra.utils.UUIDGen
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import com.tuplejump.snackfs.api.model.AppendFileCommand
import com.tuplejump.snackfs.api.model.ChmodCommand
import com.tuplejump.snackfs.api.model.CreateFileCommand
import com.tuplejump.snackfs.api.model.DeleteCommand
import com.tuplejump.snackfs.api.model.FileStatusCommand
import com.tuplejump.snackfs.api.model.ListCommand
import com.tuplejump.snackfs.api.model.MakeDirectoryCommand
import com.tuplejump.snackfs.api.model.OpenFileCommand
import com.tuplejump.snackfs.api.model.RenameCommand
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.cassandra.store.CassandraStore
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

case class SnackFS() extends FileSystem {

  private lazy val log = Logger.get(getClass)

  private var systemURI: URI = null
  private var currentDirectory: Path = null
  private var subBlockSize: Long = 0L

  private var atMost: FiniteDuration = null
  private var useLocking: Boolean = true
  private var queryDuration: FiniteDuration = null
  private var store: FileSystemStore = null
  private var customConfiguration: SnackFSConfiguration = _

  val processId = UUIDGen.getTimeUUID

  override def initialize(uri: URI, configuration: Configuration) = {
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Initializing SnackFs")
    super.initialize(uri, configuration)
    setConf(configuration)

    systemURI = URI.create(uri.getScheme + "://" + uri.getAuthority)
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " SystemUri %s", systemURI)

    val directory = new Path("/user", System.getProperty("user.name"))
    currentDirectory = makeQualified(directory)

    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " generating required configuration")
    customConfiguration = SnackFSConfiguration.get(configuration)

    store = new CassandraStore(customConfiguration)
    atMost = customConfiguration.atMost
    useLocking = customConfiguration.useLocking
    queryDuration = customConfiguration.queryDuration
    Await.ready(store.createKeyspace, atMost)
    store.init

    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " creating base directory")
    mkdirs(new Path("/"))

    subBlockSize = customConfiguration.subBlockSize
  }

  private def makeAbsolute(path: Path): Path =  if (path.isAbsolute) path else resolvePath(new Path(currentDirectory, path))

  def getUri: URI = systemURI

  def setWorkingDirectory(newDir: Path) = {
    currentDirectory = makeAbsolute(newDir)
  }

  def getWorkingDirectory: Path = currentDirectory

  def open(path: Path, bufferSize: Int): FSDataInputStream = OpenFileCommand(store, path, bufferSize, atMost)

  def mkdirs(path: Path, permission: FsPermission): Boolean = MakeDirectoryCommand(store, makeAbsolute(path), permission, atMost)

  def create(filePath: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    CreateFileCommand(store, filePath, permission, overwrite, bufferSize, replication, blockSize, progress, processId, statistics, subBlockSize, atMost, useLocking)
  }

  override def getDefaultBlockSize: Long = customConfiguration.blockSize

  def append(path: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = AppendFileCommand(store, path, bufferSize, progress, atMost)

  def getFileStatus(path: Path): FileStatus = FileStatusCommand(this, store, makeAbsolute(path), atMost)

  def delete(path: Path, isRecursive: Boolean): Boolean = {
    //check WRITE permissions before allowing a delete
    store.permissionChecker.checkPermission(path, FsAction.WRITE, false, false, checkChildren = true, atMost)
    DeleteCommand(this, store, makeAbsolute(path), isRecursive, queryDuration)
  }

  def rename(src: Path, dst: Path): Boolean = RenameCommand(store, makeAbsolute(src), makeAbsolute(dst), atMost)

  def listStatus(path: Path): Array[FileStatus] = ListCommand(this, store, makeAbsolute(path), queryDuration)

  override def delete(p1: Path): Boolean = delete(p1, isRecursive = false)

  override def getFileBlockLocations(path: Path, start: Long, len: Long): Array[BlockLocation] = {
    val blocks: Map[BlockMeta, List[String]] = Await.result(store.getBlockLocations(path), atMost)
    val locs = blocks.filterNot(x => x._1.offset + x._1.length < start)
    val locsMap = locs.map {
      case (b, ips) =>
        val bl = new BlockLocation()
        bl.setHosts(ips.map(p => InetAddress.getByName(p).getHostName).toArray)
        bl.setNames(ips.map(i => "%s:%s".format(i, customConfiguration.CassandraThriftPort)).toArray)
        bl.setOffset(b.offset)
        bl.setLength(b.length)
        if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Path %s BlockLocation %s ", path, bl)
        bl
    }
    locsMap.toArray
  }

  override def getFileBlockLocations(file: FileStatus, start: Long, len: Long): Array[BlockLocation] = {
    if(LogConfiguration.isDebugEnabled()) log.debug("Retrieve block locations for file %s ", file);
    getFileBlockLocations(file.getPath, start, len)
  }
  
  override def resolvePath(p: Path): Path = {
    checkPath(p)
    if (p.isAbsolute && p.toUri().getScheme == null && p.toUri().getAuthority == null) {
      def path = new Path(getUri.getScheme, getUri.getAuthority, p.toUri().getPath)
      if(LogConfiguration.isDebugEnabled()) log.debug("Received resolve path request %s resolved to %s", p, path);
      return path
    }
    p
  }
  
  def getConfiguration: SnackFSConfiguration = customConfiguration
  
  override def setPermission(path: Path, permission: FsPermission) = ChmodCommand(store, path, permission, queryDuration)
}
