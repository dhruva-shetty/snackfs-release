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
package com.tuplejump.snackfs.fs.stream

import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.UUID

import scala.compat.Platform
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

import org.apache.cassandra.utils.UUIDGen
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission

import com.tuplejump.snackfs.cassandra.model.GenericOpSuccess
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.fs.model.BlockMeta
import com.tuplejump.snackfs.fs.model.FileType
import com.tuplejump.snackfs.fs.model.INode
import com.tuplejump.snackfs.fs.model.SubBlockMeta
import com.tuplejump.snackfs.security.UnixGroupsMapping
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

case class FileSystemOutputStream(store: FileSystemStore, path: Path,
                                  blockSize: Long, subBlockSize: Long,
                                  bufferSize: Long, atMost: FiniteDuration) extends OutputStream {

  private lazy val log = Logger.get(getClass)

  private var blockFutures: Seq[Future[GenericOpSuccess]] = Nil
  private var blockId: UUID = UUIDGen.getTimeUUID
  private var outBuffer = Array.empty[Byte]
  private var subBlocksMeta = List[SubBlockMeta]()
  private var blocksMeta = List[BlockMeta]()

  private var isClosed: Boolean = false
  private var isClosing = false

  private var bytesWrittenToBlock: Long = 0
  private var subBlockOffset: Long = 0
  private var blockOffset: Long = 0
  private var position: Long = 0
  
  private val compressed = store.config.isCompressed(path)

  def write(p1: Int) = {
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to write as stream is closed")
      throw ex
    }
    outBuffer = outBuffer ++ Array(p1.toByte)
    position += 1
    if (position == subBlockSize) {
      flush()
    }
  }

  override def write(buf: Array[Byte], offset: Int, length: Int) = {
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " write length %s", length)
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to write as stream is closed")
      throw ex
    }
    val startTime = Platform.currentTime
    try {
      var lengthTemp = length
      var offsetTemp = offset
      while (lengthTemp > 0) {
        val lengthToWrite = math.min(subBlockSize - position, lengthTemp).asInstanceOf[Int]
        val slice: Array[Byte] = buf.slice(offsetTemp, offsetTemp + lengthToWrite)
        outBuffer = outBuffer ++ slice
        lengthTemp -= lengthToWrite
        offsetTemp += lengthToWrite
        position += lengthToWrite
        if (position == subBlockSize) flush
      }
    } finally {
      if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Total write time %s", Platform.currentTime - startTime)
    }
  }

  private def endSubBlock() = {
    if (position != 0) {
      val subBlockMeta = SubBlockMeta(UUIDGen.getTimeUUID, subBlockOffset, position)
      val start = Platform.currentTime
      try {
        blockFutures = blockFutures :+ store.storeSubBlock(blockId, subBlockMeta, ByteBuffer.wrap(outBuffer), compressed)
        subBlockOffset += position
        bytesWrittenToBlock += position
        subBlocksMeta = subBlocksMeta :+ subBlockMeta
        position = 0
        outBuffer = Array.empty[Byte]
      } finally {
        if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Elapsed time to flush sblockId %s is %s ms", subBlockMeta.id, (Platform.currentTime - start))
      }
    }
  }

  private def endBlock() = {
	  val user = System.getProperty("user.name")
    val subBlockLengths = subBlocksMeta.map(_.length).sum
    val block = BlockMeta(blockId, blockOffset, subBlockLengths, subBlocksMeta)
    blocksMeta = blocksMeta :+ block
    val iNode = INode(user, UnixGroupsMapping.getUserGroup(user), FsPermission.getDefault, FileType.FILE, blocksMeta, Platform.currentTime)
    
    val start = Platform.currentTime
    try {
      Await.ready(store.storeINode(path, iNode), atMost)
    } finally {
    	if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " Elapsed time to flush inode for path %s is %s ms", path, (Platform.currentTime - start))
    }
    
    blockOffset += subBlockLengths.asInstanceOf[Long]
    subBlocksMeta = List()
    subBlockOffset = 0
    blockId = UUIDGen.getTimeUUID
    bytesWrittenToBlock = 0
  }

  override def flush() = {
    if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " flushing data at %s", position)
    if (isClosed) {
      val ex = new IOException("Stream closed")
      log.error(ex, "Failed to write as stream is closed")
      throw ex
    }
    endSubBlock
    if (bytesWrittenToBlock >= blockSize || isClosing) endBlock
    if(isClosing) {
      blockFutures.foreach(f => Await.ready(f, atMost))
      blockFutures = Nil
    }
  }

  override def close() = {
    if (!isClosed) {
      if(LogConfiguration.isDebugEnabled) log.debug(Thread.currentThread.getName() + " closing stream")
      isClosing = true
      flush
      super.close
      isClosed = true
    }
  }
}
